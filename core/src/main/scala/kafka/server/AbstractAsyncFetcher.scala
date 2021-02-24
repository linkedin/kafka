package kafka.server

/**
 * change categories:
 * 1. adding the async model
 * 2. still using network blocking calls for adding partitions
 * 3. removing the partitionMapLock in all of the methods
 * 4. will handle backoff by scheduling future events
 */

import kafka.cluster.BrokerEndPoint
import kafka.common.ClientIdAndBroker
import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.utils.CoreUtils.inLock
import kafka.utils.{DelayedItem, Logging}
import org.apache.kafka.common.{InvalidRecordException, TopicPartition}
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.{EpochEndOffset, FetchRequest, FetchResponse, OffsetsForLeaderEpochRequest}
import org.apache.kafka.common.utils.Time

import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import scala.collection.{Map, Seq, Set, mutable}
import scala.collection.JavaConverters._


sealed trait FetcherEvent {
  def priority: Int
  def state: FetcherState
}

// TODO: modify the AddPartitions event to include remove partitions as well
case class AddPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]) extends FetcherEvent {
  def priority = 2
  def state = FetcherState.AddPartitions
}

case class RemovePartitions(topicPartitions: Set[TopicPartition]) extends FetcherEvent {
  def priority = 2
  def state = FetcherState.RemovePartitions
}

case class TruncateAndFetch() extends FetcherEvent {
  def priority = 1
  def state = FetcherState.TruncateAndFetch
}

abstract class AbstractAsyncFetcher(fetcherId: String,
                                    clientId: String,
                                    val sourceBroker: BrokerEndPoint,
                                    failedPartitions: FailedPartitions,
                                    fetchBackOffMs: Int = 0,
                                    time: Time) extends FetcherEventProcessor with Logging {

  type FetchData = FetchResponse.PartitionData[Records]
  type EpochData = OffsetsForLeaderEpochRequest.PartitionData

  private val partitionStates = new PartitionStates[PartitionFetchState]

  private val metricId = ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)
  var idle = false

  // process fetched data
  protected def processPartitionData(topicPartition: TopicPartition,
                                     fetchOffset: Long,
                                     partitionData: FetchData): Option[LogAppendInfo]

  protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit

  protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit

  protected def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[FetchRequest.Builder]]

  protected def latestEpoch(topicPartition: TopicPartition): Option[Int]

  protected def logEndOffset(topicPartition: TopicPartition): Long

  protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch]

  protected def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset]

  protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, FetchData)]

  protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def isOffsetForLeaderEpochSupported: Boolean

  private[server] val eventManager = new FetcherEventManager(fetcherId, this, time,
    fetcherStats.rateAndTimeMetrics)

  def start(): Unit = {
    eventManager.start()
  }

  def doWork(): Unit = {
    maybeTruncate()
    maybeFetch()
  }

  private def maybeFetch(): Unit = {
    val (fetchStates, fetchRequestOpt) = {
      val fetchStates = partitionStates.partitionStateMap.asScala
      val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = buildFetch(fetchStates)

      handlePartitionsWithErrors(partitionsWithError, "maybeFetch")

      if (fetchRequestOpt.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }

      (fetchStates, fetchRequestOpt)
    }

    fetchRequestOpt.foreach { fetchRequest =>
      processFetchRequest(fetchStates, fetchRequest)
    }
  }

  // deal with partitions with errors, potentially due to leadership changes
  private def handlePartitionsWithErrors(partitions: Iterable[TopicPartition], methodName: String): Unit = {
    if (partitions.nonEmpty) {
      debug(s"Handling errors in $methodName for partitions $partitions")
      delayPartitions(partitions, fetchBackOffMs)
    }
  }

  override def process(event: FetcherEvent): Unit = {
    event match {
      case AddPartitions(initialFetchStates) =>
        addPartitions(initialFetchStates)
      case RemovePartitions(topicPartitions) =>
        removePartitions(topicPartitions)
      case TruncateAndFetch() =>
        doWork()
    }
  }

  /**
   * Builds offset for leader epoch requests for partitions that are in the truncating phase based
   * on latest epochs of the future replicas (the one that is fetching)
   */
  private def fetchTruncatingPartitions(): (Map[TopicPartition, EpochData], Set[TopicPartition]) = {
    val partitionsWithEpochs = mutable.Map.empty[TopicPartition, EpochData]
    val partitionsWithoutEpochs = mutable.Set.empty[TopicPartition]

    partitionStates.stream().forEach(new Consumer[PartitionStates.PartitionState[PartitionFetchState]] {
      override def accept(state: PartitionStates.PartitionState[PartitionFetchState]): Unit = {
        if (state.value.isTruncating) {
          val tp = state.topicPartition
          latestEpoch(tp) match {
            case Some(epoch) if isOffsetForLeaderEpochSupported =>
              partitionsWithEpochs += tp -> new EpochData(Optional.of(state.value.currentLeaderEpoch), epoch)
            case _ =>
              partitionsWithoutEpochs += tp
          }
        }
      }
    })

    (partitionsWithEpochs, partitionsWithoutEpochs)
  }

  private def maybeTruncate(): Unit = {
    val (partitionsWithEpochs, partitionsWithoutEpochs) = fetchTruncatingPartitions()
    if (partitionsWithEpochs.nonEmpty) {
      truncateToEpochEndOffsets(partitionsWithEpochs)
    }
    if (partitionsWithoutEpochs.nonEmpty) {
      truncateToHighWatermark(partitionsWithoutEpochs)
    }
  }

  /**
   * - Build a leader epoch fetch based on partitions that are in the Truncating phase
   * - Send OffsetsForLeaderEpochRequest, retrieving the latest offset for each partition's
   *   leader epoch. This is the offset the follower should truncate to ensure
   *   accurate log replication.
   * - Finally truncate the logs for partitions in the truncating phase and mark the
   *   truncation complete. Do this within a lock to ensure no leadership changes can
   *   occur during truncation.
   */
  private def truncateToEpochEndOffsets(latestEpochsForPartitions: Map[TopicPartition, EpochData]): Unit = {
    val endOffsets = fetchEpochEndOffsets(latestEpochsForPartitions)
    //Ensure we hold a lock during truncation.
      //Check no leadership and no leader epoch changes happened whilst we were unlocked, fetching epochs
    val epochEndOffsets = endOffsets.filter { case (tp, _) =>
      val curPartitionState = partitionStates.stateValue(tp)
      val partitionEpochRequest = latestEpochsForPartitions.get(tp).getOrElse {
        throw new IllegalStateException(
          s"Leader replied with partition $tp not requested in OffsetsForLeaderEpoch request")
      }
      val leaderEpochInRequest = partitionEpochRequest.currentLeaderEpoch.get
      curPartitionState != null && leaderEpochInRequest == curPartitionState.currentLeaderEpoch
    }

    val ResultWithPartitions(fetchOffsets, partitionsWithError) = maybeTruncateToEpochEndOffsets(epochEndOffsets, latestEpochsForPartitions)
    handlePartitionsWithErrors(partitionsWithError, "truncateToEpochEndOffsets")
    updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
  }

  // Visible for testing
  private[server] def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]

    for (tp <- partitions) {
      val partitionState = partitionStates.stateValue(tp)
      if (partitionState != null) {
        val highWatermark = partitionState.fetchOffset
        val truncationState = OffsetTruncationState(highWatermark, truncationCompleted = true)

        info(s"Truncating partition $tp to local high watermark $highWatermark")
        if (doTruncate(tp, truncationState))
          fetchOffsets.put(tp, truncationState)
      }
    }

    updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
  }

  private def maybeTruncateToEpochEndOffsets(fetchedEpochs: Map[TopicPartition, EpochEndOffset],
                                             latestEpochsForPartitions: Map[TopicPartition, EpochData]): ResultWithPartitions[Map[TopicPartition, OffsetTruncationState]] = {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]
    val partitionsWithError = mutable.HashSet.empty[TopicPartition]

    fetchedEpochs.foreach { case (tp, leaderEpochOffset) =>
      leaderEpochOffset.error match {
        case Errors.NONE =>
          val offsetTruncationState = getOffsetTruncationState(tp, leaderEpochOffset)
          if(doTruncate(tp, offsetTruncationState))
            fetchOffsets.put(tp, offsetTruncationState)

        case Errors.FENCED_LEADER_EPOCH =>
          if (onPartitionFenced(tp, latestEpochsForPartitions.get(tp).flatMap {
            p =>
              if (p.currentLeaderEpoch.isPresent) Some(p.currentLeaderEpoch.get())
              else None
          })) partitionsWithError += tp

        case error =>
          info(s"Retrying leaderEpoch request for partition $tp as the leader reported an error: $error")
          partitionsWithError += tp
      }
    }

    ResultWithPartitions(fetchOffsets, partitionsWithError)
  }

  private def processFetchRequest(fetchStates: Map[TopicPartition, PartitionFetchState],
                                  fetchRequest: FetchRequest.Builder): Unit = {
    val partitionsWithError = mutable.Set[TopicPartition]()
    var responseData: Seq[(TopicPartition, FetchData)] = Seq.empty

    try {
      trace(s"Sending fetch request $fetchRequest")
      responseData = fetchFromLeader(fetchRequest)
    } catch {
      case t: Throwable =>
        // if (isRunning) { removing isRunning since it must be true
        warn(s"Error in response for fetch request $fetchRequest", t)
        fetcherStats.requestFailureRate.mark()

        partitionsWithError ++= partitionStates.partitionSet.asScala
        // there is an error occurred while fetching partitions, sleep a while
        // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
        // partition with error effectively doubling the delay. It would be good to improve this.
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)

        // }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data
        responseData.foreach { case (topicPartition, partitionData) =>
          Option(partitionStates.stateValue(topicPartition)).foreach { currentFetchState =>
            // It's possible that a partition is removed and re-added or truncated when there is a pending fetch request.
            // In this case, we only want to process the fetch response if the partition state is ready for fetch and
            // the current offset is the same as the offset requested.
            val fetchState = fetchStates(topicPartition)
            if (fetchState.fetchOffset == currentFetchState.fetchOffset && currentFetchState.isReadyForFetch) {
              val requestEpoch = if (fetchState.currentLeaderEpoch >= 0)
                Some(fetchState.currentLeaderEpoch)
              else
                None
              partitionData.error match {
                case Errors.NONE =>
                  try {
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    val logAppendInfoOpt = processPartitionData(topicPartition, currentFetchState.fetchOffset,
                      partitionData)

                    logAppendInfoOpt.foreach { logAppendInfo =>
                      val validBytes = logAppendInfo.validBytes
                      val nextOffset = if (validBytes > 0) logAppendInfo.lastOffset + 1 else currentFetchState.fetchOffset
                      fetcherLagStats.getAndMaybePut(topicPartition).lag = Math.max(0L, partitionData.highWatermark - nextOffset)

                      // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
                      if (validBytes > 0 && partitionStates.contains(topicPartition)) {
                        // Update partitionStates only if there is no exception during processPartitionData
                        val newFetchState = PartitionFetchState(nextOffset, fetchState.currentLeaderEpoch,
                          state = Fetching)
                        partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
                        fetcherStats.byteRate.mark(validBytes)
                      }
                    }
                  } catch {
                    case ime@( _: CorruptRecordException | _: InvalidRecordException) =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread
                      //    down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes
                      //    can cause this), we simply continue and should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition " +
                        s"offset ${currentFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", e)
                      markPartitionFailed(topicPartition)
                    case t: Throwable =>
                      // stop monitoring this partition and add it to the set of failed partitions
                      error(s"Unexpected error occurred while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", t)
                      markPartitionFailed(topicPartition)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  if (handleOutOfRangeError(topicPartition, currentFetchState, requestEpoch))
                    partitionsWithError += topicPartition

                case Errors.UNKNOWN_LEADER_EPOCH =>
                  debug(s"Remote broker has a smaller leader epoch for partition $topicPartition than " +
                    s"this replica's current leader epoch of ${fetchState.currentLeaderEpoch}.")
                  partitionsWithError += topicPartition

                case Errors.FENCED_LEADER_EPOCH =>
                  if (onPartitionFenced(topicPartition, requestEpoch)) partitionsWithError += topicPartition

                case Errors.NOT_LEADER_FOR_PARTITION =>
                  debug(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  partitionsWithError += topicPartition

                case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
                  warn(s"Remote broker does not host the partition $topicPartition, which could indicate " +
                    "that the partition is being created or deleted.")
                  partitionsWithError += topicPartition

                case _ =>
                  error(s"Error for partition $topicPartition at offset ${currentFetchState.fetchOffset}",
                    partitionData.error.exception)
                  partitionsWithError += topicPartition
              }
            }
          }
        }

    }

    if (partitionsWithError.nonEmpty) {
      handlePartitionsWithErrors(partitionsWithError, "processFetchRequest")
    }
  }

  private def markPartitionFailed(topicPartition: TopicPartition): Unit = {
    partitionMapLock.lock()
    try {
      failedPartitions.add(topicPartition)
      removePartitions(Set(topicPartition))
    } finally partitionMapLock.unlock()
    warn(s"Partition $topicPartition marked as failed")
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]): Set[TopicPartition] = {
    failedPartitions.removeAll(initialFetchStates.keySet)

    initialFetchStates.foreach { case (tp, initialFetchState) =>
      // We can skip the truncation step iff the leader epoch matches the existing epoch
      val currentState = partitionStates.stateValue(tp)
      val updatedState = if (currentState != null && currentState.currentLeaderEpoch == initialFetchState.leaderEpoch) {
        currentState
      } else {
        val initialFetchOffset = if (initialFetchState.offset < 0)
          fetchOffsetAndTruncate(tp, initialFetchState.leaderEpoch)
        else
          initialFetchState.offset
        PartitionFetchState(initialFetchOffset, initialFetchState.leaderEpoch, state = Truncating)
      }
      partitionStates.updateAndMoveToEnd(tp, updatedState)
    }

    maybeUpdateIdleFlag()
    initialFetchStates.keySet
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  protected def fetchOffsetAndTruncate(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    val replicaEndOffset = logEndOffset(topicPartition)

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset = fetchLatestOffsetFromLeader(topicPartition, currentLeaderEpoch)
    if (leaderEndOffset < replicaEndOffset) {
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's latest offset $leaderEndOffset")
      truncate(topicPartition, OffsetTruncationState(leaderEndOffset, truncationCompleted = true))
      leaderEndOffset
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       */
      val leaderStartOffset = fetchEarliestOffsetFromLeader(topicPartition, currentLeaderEpoch)
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's start offset $leaderStartOffset")
      val offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replicaEndOffset)
        truncateFullyAndStartAt(topicPartition, leaderStartOffset)
      offsetToFetch
    }
  }

  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {
    for (partition <- partitions) {
      Option(partitionStates.stateValue(partition)).foreach { currentFetchState =>
        if (!currentFetchState.isDelayed) {
          partitionStates.updateAndMoveToEnd(partition, PartitionFetchState(currentFetchState.fetchOffset,
            currentFetchState.currentLeaderEpoch, new DelayedItem(delay), currentFetchState.state))
        }
      }
    }
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): Unit = {
    topicPartitions.foreach { topicPartition =>
      partitionStates.remove(topicPartition)
      fetcherLagStats.unregister(topicPartition)
    }
    maybeUpdateIdleFlag()
  }

  // This method should only be called when holding the partitionMapLock
  private def maybeUpdateIdleFlag(): Unit = {
    idle = partitionStates.size() <= 0
  }

}
