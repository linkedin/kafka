package kafka.controller

import kafka.server.{KafkaConfig, OffsetAndEpoch}
import kafka.utils.KafkaScheduler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractResponse, ListOffsetsRequest, ListOffsetsResponse}

import java.util
import java.util.concurrent.ScheduledFuture
import scala.collection.{Set, mutable}

class DelayedElectionManager(
  val config: KafkaConfig,
  val controllerBrokerId: Int,
  val controllerContext: ControllerContext,
  val eventManager: ControllerEventManager,
  val channelManager: ControllerChannelManager,
) {
  class DelayedElectionTask(
    val partition: TopicPartition,
    val onComplete: (DelayedElectionTask) => Unit
  ) {
    var isCancelled = false
    val brokerIdToOffsetAndEpochMap: mutable.Map[Int, OffsetAndEpoch] = mutable.Map[Int, OffsetAndEpoch]()
    private var electionFuture: Option[ScheduledFuture[_]] = None

    def addBrokerOffsetAndEpoch(brokerId: Int, offsetAndEpoch: OffsetAndEpoch): Unit = {
      brokerIdToOffsetAndEpochMap += brokerId -> offsetAndEpoch
    }

    def start(): Unit = {
      electionFuture = Some(scheduler.schedule(s"delayed-election-$partition", done, delay=electionWaitMs))
    }

    def cancel(): Unit = {
      electionFuture.foreach(_.cancel(false))
      isCancelled = true
      onComplete(this)
    }

    def done(): Unit = {
      onComplete(this)
    }
  }

  private val partitionToDelayedTaskMap = mutable.Map[TopicPartition, DelayedElectionTask]()
  private val electionWaitMs = config.liLeaderElectionOnCorruptionWaitMs
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "delayed-election-")

  private def onDelayedElectionDone(delayedElectionTask: DelayedElectionTask): Unit = {
    partitionToDelayedTaskMap.remove(delayedElectionTask.partition)
    if (!delayedElectionTask.isCancelled) {
      eventManager.put(DelayedElectionSuccess(
        delayedElectionTask.partition, delayedElectionTask.brokerIdToOffsetAndEpochMap))
    }
  }

  def onCorruptedBrokerStartup(brokerId: Int): Unit = {
    val partitionsAwaitingElection = partitionToDelayedTaskMap.keySet
    val partitionsOnBroker = controllerContext.partitionsOnBroker(brokerId)

    val partitionsToListOffsets = partitionsOnBroker.intersect(partitionsAwaitingElection)
    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    listOffsetsForOnlineReplica(controllerContextSnapshot, brokerId, partitionsToListOffsets)
  }

  def onListOffsetsResponse(brokerId: Int, listOffsetsResponse: ListOffsetsResponse): Unit = {
    val topics = listOffsetsResponse.topics()
    val partitionToOffsetAndEpochMap = mutable.Map[TopicPartition, OffsetAndEpoch]()

    topics.forEach(topic => {
      val partitions = topic.partitions()
      partitions.forEach(partition => {
        val topicPartition = new TopicPartition(topic.name(), partition.partitionIndex())
        val offsetAndEpoch = OffsetAndEpoch(partition.offset(), partition.leaderEpoch())
        partitionToOffsetAndEpochMap += topicPartition -> offsetAndEpoch
      })
    })

    partitionToOffsetAndEpochMap.filterKeys(partitionToDelayedTaskMap.contains).foreach {
      case (partition, offsetAndEpoch) =>
        partitionToDelayedTaskMap(partition).addBrokerOffsetAndEpoch(brokerId, offsetAndEpoch)
    }
  }

  def startDelayedElectionsForPartitions(partitionsWithCorruptedLeaders: Seq[TopicPartition]): Unit = {
    val corruptedPartitionsToAdd = partitionsWithCorruptedLeaders.toSet -- partitionToDelayedTaskMap.keySet
    val corruptedPartitionsToRemove = partitionToDelayedTaskMap.keySet -- partitionsWithCorruptedLeaders.toSet

    corruptedPartitionsToAdd.foreach(partition => {
      partitionToDelayedTaskMap.put(partition, new DelayedElectionTask(partition, onDelayedElectionDone))
    })

    corruptedPartitionsToRemove.foreach(partition => {
      partitionToDelayedTaskMap.remove(partition).foreach(delayedElectionTask => {
        delayedElectionTask.cancel()
      })
    })

    listOffsetsForPartitionsWithOnlineReplicas(corruptedPartitionsToAdd)
  }

  private def listOffsetsForPartitionsWithOnlineReplicas(partitions: Set[TopicPartition]): Unit = {
    val brokerToPartitionsMap = partitions.flatMap(partition =>
      controllerContext.partitionReplicaAssignment(partition)
        .filter(controllerContext.corruptedBrokers.contains)
        .map(replicaId => replicaId -> partition))
    .groupBy {
        case (replicaId, _) => replicaId
    }.mapValues (_.map {
      case (_, partition) => partition
    })

    val controllerContextSnapshot = ControllerContextSnapshot(controllerContext)
    brokerToPartitionsMap.foreach {
      case (brokerId, partitions) =>
        listOffsetsForOnlineReplica(controllerContextSnapshot, brokerId, partitions)
    }
  }

  private def listOffsetsForOnlineReplica(
    controllerContextSnapshot: ControllerContextSnapshot, brokerId: Int, partitions: Set[TopicPartition]): Unit = {
    val onlinePartitions =
      partitions.filter(partition => controllerContextSnapshot.isReplicaOnline(brokerId, partition))

    if (onlinePartitions.nonEmpty) {
      requestOffsetsFromBroker(brokerId, onlinePartitions)
    }
  }

  private def requestOffsetsFromBroker(brokerId: Int, partitions: Set[TopicPartition]): Unit = {
    val listOffsetsRequestBuilder: ListOffsetsRequest.Builder
    = buildListOffsetsRequest(partitions)

    def callback(response: AbstractResponse): Unit = {
      val listOffsetsResponse = response.asInstanceOf[ListOffsetsResponse]
      eventManager.put(CorruptedBrokerOffsetsReceived(brokerId, listOffsetsResponse))
    }
    channelManager.sendRequest(brokerId, listOffsetsRequestBuilder, callback)
  }

  private def buildListOffsetsRequest(
    partitions: Set[TopicPartition]): ListOffsetsRequest.Builder = {
    val partitionsByTopic = partitions.groupBy(_.topic())
    val listOffsetsTopics = new util.ArrayList[ListOffsetsTopic]()

    partitionsByTopic.foreach {
      case (topicName, partitions) =>
        val listOffsetsTopic = new ListOffsetsTopic()
          .setName(topicName)
          .setPartitions(new util.ArrayList())
        listOffsetsTopics.add(listOffsetsTopic)

        partitions.foreach(partition => {
          listOffsetsTopic.partitions().add(new ListOffsetsPartition()
            .setPartitionIndex(partition.partition())
            .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
            //.setCurrentLeaderEpoch()
          )
        })
        listOffsetsTopics.add(listOffsetsTopic)
    }

    val listOffsetsRequestBuilder = ListOffsetsRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion(), controllerBrokerId)
      .setTargetTimes(listOffsetsTopics)
    listOffsetsRequestBuilder
  }

  def addOffsetForBroker(partition: TopicPartition, brokerId: Int, offsetAndEpoch: OffsetAndEpoch): Unit = {
    partitionToDelayedTaskMap.get(partition).foreach(delayedElectionTask => {
      delayedElectionTask.addBrokerOffsetAndEpoch(brokerId, offsetAndEpoch)
    })
  }
}
