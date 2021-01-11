/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.ForkJoinPool

import kafka.utils.Logging
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import scala.collection.mutable

import scala.collection.parallel.CollectionConverters.ImmutableMapIsParallelizable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.{Map, Set}

abstract class AbstractFetcherManager[T <: AbstractFetcherThread](val name: String, clientId: String, numFetchers: Int)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private[server] val fetcherThreadMap = new scala.collection.parallel.mutable.ParHashMap[BrokerIdAndFetcherId, T]
  val forkJoinPool = new ForkJoinPool()
  fetcherThreadMap.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

  private val lock = new Object
  private val numFetchersPerBroker = numFetchers
  val failedPartitions = new FailedPartitions
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value: Long = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value: Double = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  newGauge(
    "FetchFailureRate", {
      new Gauge[Double] {
        // fetch failure rate sum across all fetchers/topics/partitions
        def value: Double = {
          val headRate: Double =
            fetcherThreadMap.headOption.map(_._2.fetcherStats.requestFailureRate.oneMinuteRate).getOrElse(0)

          fetcherThreadMap.foldLeft(headRate)((curSum, fetcherThreadMapEntry) => {
            fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate + curSum
          })
        }
      }
    },
    Map("clientId" -> clientId)
  )

  val failedPartitionsCount = newGauge(
    "FailedPartitionsCount", {
      new Gauge[Int] {
        def value: Int = failedPartitions.size
      }
    },
    Map("clientId" -> clientId)
  )

  newGauge("DeadThreadCount", {
    new Gauge[Int] {
      def value: Int = {
        deadThreadCount
      }
    }
  }, Map("clientId" -> clientId))

  private[server] def deadThreadCount: Int = lock synchronized { fetcherThreadMap.values.count(_.isThreadFailed) }

  def resizeThreadPool(newSize: Int): Unit = {
    /*
    def migratePartitions(newSize: Int): Unit = {
      info(s"total fetcherThreadMap size:${fetcherThreadMap.size}")
      var index = 0
      fetcherThreadMapView.foreach { entry =>
        val id = entry._1
        val thread = entry._2
        val removedPartitions = thread.partitionsAndOffsets
        removeFetcherForPartitions(removedPartitions.keySet)
        if (id.fetcherId >= newSize)
          thread.shutdown()
        addFetcherForPartitions(removedPartitions)
        info(s"done processing fetcherThreadMap entry $index")
        index += 1
      }
    }
    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize
      if (newSize != currentSize) {
        // We could just migrate some partitions explicitly to new threads. But this is currently
        // reassigning all partitions using the new thread size so that hash-based allocation
        // works with partition add/delete as it did before.
        migratePartitions(newSize)
      }
      shutdownIdleFetcherThreads()
    }
    */
  }

  // Visible for testing
  private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
    lock synchronized {
      fetcherThreadMap.values.find { fetcherThread =>
        fetcherThread.fetchState(topicPartition).isDefined
      }
    }
  }

  // Visibility for testing
  private[server] def getFetcherId(topicPartition: TopicPartition): Int = {
    lock synchronized {
      Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
    }
  }

  // This method is only needed by ReplicaAlterDirManager
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    lock synchronized {
      val fetcherId = getFetcherId(topicPartition)
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): T

  case class LwTimes(createThreadTime: Long, addPartitionsTime : Long)

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    lock synchronized {
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
      }

      def startFetcherThread(brokerAndFetcherId: BrokerAndFetcherId,
                                   brokerIdAndFetcherId: BrokerIdAndFetcherId): T = {
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        fetcherThread.start()
        fetcherThread
      }

      warn("# of entries in partitionsPerFetcher:" + partitionsPerFetcher.size)
      @volatile var times : List[LwTimes] = List.empty
      val fetcherIdAndUpdatedThreads = for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher.par) yield {
        info(s"adding partitions to fetcher $brokerAndFetcherId")
        val fetcherStart = System.currentTimeMillis()
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
            // reuse the fetcher thread
            currentFetcherThread
          case Some(f) =>
            f.shutdown()
            startFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None =>
            startFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }

        val createThreadFinishTime = System.currentTimeMillis()
        val initialOffsetAndEpochs = initialFetchOffsets.map { case (tp, brokerAndInitOffset) =>
          tp -> OffsetAndEpoch(brokerAndInitOffset.initOffset, brokerAndInitOffset.currentLeaderEpoch)
        }

        addPartitionsToFetcherThread(fetcherThread, initialOffsetAndEpochs)
        val addPartitionsFinishTime = System.currentTimeMillis()
        times = (LwTimes(createThreadFinishTime - fetcherStart, addPartitionsFinishTime - createThreadFinishTime)) :: times
        (brokerIdAndFetcherId, fetcherThread)
      }

      fetcherThreadMap.addAll(fetcherIdAndUpdatedThreads)
      val createThreadTimes = times.map(_.createThreadTime)
      val addPartitionTimes = times.map(_.addPartitionsTime)
      warn("entries in times " + times.size)
      warn("average create thread times " + (getAvg(createThreadTimes)) + " and the total time is " + createThreadTimes.sum)
      warn("average add partition times " + (getAvg(addPartitionTimes)) + " and the total time is " + addPartitionTimes.sum)
    }
  }

  def getAvg(times: List[Long]) = {
    times.sum / times.length
  }

  protected def addPartitionsToFetcherThread(fetcherThread: T,
                                             initialOffsetAndEpochs: collection.Map[TopicPartition, OffsetAndEpoch]): Unit = {
    fetcherThread.addPartitions(initialOffsetAndEpochs)
    info(s"Added fetcher to broker ${fetcherThread.sourceBroker.id} for partitions $initialOffsetAndEpochs")
  }

  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Unit = {
    lock synchronized {
      for (fetcher <- fetcherThreadMap.values)
        fetcher.removePartitions(partitions)
      failedPartitions.removeAll(partitions)
    }
    if (partitions.nonEmpty)
      info(s"Removed fetcher for partitions $partitions")
  }

  def shutdownIdleFetcherThreads(): Unit = {
    lock synchronized {
      val keysToBeRemoved = for ((key, fetcher) <- fetcherThreadMap if fetcher.idle) yield {
        fetcher.shutdown()
        key
      }

      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers(): Unit = {
    lock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
      forkJoinPool.shutdown()
    }
  }
}

/**
  * The class FailedPartitions would keep a track of partitions marked as failed either during truncation or appending
  * resulting from one of the following errors -
  * <ol>
  *   <li> Storage exception
  *   <li> Fenced epoch
  *   <li> Unexpected errors
  * </ol>
  * The partitions which fail due to storage error are eventually removed from this set after the log directory is
  * taken offline.
  */
class FailedPartitions {
  private val failedPartitionsSet = new mutable.HashSet[TopicPartition]

  def size: Int = synchronized {
    failedPartitionsSet.size
  }

  def add(topicPartition: TopicPartition): Unit = synchronized {
    failedPartitionsSet += topicPartition
  }

  def removeAll(topicPartitions: Set[TopicPartition]): Unit = synchronized {
    failedPartitionsSet --= topicPartitions
  }

  def contains(topicPartition: TopicPartition): Boolean = synchronized {
    failedPartitionsSet.contains(topicPartition)
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class InitialFetchState(leader: BrokerEndPoint, currentLeaderEpoch: Int, initOffset: Long)

case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
