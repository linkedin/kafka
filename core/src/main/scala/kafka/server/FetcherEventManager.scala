package kafka.server


import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.{DelayedItem, ShutdownableThread}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaFuture, TopicPartition}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.{Comparator, PriorityQueue}
import scala.collection.{Map, Set}
import scala.util.control.Breaks.break

trait FetcherEventProcessor {
  def process(event: FetcherEvent)
  def fetcherStats: AsyncFetcherStats
  def fetcherLagStats : AsyncFetcherLagStats
  def sourceBroker: BrokerEndPoint
  def close(): Unit
}


class QueuedFetcherEvent(val event: FetcherEvent,
                         val enqueueTimeMs: Long)

object FetcherEventManager {
  val FetcherEventThreadName = "fetcher-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

/**
 * The SimpleScheduler is not thread safe
 */
class SimpleScheduler[T <: DelayedItem] {
  private val delayedQueue = new PriorityQueue[T](new Comparator[T]() {
    override def compare(t1: T, t2: T): Int = {
      // here we use natural ordering so that events with the earliest due time can be checked first
      t1.compareTo(t2)
    }
  })

  def schedule(item: T) : Unit = {
    delayedQueue.add(item)
  }

  /**
   * @return Peek() returns the item with the earliest due time if there is at least one item.
   *         Otherwise if there are no items, it returns None.
   */
  def peek(): Option[T] = {
    if (delayedQueue.isEmpty) {
      None
    } else {
      Some(delayedQueue.poll())
    }
  }
}

// TODO: add locks to the addPartitions, removePartitions, getPartitionsCount methods
class FetcherEventManager(name: String,
                          fetcherEventBus: FetcherEventBus,
                          processor: FetcherEventProcessor,
                          time: Time) extends KafkaMetricsGroup {

  import FetcherEventManager._

  val rateAndTimeMetrics: Map[FetcherState, KafkaTimer] = FetcherState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

  @volatile private var _state: FetcherState = FetcherState.Idle
  private[server] val thread = new FetcherEventThread(FetcherEventThreadName)

  def fetcherStats: AsyncFetcherStats = processor.fetcherStats
  def fetcherLagStats : AsyncFetcherLagStats = processor.fetcherLagStats
  def sourceBroker: BrokerEndPoint = processor.sourceBroker
  def isThreadFailed: Boolean = thread.isThreadFailed

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(
    EventQueueSizeMetricName,
    new Gauge[Int] {
      def value: Int = {
        fetcherEventBus.size()
      }
    }
  )

  def state: FetcherState = _state

  def start(): Unit = {
    fetcherEventBus.put(TruncateAndFetch)
    thread.start()
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    fetcherEventBus.put(AddPartitions(initialFetchStates, future))
    future
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    fetcherEventBus.put(RemovePartitions(topicPartitions, future))
    future
  }

  def getPartitionsCount(): KafkaFuture[Int] = {
    val future = new KafkaFutureImpl[Int]{}
    fetcherEventBus.put(GetPartitionCount(future))
    future
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }

    processor.close()
  }


  class FetcherEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[FetcherEventThread fetcherId=$name] "


    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    override def doWork(): Unit = {
      val (fetcherEvent, optionalEnqueueTime) = fetcherEventBus.getNextEvent() match {
        case Left(dequeued: QueuedFetcherEvent) =>
          (dequeued.event, Some(dequeued.enqueueTimeMs))

        case Right(delayedFetcherEvent: DelayedFetcherEvent) => {
          (delayedFetcherEvent.fetcherEvent, None)
        }
      }

      _state = fetcherEvent.state
      optionalEnqueueTime match {
        case Some(enqueueTimeMs) => {
          eventQueueTimeHist.update(time.milliseconds() - enqueueTimeMs)
        }
        case None =>
      }

      try {
        rateAndTimeMetrics(state).time {
          processor.process(fetcherEvent)
        }
      } catch {
        case e: Throwable => error(s"Uncaught error processing event $fetcherEvent", e)
      }

      _state = FetcherState.Idle
    }
  }
}
