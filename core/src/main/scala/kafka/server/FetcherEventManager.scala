package kafka.server


import com.yammer.metrics.core.Gauge
import kafka.cluster.BrokerEndPoint
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Set}
import java.util.concurrent.{DelayQueue, Future, PriorityBlockingQueue}

trait FetcherEventProcessor {
  def process(event: FetcherEvent)
  def fetcherStats: FetcherStats
  def fetcherLagStats : FetcherLagStats
  def sourceBroker: BrokerEndPoint
}


class FetcherQueuedEvent(val event: FetcherEvent,
                         val enqueueTimeMs: Long)

object FetcherEventManager {
  val FetcherEventThreadName = "fetcher-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

class FetcherEventManager(fetcherId: String,
                          processor: FetcherEventProcessor,
                          time: Time,
                          rateAndTimeMetrics: Map[FetcherState, KafkaTimer]) extends KafkaMetricsGroup {
  import FetcherEventManager._

  @volatile private var _state: FetcherState = FetcherState.Idle
  private val queue = new PriorityBlockingQueue[FetcherQueuedEvent]
  private val delayedQueue = new DelayQueue[DelayedFetcherEvent]
  private[server] val thread = new FetcherEventThread(FetcherEventThreadName)

  def fetcherStats: FetcherStats = processor.fetcherStats
  def fetcherLagStats : FetcherLagStats = processor.fetcherLagStats
  def sourceBroker: BrokerEndPoint = processor.sourceBroker
  def isThreadFailed: Boolean = thread.isThreadFailed

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(
    EventQueueSizeMetricName,
    new Gauge[Int] {
      def value: Int = {
        queue.size()
      }
    }
  )

  def state: FetcherState = _state

  def start(): Unit = thread.start()

  def shutdown(): Unit = thread.shutdown()

  def addPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    put(AddPartitions(initialFetchStates, future))
    future
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): KafkaFuture[Void] = {
    val future = new KafkaFutureImpl[Void] {}
    put(RemovePartitions(topicPartitions, future))
    future
  }

  def getPartitionsCount(): KafkaFuture[Int] = {
    val future = new KafkaFutureImpl[Int]{}
    put(GetPartitionCount(future))
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
  }

  def put(event: FetcherEvent): FetcherQueuedEvent = {
    val queuedEvent = new FetcherQueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  def schedule(delayedEvent: DelayedFetcherEvent) = {
    delayedQueue.add(delayedEvent)
  }

  class FetcherEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[FetcherEventThread fetcherId=$fetcherId] "
    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    override def doWork(): Unit = {
      // TODO: add the logic to check the delayedQueue
      val dequeued = queue.take()
      dequeued.event match {
        case PoisonPill(future) =>

        case fetcherEvent =>
          _state = fetcherEvent.state

          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

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
}
