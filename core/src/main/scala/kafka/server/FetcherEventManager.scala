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
class FetcherEventManager(fetcherId: String,
                          processor: FetcherEventProcessor,
                          time: Time,
                          rateAndTimeMetrics: Map[FetcherState, KafkaTimer]) extends KafkaMetricsGroup {
  import FetcherEventManager._

  @volatile private var _state: FetcherState = FetcherState.Idle
  private val eventLock = new ReentrantLock()
  private val newEventCondition = eventLock.newCondition()

  private val queue = new PriorityQueue[QueuedFetcherEvent]
  private val scheduler = new SimpleScheduler[DelayedFetcherEvent]
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
        queue.size()
      }
    }
  )

  def state: FetcherState = _state

  def start(): Unit = thread.start()

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

  def put(event: FetcherEvent): QueuedFetcherEvent = {
    inLock(eventLock) {
      val queuedEvent = new QueuedFetcherEvent(event, time.milliseconds())
      queue.add(queuedEvent)
      newEventCondition.signalAll()
      queuedEvent
    }
  }

  def schedule(delayedEvent: DelayedFetcherEvent) = {
    inLock(eventLock) {
      scheduler.schedule(delayedEvent)
      newEventCondition.signalAll()
    }
  }

  class FetcherEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[FetcherEventThread fetcherId=$fetcherId] "

    /**
     * There are 3 cases when the getNextEvent() method is called
     * 1. There is at least one delayed event that has become current. If so, we return the delayed event with the earliest
     * due time.
     * 2. There is at least one event in the queue. If so, we return the event with the highest priority from the queue.
     * 3. There are neither delayed events that have become current, nor queued events. We block until the earliest delayed
     * event becomes current. A special case is that there are no delayed events at all, under which we would block
     * indefinitely until being explicitly waken up by a new delayed or queued event.
     *
     * @return Either a QueuedFetcherEvent or a DelayedFetcherEvent that has become current
     */
    private def getNextEvent(): Either[QueuedFetcherEvent, DelayedFetcherEvent] = {
      inLock(eventLock) {
        var result : Either[QueuedFetcherEvent, DelayedFetcherEvent] = null

        while (true) {
          val (delayedFetcherEvent, delayMs) = scheduler.peek() match {
            case Some(delayedEvent: DelayedFetcherEvent) => {
              val delayMs = delayedEvent.getDelay(TimeUnit.MILLISECONDS)
              if (delayMs == 0) {
                (Some(delayedEvent), 0L)
              } else {
                (None, delayMs)
              }
            }
            case _ => (None, Long.MaxValue)
          }

          if (delayedFetcherEvent.nonEmpty) {
            result = Right(delayedFetcherEvent.get)
            break
          } else if (!queue.isEmpty) {
            result = Left(queue.poll())
            break
          } else {
            newEventCondition.wait(delayMs)
          }
        }

        result
      }
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    override def doWork(): Unit = {
      val (fetcherEvent, optionalEnqueueTime) = getNextEvent() match {
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
