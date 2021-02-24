package kafka.server

import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time
import scala.collection.Map

import java.util.concurrent.PriorityBlockingQueue

trait FetcherEventProcessor {
  def process(event: FetcherEvent)
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
  private[server] val thread = new FetcherEventThread(FetcherEventThreadName)

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

  class FetcherEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[FetcherEventThread fetcherId=$fetcherId] "
    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    override def doWork(): Unit = {
      val dequeued = queue.take()
      dequeued.event match {
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
