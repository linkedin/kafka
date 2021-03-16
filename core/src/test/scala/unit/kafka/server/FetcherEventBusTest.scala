package unit.kafka.server

import kafka.server
import kafka.server.{AddPartitions, DelayedFetcherEvent, FetcherEvent, FetcherEventBus, QueuedFetcherEvent, RemovePartitions, TruncateAndFetch}
import kafka.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

import java.util.concurrent.{CountDownLatch, Executors}
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


class FetcherEventBusTest {
  @Test
  def testGetWhileEmpty(): Unit = {
    // test the getNextEvent method while the fetcherEventBus is empty
    val fetcherEventBus = new FetcherEventBus(new MockTime())

    val service = Executors.newSingleThreadExecutor()
    @volatile var counter = 0
    val runnableFinished = new CountDownLatch(1)
    service.submit(new Runnable {
      override def run(): Unit = {
        // trying to call get will block indefinitely
        fetcherEventBus.getNextEvent()
        counter = 1
        runnableFinished.countDown()
      }
    })

    // sleep for 500ms, during which the runnable should still be blocked
    Thread.sleep(500)
    assertTrue(counter == 0)

    // put a event to unblock the runnable
    fetcherEventBus.put(AddPartitions(Map.empty, null))
    service.shutdown()
    runnableFinished.await()
    assertTrue(counter == 1)
  }

  @Test
  def testQueuedEvent(): Unit = {
    val fetcherEventBus = new FetcherEventBus(new MockTime())
    val addPartitions = AddPartitions(Map.empty, null)
    fetcherEventBus.put(addPartitions)
    fetcherEventBus.getNextEvent() match {
      case Left(queuedFetcherEvent: QueuedFetcherEvent) =>
        assertTrue(queuedFetcherEvent.event == addPartitions)
      case Right(_) => fail("a QueuedFetcherEvent should have been returned")
    }
  }

  @Test
  def testQueuedEventsWithDifferentPriorities(): Unit = {
    val fetcherEventBus = new FetcherEventBus(new MockTime())

    val lowPriorityTask = TruncateAndFetch
    fetcherEventBus.put(lowPriorityTask)

    val highPriorityTask = AddPartitions(Map.empty, null)
    fetcherEventBus.put(highPriorityTask)

    val expectedSequence = Seq(highPriorityTask, lowPriorityTask)

    val actualSequence = ArrayBuffer[FetcherEvent]()

    for (_ <- 0 until 2) {
      fetcherEventBus.getNextEvent() match {
        case Left(queuedFetcherEvent: QueuedFetcherEvent) =>
          actualSequence += queuedFetcherEvent.event
        case Right(_) => fail("a QueuedFetcherEvent should have been returned")
      }
    }

    assertEquals(expectedSequence, actualSequence)
  }

  @Test
  def testDelayedEvent(): Unit = {
    val time = Time.SYSTEM
    val fetcherEventBus = new FetcherEventBus(time)
    val addPartitions = AddPartitions(Map.empty, null)
    val delay = 500
    fetcherEventBus.schedule(new DelayedFetcherEvent(delay, addPartitions))

    val service = Executors.newSingleThreadExecutor()

    val t1 = time.milliseconds()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        // trying to call get will block for at least 500ms
        fetcherEventBus.getNextEvent() match {
          case Left(_) => fail("a DelayedFetcherEvent should have been returned")
          case Right(delayedFetcherEvent: DelayedFetcherEvent) => {
            assertTrue(delayedFetcherEvent.fetcherEvent == addPartitions)
          }
        }
        // verify that at least 500ms has passed

        val t2 = time.milliseconds()
        assertTrue(t2 - t1 >= delay)
      }
    })

    future.get()
    service.shutdown()
  }

  @Test
  def testDelayedEventsWithDifferentDueTimes(): Unit = {
    val time = Time.SYSTEM
    val fetcherEventBus = new FetcherEventBus(time)
    val secondTask = AddPartitions(Map.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(200, secondTask))

    val firstTask = RemovePartitions(Set.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(100, firstTask))

    val service = Executors.newSingleThreadExecutor()

    val expectedSequence = Seq(firstTask, secondTask)

    val actualSequence = ArrayBuffer[FetcherEvent]()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 0 until 2) {
          fetcherEventBus.getNextEvent() match {
            case Left(_) => fail("a DelayedFetcherEvent should have been returned")
            case Right(delayedFetcherEvent: DelayedFetcherEvent) => {
              actualSequence += delayedFetcherEvent.fetcherEvent
            }
          }
        }
      }
    })

    future.get()
    assertEquals(expectedSequence, actualSequence)
    service.shutdown()
  }

  @Test
  def testBothDelayedAndQueuedEvent(): Unit = {
    val time = Time.SYSTEM
    val fetcherEventBus = new FetcherEventBus(time)

    val queuedEvent = RemovePartitions(Set.empty, null)
    fetcherEventBus.put(queuedEvent)

    val delay = 10
    val scheduledEvent = AddPartitions(Map.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(delay, scheduledEvent))

    val service = Executors.newSingleThreadExecutor()

    @volatile var receivedEvents = 0
    val expectedEvents = 2
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 0 until expectedEvents) {
          fetcherEventBus.getNextEvent() match {
            case Left(queuedFetcherEvent: QueuedFetcherEvent) => {
              assertTrue(queuedFetcherEvent.event == queuedEvent)
              receivedEvents += 1
            }
            case Right(delayedFetcherEvent: DelayedFetcherEvent) => {
              assertTrue(delayedFetcherEvent.fetcherEvent == scheduledEvent)
              receivedEvents += 1
            }
          }
        }
      }
    })

    future.get()
    assertTrue(receivedEvents == expectedEvents)
    service.shutdown()
  }
}
