package unit.kafka.server

import kafka.server.{AddPartitions, DelayedFetcherEvent, FetcherEventBus, QueuedFetcherEvent, RemovePartitions}
import kafka.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import java.util.concurrent.{CountDownLatch, Executors}


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
