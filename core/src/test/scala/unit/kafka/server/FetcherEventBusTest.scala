package unit.kafka.server

import kafka.server.{AddPartitions, FetcherEvent, FetcherEventBus, FetcherState, QueuedFetcherEvent}
import kafka.utils.MockTime
import org.easymock.EasyMock
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

  }

}
