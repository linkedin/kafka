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

import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.utils.Time
import java.util.{Comparator, PriorityQueue}
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import kafka.utils.DelayedItem

import scala.util.control.Breaks.{break, breakable}


class QueuedFetcherEvent(val event: FetcherEvent,
  val enqueueTimeMs: Long) extends Comparable[QueuedFetcherEvent] {
  override def compareTo(other: QueuedFetcherEvent): Int = event.compareTo(other.event)
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
   * peek can be used to get the earliest item that has become current.
   * There are 3 cases when peek() is called
   * 1. There are no items whatsoever.  peek would return (None, Long.MaxValue) to indicate that the caller needs to wait
   *    indefinitely until an item is inserted.
   * 2. There are items, and yet none has become current. peek would return (None, delay) where delay represents
   *    the time to wait before the earliest item becomes current.
   * 3. Some item has become current. peek would return (Some(item), 0L)
   */
  def peek(): (Option[T], Long) = {
    if (delayedQueue.isEmpty) {
      (None, Long.MaxValue)
    } else {
      val delayedEvent = delayedQueue.peek()
      val delayMs = delayedEvent.getDelay(TimeUnit.MILLISECONDS)
      if (delayMs == 0) {
        (Some(delayedQueue.peek()), 0L)
      } else {
        (None, delayMs)
      }
    }
  }

  /**
   * poll() unconditionally removes the earliest item
   * If there are no items, poll() has no effect.
   */
  def poll(): Unit = {
    delayedQueue.poll()
  }

  def size = delayedQueue.size
}

/**
 * The FetcherEventBus supports queued events and delayed events.
 * Queued events are inserted via the {@link #put} method, and delayed events
 * are inserted via the {@link #schedule} method.
 * Events are polled via the {@link #getNextEvent} method, which returns
 * either a queued event or a scheduled event.
 * @param time
 */
class FetcherEventBus(time: Time) {
  private val eventLock = new ReentrantLock()
  private val newEventCondition = eventLock.newCondition()

  private val queue = new PriorityQueue[QueuedFetcherEvent]
  private val scheduler = new SimpleScheduler[DelayedFetcherEvent]
  @volatile private var shutdownInitialized = false

  def eventQueueSize() = queue.size

  def scheduledEventQueueSize() = scheduler.size

  /**
   * close should be called in a thread different from the one calling getNextEvent()
   */
  def close(): Unit = {
    shutdownInitialized = true
    inLock(eventLock) {
      newEventCondition.signalAll()
    }
  }

  def put(event: FetcherEvent): Unit = {
    inLock(eventLock) {
      queue.add(new QueuedFetcherEvent(event, time.milliseconds()))
      newEventCondition.signalAll()
    }
  }

  def schedule(delayedEvent: DelayedFetcherEvent): Unit = {
    inLock(eventLock) {
      scheduler.schedule(delayedEvent)
      newEventCondition.signalAll()
    }
  }

  /**
   * There are 3 cases when the getNextEvent() method is called
   * 1. There is at least one delayed event that has become current. If so, we return the delayed event with the earliest
   * due time.
   * 2. There is at least one queued event. If so, we return the queued event with the highest priority.
   * 3. There are neither delayed events that have become current, nor queued events. We block until the earliest delayed
   * event becomes current. A special case is that there are no delayed events at all, under which the call would block
   * indefinitely until it is waken up by a new delayed or queued event.
   *
   * @return Either a QueuedFetcherEvent or a DelayedFetcherEvent that has become current. A special case is that the
   *         FetcherEventBus is shutdown before an event can be polled, under which null will be returned.
   */
  def getNextEvent(): Either[QueuedFetcherEvent, DelayedFetcherEvent] = {
    inLock(eventLock) {
      var result : Either[QueuedFetcherEvent, DelayedFetcherEvent] = null

      breakable {
        while (!shutdownInitialized) {
          val (delayedFetcherEvent, delayMs) = scheduler.peek()
          if (delayedFetcherEvent.nonEmpty) {
            scheduler.poll()
            result = Right(delayedFetcherEvent.get)
            break
          } else if (!queue.isEmpty) {
            result = Left(queue.poll())
            break
          } else {
            newEventCondition.await(delayMs, TimeUnit.MILLISECONDS)
          }
        }
      }

      result
    }
  }
}
