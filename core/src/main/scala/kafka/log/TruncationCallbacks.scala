/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import org.apache.kafka.common.TopicPartition

trait TruncationObserver {
  def onTruncated(topicPartition: TopicPartition,
                  op: String,              // observer
                  target: Long,            // requested target offset
                  fromLeo: Long,           // LEO before truncation
                  toLeo: Long,             // LEO after truncation
                  messagesTruncated: Long, // number of messages removed
                  bytesTruncated: Long     // number of bytes removed
                 ): Unit
}

// Thread-local hook to attach an observer during a truncation
object TruncationCallbacks {
  private val observerTL = new ThreadLocal[TruncationObserver]()

  def withObserver[T](observer: TruncationObserver)(f: => T): T = {
    observerTL.set(observer)
    try f
    finally observerTL.remove()
  }

  private[log] def notify(tp: TopicPartition,
                          op: String,
                          target: Long,
                          fromLeo: Long,
                          toLeo: Long,
                          msgs: Long,
                          bytes: Long): Unit = {
    val obs = observerTL.get()
    if (obs != null) {
      obs.onTruncated(tp, op, target, fromLeo, toLeo, msgs, bytes)
    }
  }
}
