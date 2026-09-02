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

package kafka.network

import com.yammer.metrics.core.Histogram
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class RequestChannelWatchdogTest {
  @Test
  def testPollTimestampAndIntervalMetric(): Unit = {
    val time = new MockTime
    val channel = new RequestChannel(1, "", time, new RequestChannel.Metrics(Seq.empty),
      enableDequeueWatchdog = true)
    try {
      channel.receiveRequest(0)
      val firstPoll = channel.lastDequeueTimeMs
      time.sleep(25)
      channel.receiveRequest(0)
      assertEquals(firstPoll + 25, channel.lastDequeueTimeMs)

      val histogram = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collectFirst {
        case (name, value: Histogram) if name.getName == "RequestDequeuePollIntervalMs" => value
      }.get
      assertEquals(1L, histogram.count)
      assertEquals(25L, histogram.max)
    } finally {
      channel.shutdown()
    }
    assertFalse(KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .exists(_.getName == "RequestDequeuePollIntervalMs"))
  }
}
