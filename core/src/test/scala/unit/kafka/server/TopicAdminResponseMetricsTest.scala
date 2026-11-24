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

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.{Counter, MetricName}
import kafka.metrics.KafkaYammerMetrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.Test

class TopicAdminResponseMetricsTest {
  @Test
  def testCountsTopicResultsAndBucketsSuccessfulResponses(): Unit = {
    val brokerId = 40011
    val metrics = new TopicAdminResponseMetrics(brokerId, Array(0, 5, 20, 100))
    val registry = KafkaYammerMetrics.defaultRegistry()

    def requestTags(api: ApiKeys) = Map("broker-id" -> brokerId.toString, "request" -> api.name)

    def outcomeName(api: ApiKeys, outcome: String) =
      metrics.metricName("TopicResponses", requestTags(api) + ("outcome" -> outcome))

    val createFast = metrics.metricName("SuccessfulTopicResponseTime_Bin2_5To20Ms", requestTags(ApiKeys.CREATE_TOPICS))
    val createSlow = metrics.metricName("SuccessfulTopicResponseTime_Bin3_20To100Ms", requestTags(ApiKeys.CREATE_TOPICS))
    val deleteSlow = metrics.metricName("SuccessfulTopicResponseTime_Bin4_100MsGreater", requestTags(ApiKeys.DELETE_TOPICS))

    def count(name: MetricName): Long =
      registry.allMetrics().get(name).asInstanceOf[Counter].count()

    try {
      metrics.record(ApiKeys.CREATE_TOPICS, TimeUnit.MILLISECONDS.toNanos(19), Map(
        Errors.NONE -> Int.box(3),
        Errors.REQUEST_TIMED_OUT -> Int.box(2),
        Errors.INVALID_TOPIC_EXCEPTION -> Int.box(1)))
      metrics.record(ApiKeys.CREATE_TOPICS, TimeUnit.MILLISECONDS.toNanos(20), Map(Errors.NONE -> Int.box(1)))
      metrics.record(ApiKeys.DELETE_TOPICS, TimeUnit.MILLISECONDS.toNanos(200), Map(Errors.NONE -> Int.box(2)))

      assertEquals(4L, count(outcomeName(ApiKeys.CREATE_TOPICS, "success")))
      assertEquals(2L, count(outcomeName(ApiKeys.CREATE_TOPICS, "timed-out")))
      assertEquals(1L, count(outcomeName(ApiKeys.CREATE_TOPICS, "failed")))
      assertEquals(3L, count(createFast))
      assertEquals(1L, count(createSlow))

      assertEquals(2L, count(outcomeName(ApiKeys.DELETE_TOPICS, "success")))
      assertEquals(0L, count(outcomeName(ApiKeys.DELETE_TOPICS, "timed-out")))
      assertEquals(2L, count(deleteSlow))
    } finally {
      metrics.close()
    }

    assertNull(registry.allMetrics().get(outcomeName(ApiKeys.CREATE_TOPICS, "success")))
    assertNull(registry.allMetrics().get(createFast))
  }
}
