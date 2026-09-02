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

package kafka.server

import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.concurrent.{Callable, CountDownLatch, Executors}
import scala.jdk.CollectionConverters._

class ListOffsetsRequestInstrumentationTest {
  @Test
  def testDisabledInstrumentationCreatesNoMetricsOrUsageState(): Unit = {
    val before = instrumentationMetricNames
    val instrumentation = new ListOffsetsRequestInstrumentation(enabled = false)
    assertEquals(before, instrumentationMetricNames)
    assertTrue(instrumentation.snapshotAndResetListOffsetByTimeStampApiUsers().isEmpty)
  }

  @Test
  def testEnabledInstrumentationRemovesMetricsOnClose(): Unit = {
    val before = instrumentationMetricNames
    val instrumentation = new ListOffsetsRequestInstrumentation(enabled = true)
    assertTrue(instrumentationMetricNames.size > before.size)
    instrumentation.close()
    assertEquals(before, instrumentationMetricNames)
  }

  @Test
  def testConcurrentUsageTrackingDoesNotLoseRequests(): Unit = {
    val instrumentation = new ListOffsetsRequestInstrumentation(enabled = true)
    val executor = Executors.newFixedThreadPool(8)
    val start = new CountDownLatch(1)
    val requestsPerThread = 1000
    val topic = new ListOffsetsTopic()
      .setName("topic")
      .setPartitions(Seq(new ListOffsetsPartition().setTimestamp(1L)).asJava)
    try {
      val tasks = (1 to 8).map { _ =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            start.await()
            (1 to requestsPerThread).foreach(_ =>
              instrumentation.logUsage(KafkaPrincipal.ANONYMOUS, topic))
          }
        })
      }
      start.countDown()
      tasks.foreach(_.get())

      val snapshot = instrumentation.snapshotAndResetListOffsetByTimeStampApiUsers()
      assertEquals(8 * requestsPerThread,
        snapshot(KafkaPrincipal.ANONYMOUS.getName)("topic").get())
      assertTrue(instrumentation.snapshotAndResetListOffsetByTimeStampApiUsers().isEmpty)
    } finally {
      executor.shutdownNow()
      instrumentation.close()
    }
  }

  private def instrumentationMetricNames: Set[String] =
    KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .filter(_.getMBeanName.contains("type=ListOffsetsRequestInstrumentation"))
      .map(_.getMBeanName).toSet
}
