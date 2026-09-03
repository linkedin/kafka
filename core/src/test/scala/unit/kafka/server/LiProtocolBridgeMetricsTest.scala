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

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaYammerMetrics
import kafka.utils.TestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

class LiProtocolBridgeMetricsTest {
  @Test
  def testEffectiveValuesAndCleanup(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(4, "localhost:2181"))
    val metrics = new LiProtocolBridgeMetrics(config)
    try {
      val values = metricValues(4)
      assertEquals(LiProtocolBridgeMetrics.MetricNames.toSet, values.keySet)
      assertEquals(0, values(LiProtocolBridgeMetrics.ModeEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.FollowerRecoveryEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.ControllerInitializationThreads))
      assertEquals(0, values(LiProtocolBridgeMetrics.ProduceRequestInstrumentationEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.RequestMetricBucketsEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.RequestChannelWatchdogEnabled))
      assertEquals(0, values(LiProtocolBridgeMetrics.MinimumLogRollEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.ReassignmentCancellationSafetyEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.ListOffsetsInstrumentationEnabled))
      assertEquals(0, values(LiProtocolBridgeMetrics.StaticDefaultQuotasEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.ReplicaRequestTimeoutEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.OffsetsTopicConfigEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.LeaderTransferEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.LegacyRequestMetricsEnabled))
      assertEquals(1, values(LiProtocolBridgeMetrics.LogTruncationMetricsEnabled))
    } finally {
      metrics.close()
    }
    assertTrue(metricValues(4).isEmpty)
  }

  @Test
  def testModeGaugeTracksDynamicConfig(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(5, "localhost:2181"))
    val metrics = new LiProtocolBridgeMetrics(config)
    try {
      assertEquals(0, metricValues(5)(LiProtocolBridgeMetrics.ModeEnabled))
      val props = new java.util.Properties
      props.put(KafkaConfig.LiProtocolBridgeModeEnableProp, "true")
      config.dynamicConfig.updateDefaultConfig(props)
      assertEquals(1, metricValues(5)(LiProtocolBridgeMetrics.ModeEnabled))
    } finally {
      metrics.close()
    }
  }

  @Test
  def testMalformedMinimumLogRollValueDoesNotBreakGauge(): Unit = {
    val props = TestUtils.createBrokerConfig(6, "localhost:2181")
    props.put(KafkaConfig.LiMinLogRollTimeMillisProp, "not-a-number")
    val metrics = new LiProtocolBridgeMetrics(KafkaConfig.fromProps(props))
    try {
      assertEquals(0, metricValues(6)(LiProtocolBridgeMetrics.MinimumLogRollEnabled))
    } finally {
      metrics.close()
    }
  }

  private def metricValues(brokerId: Int): Map[String, Int] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collect {
      case (name, gauge: Gauge[_])
        if name.getMBeanName.contains("type=LiProtocolBridgeMetrics") &&
          name.getMBeanName.contains(s"broker-id=$brokerId") =>
        name.getName -> gauge.value.asInstanceOf[Int]
    }.toMap
  }
}
