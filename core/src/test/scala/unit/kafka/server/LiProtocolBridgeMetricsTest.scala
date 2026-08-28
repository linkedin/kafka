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
import kafka.utils.TestUtils
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.jdk.CollectionConverters._

class LiProtocolBridgeMetricsTest {

  @Test
  def testMetricsFollowDynamicFlags(): Unit = {
    val brokerId = 987654
    val config = KafkaConfig(TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect))
    val metrics = new LiProtocolBridgeMetrics(config)

    try {
      assertTrue(metricValues(brokerId).values.forall(_ == 0))

      val props = new Properties
      Seq(
        KafkaConfig.LiProtocolBridgeModeEnableProp,
        KafkaConfig.LiProtocolBridgeFollowerRecoveryEnableProp,
        KafkaConfig.LiProtocolBridgeRecommendedElectionEnableProp,
        KafkaConfig.LiProtocolBridgeExcludePartitionsEnableProp,
        KafkaConfig.LiProtocolBridgeMoveControllerEnableProp,
        KafkaConfig.LiProtocolBridgeShutdownSafetyOverrideEnableProp
      ).foreach(props.put(_, "true"))
      config.dynamicConfig.updateDefaultConfig(props)

      val updatedValues = metricValues(brokerId)
      assertEquals(LiProtocolBridgeMetrics.MetricNames.toSet, updatedValues.keySet)
      assertEquals(0, updatedValues(LiProtocolBridgeMetrics.PreferredControllerEnabled))
      assertTrue(updatedValues.filterNot(_._1 == LiProtocolBridgeMetrics.PreferredControllerEnabled)
        .values.forall(_ == 1))
    } finally {
      metrics.close()
    }
    assertTrue(metricValues(brokerId).isEmpty)
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
