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

package kafka.controller

import java.util.Properties
import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.server.{BrokerFeatures, DelegationTokenManager, KafkaConfig}
import kafka.server.metadata.ZkMetadataCache
import kafka.utils.TestUtils
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.config.ConfigType
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, times, verify, verifyNoMoreInteractions, when}

class LiShutdownSafetyTest {
  @Test
  def testTopicConfigsAreReadOncePerShutdownCheck(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect))
    val zkClient = mock(classOf[KafkaZkClient])
    val controller = new KafkaController(config, zkClient, new MockTime, mock(classOf[Metrics]),
      mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
      mock(classOf[ZkMetadataCache]))
    try {
      val context = controller.controllerContext
      context.setLiveBrokers(Map(Broker(0, Seq.empty, None) -> 1L, Broker(1, Seq.empty, None) -> 2L))
      (0 until 100).foreach { index =>
        val partition = new TopicPartition("orders", index)
        context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(0, 1)))
        context.putPartitionLeadershipInfo(partition,
          LeaderIsrAndControllerEpoch(LeaderAndIsr(0, List(0, 1)), 1))
      }
      val topicConfig = new Properties
      when(zkClient.getEntitiesConfigs(ConfigType.TOPIC, Set("orders")))
        .thenReturn(Map("orders" -> topicConfig))
      assertTrue(controller.safeToShutdown(0))
      verify(zkClient, times(1)).getEntitiesConfigs(ConfigType.TOPIC, Set("orders"))

      topicConfig.setProperty("min.insync.replicas", "2")
      assertFalse(controller.safeToShutdown(0))
      verify(zkClient, times(2)).getEntitiesConfigs(ConfigType.TOPIC, Set("orders"))
      context.shuttingDownBrokerIds.add(0)
      assertTrue(controller.safeToShutdown(0))
      verifyNoMoreInteractions(zkClient)
    } finally controller.shutdown()
  }
}
