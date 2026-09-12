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

import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.utils.TestUtils
import kafka.zk.ZkVersion
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.{Collections, Optional, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

class LiReassignmentCancellationGateTest extends QuorumTestHarness {
  private var broker: KafkaServer = _

  @AfterEach
  override def tearDown(): Unit = {
    if (broker != null) broker.shutdown()
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testCancellationGateAtControllerRequestBoundary(enabled: Boolean): Unit = {
    val tp = new TopicPartition("cancel-gate", 0)
    val topicId = Some(Uuid.randomUuid())
    // Resume an existing move from [0,1] to [2], with only original replica 0 online.
    // Seeding before startup avoids a race with a destination catching up in the fixture.
    zkClient.createTopicAssignment(tp.topic, topicId, Map(tp -> Seq(0, 1)))
    zkClient.setTopicAssignment(tp.topic, topicId,
      Map(tp -> ReplicaAssignment(Seq(2, 0, 1), Seq(2), Seq(0, 1))))
    zkClient.createTopicPartitionStatesRaw(
      Map(tp -> LeaderIsrAndControllerEpoch(LeaderAndIsr(0, List(0)), 0)), ZkVersion.MatchAnyVersion)
      .foreach(_.maybeThrow())
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
    props.put(KafkaConfig.LiMinOriginalAliveReplicasProp, "2")
    if (enabled) props.put(KafkaConfig.LiProtocolBridgeReassignmentCancellationSafetyEnableProp, "true")
    broker = createBroker(KafkaConfig.fromProps(props)).asInstanceOf[KafkaServer]
    assertEquals(0, TestUtils.waitUntilControllerElected(zkClient))
    val adminProps = new Properties
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      TestUtils.bootstrapServers(Seq(broker), broker.config.interBrokerListenerName))
    val admin = Admin.create(adminProps)
    try {
      val result = admin.alterPartitionReassignments(
        Collections.singletonMap(tp, Optional.empty[NewPartitionReassignment]())).all()
      if (enabled) {
        val error = assertThrows(classOf[ExecutionException], () => result.get(10, TimeUnit.SECONDS))
        assertTrue(error.getCause.isInstanceOf[InvalidReplicaAssignmentException])
      } else result.get(10, TimeUnit.SECONDS)
      val assignment = zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic))(tp)
      assertEquals(if (enabled) Seq(2) else Seq(0, 1), assignment.targetReplicas)
    } finally admin.close()
  }
}
