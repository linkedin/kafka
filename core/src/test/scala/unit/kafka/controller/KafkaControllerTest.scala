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

package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.server.metadata.ZkMetadataCache
import kafka.server.{BrokerFeatures, DelegationTokenManager, KafkaConfig}
import kafka.utils.TestUtils
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{doAnswer, mock, mockConstruction, never, times, verify, verifyNoMoreInteractions, when}

class KafkaControllerTest {
  var config: KafkaConfig = _

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    config = KafkaConfig.fromProps(props)
  }

  @Test
  def testCompatibilityControllerMetricsRequireOptIn(): Unit = {
    for (enabled <- Seq(false, true)) {
      val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
      props.put(KafkaConfig.LiProtocolBridgeConfigMetricsEnableProp, enabled.toString)
      val construction = mockConstruction(classOf[KafkaMetricsGroup])
      try {
        val controller = new KafkaController(KafkaConfig.fromProps(props), mock(classOf[KafkaZkClient]),
          new MockTime(), mock(classOf[Metrics]), mock(classOf[BrokerInfo]), 0L,
          mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]), mock(classOf[ZkMetadataCache]))
        controller.shutdown()
        val group = construction.constructed.get(0)
        val diagnostics = Seq("ActivePreferredControllerCount", "StandbyPreferredControllerCount", "MaintenanceBrokerCount")
        diagnostics.foreach { name =>
          verify(group, times(if (enabled) 1 else 0)).newGauge(ArgumentMatchers.eq(name), any())
          verify(group, times(if (enabled) 1 else 0)).removeMetric(name)
        }
      } finally construction.close()
    }
  }

  @Test
  def testInterruptedDeletionRecoveryRetainsReplicasAndUnrelatedReassignments(): Unit = {
    for (enabled <- Seq(false, true); deletionEnabled <- Seq(false, true)) {
      val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
      props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, enabled.toString)
      props.put(KafkaConfig.DeleteTopicEnableProp, deletionEnabled.toString)
      val client = mock(classOf[KafkaZkClient])
      val controller = new KafkaController(KafkaConfig.fromProps(props), client, new MockTime(), mock(classOf[Metrics]),
        mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
        mock(classOf[ZkMetadataCache]))
      val deleted = new TopicPartition("deleted", 0)
      val live = new TopicPartition("live", 0)
      val withState = new TopicPartition("with-state", 0)
      val assignment = ReplicaAssignment(Seq(1, 2), Seq(2), Seq(1))
      val context = controller.controllerContext
      Seq(deleted, live, withState).foreach { tp =>
        context.updatePartitionFullReplicaAssignment(tp, assignment)
        context.partitionsBeingReassigned.add(tp)
      }
      context.putPartitionLeadershipInfo(withState, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1, 2)), 0))
      when(client.reassignPartitionsInProgress).thenReturn(true)
      when(client.getPartitionReassignment).thenReturn(Map(deleted -> Seq(2), live -> Seq(2)))
      try {
        controller.recoverInterruptedTopicDeletions(Set("deleted", "with-state"))
        assertEquals(assignment, context.partitionFullReplicaAssignment(live))
        assertEquals(assignment, context.partitionFullReplicaAssignment(withState))
        assertTrue(context.partitionsBeingReassigned.contains(live))
        assertTrue(context.partitionsBeingReassigned.contains(withState))
        if (enabled && deletionEnabled) {
          val retained = ReplicaAssignment(Seq(1, 2))
          assertEquals(retained, context.partitionFullReplicaAssignment(deleted))
          assertEquals(Set(live, withState), context.partitionsBeingReassigned.toSet)
          verify(client).setTopicAssignment("deleted", None, Map(deleted -> retained), context.epochZkVersion)
          verify(client).setOrCreatePartitionReassignment(Map(live -> Seq(2)), context.epochZkVersion)
        } else {
          assertEquals(assignment, context.partitionFullReplicaAssignment(deleted))
          verify(client, never()).setTopicAssignment(anyString(), any(), any(), anyInt())
        }
      } finally controller.shutdown()
    }
  }

  @Test
  def testInterruptedDeletionWriteFailureDoesNotPublishNewAssignment(): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, "true")
    val client = mock(classOf[KafkaZkClient])
    val controller = new KafkaController(KafkaConfig.fromProps(props), client, new MockTime(), mock(classOf[Metrics]),
      mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
      mock(classOf[ZkMetadataCache]))
    val tp = new TopicPartition("deleted", 0)
    val assignment = ReplicaAssignment(Seq(1, 2), Seq(2), Seq(1))
    controller.controllerContext.updatePartitionFullReplicaAssignment(tp, assignment)
    controller.controllerContext.partitionsBeingReassigned.add(tp)
    val failure = new ControllerMovedException("fenced")
    doAnswer(_ => throw failure).when(client).setTopicAssignment(anyString(), any(), any(), anyInt())
    try {
      assertSame(failure, assertThrows(classOf[ControllerMovedException],
        () => controller.recoverInterruptedTopicDeletions(Set("deleted"))))
      assertEquals(assignment, controller.controllerContext.partitionFullReplicaAssignment(tp))
      assertTrue(controller.controllerContext.partitionsBeingReassigned.contains(tp))
    } finally controller.shutdown()
  }

  @Test
  def testRemoveMetricsOnClose(): Unit = {
    val mockMetricsGroupCtor = mockConstruction(classOf[KafkaMetricsGroup])
    try {
      val kafkaController = new KafkaController(
        config = config,
        zkClient = mock(classOf[KafkaZkClient]),
        time = new MockTime(),
        metrics = mock(classOf[Metrics]),
        initialBrokerInfo = mock(classOf[BrokerInfo]),
        initialBrokerEpoch = 0,
        tokenManager = mock(classOf[DelegationTokenManager]),
        brokerFeatures = mock(classOf[BrokerFeatures]),
        featureCache = mock(classOf[ZkMetadataCache])
      )

      // shutdown kafkaController so that metrics are removed
      kafkaController.shutdown()

      val mockMetricsGroup = mockMetricsGroupCtor.constructed.get(0)
      val numMetricsRegistered = KafkaController.MetricNames.size
      verify(mockMetricsGroup, times(numMetricsRegistered)).newGauge(anyString(), any())
      KafkaController.MetricNames.foreach(metricName => verify(mockMetricsGroup).newGauge(ArgumentMatchers.eq(metricName), any()))
      // verify that each metric is removed
      verify(mockMetricsGroup, times(numMetricsRegistered)).removeMetric(anyString())
      KafkaController.MetricNames.foreach(verify(mockMetricsGroup).removeMetric(_))

      // assert that we have verified all invocations on
      verifyNoMoreInteractions(mockMetricsGroup)
    } finally {
      mockMetricsGroupCtor.close()
    }
  }

  @Test
  def testParallelControllerInitializationLoadsPartitionStatesFromEveryClient(): Unit = {
    val firstClient = mock(classOf[KafkaZkClient])
    val secondClient = mock(classOf[KafkaZkClient])
    val firstPartition = new TopicPartition("topic", 0)
    val secondPartition = new TopicPartition("topic", 1)
    val firstState = LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1)), 1)
    val secondState = LeaderIsrAndControllerEpoch(LeaderAndIsr(2, List(2)), 1)
    when(firstClient.getTopicPartitionStates(Seq(firstPartition))).thenReturn(Map(firstPartition -> firstState))
    when(secondClient.getTopicPartitionStates(Seq(secondPartition))).thenReturn(Map(secondPartition -> secondState))

    val controller = new KafkaController(config, firstClient, new MockTime(), mock(classOf[Metrics]),
      mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
      mock(classOf[ZkMetadataCache]), additionalZkClients = Seq(secondClient))
    try {
      assertEquals(Map(firstPartition -> firstState, secondPartition -> secondState),
        controller.loadPartitionStates(Seq(firstPartition, secondPartition)))
    } finally controller.shutdown()
  }

  @Test
  def testParallelControllerInitializationPreservesFailureCause(): Unit = {
    val firstClient = mock(classOf[KafkaZkClient])
    val secondClient = mock(classOf[KafkaZkClient])
    val firstPartition = new TopicPartition("topic", 0)
    val secondPartition = new TopicPartition("topic", 1)
    val firstState = LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1)), 1)
    val failure = new IllegalStateException("ZooKeeper read failed")
    when(firstClient.getTopicPartitionStates(Seq(firstPartition))).thenReturn(Map(firstPartition -> firstState))
    when(secondClient.getTopicPartitionStates(Seq(secondPartition))).thenThrow(failure)

    val controller = new KafkaController(config, firstClient, new MockTime(), mock(classOf[Metrics]),
      mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
      mock(classOf[ZkMetadataCache]), additionalZkClients = Seq(secondClient))
    try {
      val thrown = assertThrows(classOf[IllegalStateException],
        () => controller.loadPartitionStates(Seq(firstPartition, secondPartition)))
      assertSame(failure, thrown)
    } finally controller.shutdown()
  }

  @Test
  def testReassignmentCancellationRequiresEnoughLiveOriginalReplicas(): Unit = {
    assertTrue(KafkaController.validateReassignmentCancellation(
      Seq(1, 2, 3), Set(1, 4), minimumAliveReplicas = 2).isDefined)
    assertEquals(None, KafkaController.validateReassignmentCancellation(
      Seq(1, 2, 3), Set(1, 2, 4), minimumAliveReplicas = 2))
    assertEquals(None, KafkaController.validateReassignmentCancellation(
      Seq(1), Set(1, 4), minimumAliveReplicas = 2))
  }

}
