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

import kafka.api.LeaderAndIsr
import kafka.server.{BrokerFeatures, DelegationTokenManager, FinalizedFeatureCache, KafkaConfig}
import kafka.utils.TestUtils
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito._

import scala.collection.mutable.ArrayBuffer

class BridgeInterruptedDeletionTest {
  private def controller(client: KafkaZkClient, cleanup: Boolean, deletion: Boolean): KafkaController = {
    val props = TestUtils.createBrokerConfig(1, "zkConnect")
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, cleanup.toString)
    props.put(KafkaConfig.DeleteTopicEnableProp, deletion.toString)
    when(client.getTopicDeletionFlag).thenReturn(deletion.toString)
    new KafkaController(KafkaConfig.fromProps(props), ArrayBuffer(client), new MockTime(), mock(classOf[Metrics]),
      mock(classOf[BrokerInfo]), 0L, mock(classOf[DelegationTokenManager]), mock(classOf[BrokerFeatures]),
      mock(classOf[FinalizedFeatureCache]))
  }

  @Test
  def testRecoveryRetainsReplicasAndUnrelatedReassignments(): Unit = {
    for (cleanup <- Seq(false, true); deletion <- Seq(false, true)) {
      val client = mock(classOf[KafkaZkClient])
      val broker = controller(client, cleanup, deletion)
      val context = broker.controllerContext
      val deleted = new TopicPartition("deleted", 0)
      val live = new TopicPartition("live", 0)
      val withState = new TopicPartition("with-state", 0)
      val assignment = ReplicaAssignment(Seq(1, 2), Seq(2), Seq(1))
      Seq(deleted, live, withState).foreach { tp =>
        context.updatePartitionFullReplicaAssignment(tp, assignment)
        context.partitionsBeingReassigned.add(tp)
      }
      context.putPartitionLeadershipInfo(withState, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1, 2)), 0))
      when(client.reassignPartitionsInProgress).thenReturn(true)
      when(client.getPartitionReassignment).thenReturn(Map(deleted -> Seq(2), live -> Seq(2)))
      try {
        broker.recoverInterruptedTopicDeletions(Set("deleted", "with-state"))
        assertEquals(assignment, context.partitionFullReplicaAssignment(live))
        assertEquals(assignment, context.partitionFullReplicaAssignment(withState))
        assertTrue(context.partitionsBeingReassigned.contains(live))
        assertTrue(context.partitionsBeingReassigned.contains(withState))
        if (cleanup && deletion) {
          val retained = ReplicaAssignment(Seq(1, 2))
          assertEquals(retained, context.partitionFullReplicaAssignment(deleted))
          assertEquals(Set(live, withState), context.partitionsBeingReassigned.toSet)
          verify(client).setTopicAssignment("deleted", None, Map(deleted -> retained), context.epochZkVersion)
          verify(client).setOrCreatePartitionReassignment(Map(live -> Seq(2)), context.epochZkVersion)
        } else {
          assertEquals(assignment, context.partitionFullReplicaAssignment(deleted))
          verify(client, never()).setTopicAssignment(anyString(), any(), any(), anyInt())
        }
      } finally broker.shutdown()
    }
  }

  @Test
  def testWriteFailureDoesNotPublishNewAssignment(): Unit = {
    val client = mock(classOf[KafkaZkClient])
    val broker = controller(client, cleanup = true, deletion = true)
    val tp = new TopicPartition("deleted", 0)
    val assignment = ReplicaAssignment(Seq(1, 2), Seq(2), Seq(1))
    broker.controllerContext.updatePartitionFullReplicaAssignment(tp, assignment)
    broker.controllerContext.partitionsBeingReassigned.add(tp)
    val failure = new ControllerMovedException("fenced")
    doAnswer(_ => throw failure).when(client).setTopicAssignment(anyString(), any(), any(), anyInt())
    try {
      assertSame(failure, assertThrows(classOf[ControllerMovedException],
        () => broker.recoverInterruptedTopicDeletions(Set("deleted"))))
      assertEquals(assignment, broker.controllerContext.partitionFullReplicaAssignment(tp))
      assertTrue(broker.controllerContext.partitionsBeingReassigned.contains(tp))
    } finally broker.shutdown()
  }
}
