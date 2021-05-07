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

package unit.kafka.controller

import java.util
import kafka.controller.ControllerRequestMerger
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LiCombinedControlRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.LiCombinedControlTransformer
import org.junit.{Assert, Before, Test}

import scala.collection.JavaConverters._

class ControllerRequestMergerTest {
  private val controllerRequestMerger = new ControllerRequestMerger()

  val leaderAndIsrRequestVersion : Short = 5
  val brokerEpoch = 10
  val controllerId = 0
  val controllerEpoch = 0
  val topic = "tp0"
  val replicas = new util.ArrayList[Integer]()
  val isr = replicas
  val leaders = Set(0,1,2).map{id => new Node(id, "app-"+id+".linkedin.com", 9092)}

  val updateMetadataRequestVersion: Short = 7
  val updateMetadataLiveBrokers = new util.ArrayList[UpdateMetadataBroker]()

  val stopReplicaRequestVersion: Short = 3
  @Before
  def setUp(): Unit = {
    replicas.add(0)
    replicas.add(1)
    replicas.add(2)
  }

  @Test
  def testMergingDifferentLeaderAndIsrPartitions(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
    brokerEpoch, brokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 1)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, brokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = (partitionStates1 ++ partitionStates2).map{partittionState =>
      LiCombinedControlTransformer.transformLeaderAndIsrPartition(partittionState, brokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
    Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())
  }

  @Test
  def testMultipleRequestsOnSameLeaderAndIsrPartition(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, brokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, brokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = partitionStates1.map{partittionState =>
      LiCombinedControlTransformer.transformLeaderAndIsrPartition(partittionState, brokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    // test that we can poll two separate requests containing the same partition state
    for (_ <- 0 until 2) {
      val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
      Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
      Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
      Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())
    }
  }

  @Test
  def testSupersedingLeaderAndIsrPartitionStates(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, brokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 0, 1)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, brokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = partitionStates2.map{partitionState =>
      LiCombinedControlTransformer.transformLeaderAndIsrPartition(partitionState, brokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    // test that we can poll a request with the larger leader epoch
    val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
    Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())

    // test that trying to poll the request again will result in empty LeaderAndIsr partition states
    Assert.assertTrue(!controllerRequestMerger.hasPendingRequests())
    val liCombinedControlRequest2 = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest2.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest2.controllerEpoch())
    Assert.assertTrue("Mismatched partition states", liCombinedControlRequest2.leaderAndIsrPartitionStates().isEmpty)
  }

  def getLeaderAndIsrPartitionStates(topic: String, partitionIndex: Int, leaderEpoch: Int = 0): List[LeaderAndIsrPartitionState] = {
    //val partitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    List(new LeaderAndIsrPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partitionIndex)
      .setControllerEpoch(controllerEpoch)
      .setLeader(0)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(0)
      .setReplicas(replicas)
      .setIsNew(false))
  }

  @Test
  def testMergingDifferentUpdateMetadataPartitions(): Unit = {
    val partitionStates1 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest1 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, brokerEpoch,
      partitionStates1.asJava, updateMetadataLiveBrokers)

    val partitionStates2 = getUpdateMetadataPartitionStates(topic, 1)
    val updateMetadataRequest2 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, brokerEpoch,
      partitionStates2.asJava, updateMetadataLiveBrokers)

    val transformedPartitionStates = (partitionStates1 ++ partitionStates2).map{partitionState =>
      LiCombinedControlTransformer.transformUpdateMetadataPartition(partitionState)
    }

    controllerRequestMerger.addRequest(updateMetadataRequest1)
    controllerRequestMerger.addRequest(updateMetadataRequest2)


    val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
    Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.updateMetadataPartitionStates())
  }

  @Test
  def testSupersedingUpdateMetadataPartitionStates(): Unit = {
    val partitionStates1 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest1 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, brokerEpoch,
      partitionStates1.asJava, updateMetadataLiveBrokers)

    val partitionStates2 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest2 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, brokerEpoch,
      partitionStates2.asJava, updateMetadataLiveBrokers)

    val transformedPartitionStates = partitionStates2.map{partitionState =>
      LiCombinedControlTransformer.transformUpdateMetadataPartition(partitionState)
    }

    controllerRequestMerger.addRequest(updateMetadataRequest1)
    controllerRequestMerger.addRequest(updateMetadataRequest2)

    val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
    Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.updateMetadataPartitionStates())

    // test that trying to poll the request again will result in empty UpdateMetadata partition states
    Assert.assertTrue(!controllerRequestMerger.hasPendingRequests())
    val liCombinedControlRequest2 = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest2.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest2.controllerEpoch())
    Assert.assertTrue("Mismatched partition states", liCombinedControlRequest2.updateMetadataPartitionStates().isEmpty)
  }

  def getUpdateMetadataPartitionStates(topic: String, partitionIndex: Int): List[UpdateMetadataPartitionState] = {
    List(new UpdateMetadataPartitionState()
    .setTopicName(topic)
    .setPartitionIndex(partitionIndex)
    .setControllerEpoch(controllerEpoch)
    .setLeader(0)
    .setLeaderEpoch(0)
    .setIsr(isr)
    .setZkVersion(0)
    .setReplicas(replicas))
  }

  @Test
  def testMergingDifferentStopReplicaPartitionStates(): Unit = {
    val partitions1 = List(new TopicPartition(topic, 0))
    val stopReplicaRequest1 = new StopReplicaRequest.Builder(stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
      brokerEpoch, true, partitions1.asJava)

    val partitions2 = List(new TopicPartition(topic, 1))
    val stopReplicaRequest2 = new StopReplicaRequest.Builder(stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
      brokerEpoch, true, partitions2.asJava)

    controllerRequestMerger.addRequest(stopReplicaRequest1)
    controllerRequestMerger.addRequest(stopReplicaRequest2)

    val expectedPartitions = (partitions1 ++ partitions2).map{partition => new StopReplicaPartitionState()
      .setTopicName(partition.topic())
      .setPartitionIndex(partition.partition())
      .setDeletePartitions(true)
      .setBrokerEpoch(brokerEpoch)}

    val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
    Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
    Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
    Assert.assertEquals("Mismatched partition states", expectedPartitions.asJava, liCombinedControlRequest.stopReplicaPartitionStates())
  }

  @Test
  def testMultipleRequestsOnSameStopReplicaPartition(): Unit = {
    // When two requests having the same partition with different values on the deletePartitions field,
    // the merger should return two different LiCombinedControlRequests

    val partitions1 = List(new TopicPartition(topic, 0))
    val stopReplicaRequest1 = new StopReplicaRequest.Builder(stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
      brokerEpoch, false, partitions1.asJava)

    val partitions2 = List(new TopicPartition(topic, 0))
    val stopReplicaRequest2 = new StopReplicaRequest.Builder(stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
      brokerEpoch, true, partitions2.asJava)

    controllerRequestMerger.addRequest(stopReplicaRequest1)
    controllerRequestMerger.addRequest(stopReplicaRequest2)

    val requests = Seq(stopReplicaRequest1, stopReplicaRequest2)
    val expectedPartitions: Seq[List[StopReplicaPartitionState]] = requests.map{request => {
      var transformedPartitions = List[StopReplicaPartitionState]()

      request.partitions().forEach{partition =>
        transformedPartitions = new StopReplicaPartitionState()
        .setTopicName(partition.topic())
        .setPartitionIndex(partition.partition())
        .setDeletePartitions(request.deletePartitions())
        .setBrokerEpoch(brokerEpoch):: transformedPartitions}
      transformedPartitions
    }}

    for (i <- 0 until 2) {
      val liCombinedControlRequest = controllerRequestMerger.pollLatestRequest()
      Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
      Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
      Assert.assertEquals("Mismatched partition states", expectedPartitions(i).asJava, liCombinedControlRequest.stopReplicaPartitionStates())
    }
  }
}
