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

import java.util

import kafka.utils.Logging
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.{LeaderAndIsrRequestData, LiCombinedControlRequestData, UpdateMetadataRequestData}
import org.apache.kafka.common.message.LiCombinedControlRequestData.{LeaderAndIsrLiveLeader, LeaderAndIsrPartitionState, UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{AbstractControlRequest, LeaderAndIsrRequest, LiCombinedControlRequest, UpdateMetadataRequest}

import scala.collection.mutable

class ControllerState(val controllerId: Int, val controllerEpoch: Int, val maxBrokerEpoch: Long)

class ControllerRequestMerger extends Logging {
  val leaderAndIsrPartitionStates: mutable.Map[TopicPartition, util.LinkedList[LeaderAndIsrPartitionState]] = mutable.HashMap.empty
  var leaderAndIsrLiveLeaders: util.Collection[Node] = null

  val updateMetadataPartitionStates: mutable.Map[TopicPartition, UpdateMetadataPartitionState] = mutable.HashMap.empty
  var updateMetadataLiveBrokers: util.List[UpdateMetadataBroker] = null

  // is it safe to always use the latest controller state?
  // If there is a request with a higher controllerEpoch, we should clear all requests with a higher controller epoch
  // If there is a request with a higher maxBrokerEpoch, we shouldn't try to send a previous request with a newer max broker epoch
  //    for a given broker, what happens when different partitions have different maxBroker epochs?
  //    the broker should make decisions on the per-partition granularity to determine if the partition should be ignored or kept
  var currentControllerState : ControllerState = null

  def addRequest(request: AbstractControlRequest.Builder[_ <: AbstractControlRequest]): Unit = {

  }

  def isReplaceable(newState: LeaderAndIsrPartitionState,
    currentState: LeaderAndIsrPartitionState): Boolean = {
    newState.maxBrokerEpoch() > currentState.maxBrokerEpoch() || newState.leaderEpoch() > currentState.leaderEpoch()
  }

  def mergeLeaderAndIsrPartitionState(newState: LeaderAndIsrPartitionState,
    queuedStates: util.LinkedList[LeaderAndIsrPartitionState]): Unit = {
    // keep merging requests from the tail of the queued States
    while (!queuedStates.isEmpty && isReplaceable(newState, queuedStates.getLast)) {
      queuedStates.pollLast()
    }
    val inserted = queuedStates.offerLast(newState)
    if (!inserted) {
      error(s"Unable to insert LeaderAndIsrPartitionState $newState to the merger queue")
    }
  }

  private def addLeaderAndIsrRequest(request: LeaderAndIsrRequest.Builder): Unit = {
    // populate the max broker epoch field for each partition

    /**
     * the LeaderAndIsrPartitionState in the LiCombinedControlRequest has one more field
     * than the LeaderAndIsrPartitionState in the LeaderAndIsr request, i.e. an extra maxBroker epoch field.
     * Since one LiCombinedControlRequest may contain LeaderAndIsr partition states scattered across
     * multiple different max broker epochs, we need to add the maxBrokerEpoch field to the partition level.
     * @param partitionState
     * @return
     */
    def getCombinedRequestPartitionState(partitionState: LeaderAndIsrRequestData.LeaderAndIsrPartitionState,
      maxBrokerEpoch: Long) = {
      new LeaderAndIsrPartitionState()
        .setMaxBrokerEpoch(maxBrokerEpoch)
        .setTopicName(partitionState.topicName())
        .setPartitionIndex(partitionState.partitionIndex())
        .setControllerEpoch(partitionState.controllerEpoch())
        .setLeader(partitionState.leader())
        .setLeaderEpoch(partitionState.leaderEpoch())
        .setIsr(partitionState.isr())
        .setZkVersion(partitionState.zkVersion())
        .setReplicas(partitionState.replicas())
        .setAddingReplicas(partitionState.addingReplicas())
        .setRemovingReplicas(partitionState.removingReplicas())
        .setIsNew(partitionState.isNew)
    }

    request.partitionStates().forEach{partitionState => {
      val combinedRequestPartitionState = getCombinedRequestPartitionState(partitionState, request.maxBrokerEpoch())

      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())
      val currentStates = leaderAndIsrPartitionStates.getOrElseUpdate(topicPartition,
        new util.LinkedList[LeaderAndIsrPartitionState]())

      mergeLeaderAndIsrPartitionState(combinedRequestPartitionState, currentStates)
    }}
    leaderAndIsrLiveLeaders = request.liveLeaders()
  }

  private def addUpdateMetadataRequest(request: UpdateMetadataRequest.Builder): Unit = {

    def getCombinedRequestPartitionState(partitionState: UpdateMetadataRequestData.UpdateMetadataPartitionState) = {
      new UpdateMetadataPartitionState()
        .setTopicName(partitionState.topicName())
        .setPartitionIndex(partitionState.partitionIndex())
        .setControllerEpoch(partitionState.controllerEpoch())
        .setLeader(partitionState.leader())
        .setLeaderEpoch(partitionState.leaderEpoch())
        .setIsr(partitionState.isr())
        .setZkVersion(partitionState.zkVersion())
        .setReplicas(partitionState.replicas())
        .setOfflineReplicas(partitionState.offlineReplicas())
    }

    request.partitionStates().forEach{partitionState => {
      val combinedRequestPartitionState = getCombinedRequestPartitionState(partitionState)

      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())

      updateMetadataPartitionStates.put(topicPartition, combinedRequestPartitionState)
    }}

    updateMetadataLiveBrokers.clear()
    updateMetadataLiveBrokers.addAll(request.liveBrokers())
  }

  private def pollLatestLeaderAndIsrPartitionStates() : util.List[LiCombinedControlRequestData.LeaderAndIsrPartitionState] = {
    val latestPartitionStates = new util.ArrayList[LiCombinedControlRequestData.LeaderAndIsrPartitionState]()

    leaderAndIsrPartitionStates.keySet.foreach{
      partition  => {
        val partitionStateList = leaderAndIsrPartitionStates.get(partition).get
        val latestState = partitionStateList.poll()
        // clear the map if the queued states have been depleted for the given partition
        if (partitionStateList.isEmpty) {
          leaderAndIsrPartitionStates.remove(partition)
        }

        latestPartitionStates.add(latestState)
      }
    }

    latestPartitionStates
  }

  private def pollLatestUpdateMetadataPartitionStates(): util.List[UpdateMetadataPartitionState] = {
    val latestPartitionStates = new util.ArrayList[UpdateMetadataPartitionState](updateMetadataPartitionStates.size)
    updateMetadataPartitionStates.foreach{
      case (partition, latestState) => latestPartitionStates.add(latestState)
    }
    updateMetadataPartitionStates.clear()
    latestPartitionStates
  }

  // TODO: support adding StopReplica requests
  def pollLatestRequest(): Option[LiCombinedControlRequest.Builder] = {
    if (currentControllerState == null) {
      None
    } else {
      Some(new LiCombinedControlRequest.Builder(0, currentControllerState.controllerId, currentControllerState.controllerEpoch,
        pollLatestLeaderAndIsrPartitionStates(), leaderAndIsrLiveLeaders,
        pollLatestUpdateMetadataPartitionStates(), updateMetadataLiveBrokers,
        true, new util.ArrayList[TopicPartition]()
      ))
    }
  }
}
