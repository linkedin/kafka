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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.{LeaderAndIsrLiveLeader, LeaderAndIsrPartitionState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker
import org.apache.kafka.common.requests.{AbstractControlRequest, LeaderAndIsrRequest, UpdateMetadataRequest, LiCombinedControlRequest}

import scala.collection.mutable

class LeaderAndIsrPartitionStateWithBrokerEpoch(val partitionState: LeaderAndIsrPartitionState, val maxBrokerEpoch: Long)
class UpdateMetadataPartitionStateWithBrokerEpoch(val partitionState: UpdateMetadataPartitionState, val maxBrokerEpoch: Long)

class ControllerState(val controllerId: Int, val controllerEpoch: Int)

class ControllerRequestMerger extends Logging {
  val leaderAndIsrPartitionStates: mutable.Map[TopicPartition, util.LinkedList[LeaderAndIsrPartitionStateWithBrokerEpoch]] = mutable.HashMap.empty
  var leaderAndIsrLiveLeaders: util.Collection[LeaderAndIsrLiveLeader] = null

  val updateMetadataPartitionStates: mutable.Map[TopicPartition, util.LinkedList[UpdateMetadataPartitionStateWithBrokerEpoch]] = mutable.HashMap.empty
  var updateMetadataLiveBrokers: util.List[UpdateMetadataBroker] = null

  // is it safe to always use the latest controller state?
  // If there is a request with a higher controllerEpoch, we should clear all requests with a higher controller epoch
  // If there is a request with a higher maxBrokerEpoch, we shouldn't try to send a previous request with a newer max broker epoch
  //    for a given broker, what happens when different partitions have different maxBroker epochs?
  //    the broker should make decisions on the per-partition granularity to determine if the partition should be ignored or kept
  var currentControllerState : ControllerState = null

  def addRequest(request: AbstractControlRequest.Builder[_ <: AbstractControlRequest]): Unit = {

  }

  def isReplaceable(newState: LeaderAndIsrPartitionStateWithBrokerEpoch,
    currentState: LeaderAndIsrPartitionStateWithBrokerEpoch): Boolean = {
    newState.maxBrokerEpoch > currentState.maxBrokerEpoch ||
      newState.partitionState.leaderEpoch() > currentState.partitionState.leaderEpoch()
  }

  def isReplaceable(newState: UpdateMetadataPartitionStateWithBrokerEpoch,
    currentState: UpdateMetadataPartitionStateWithBrokerEpoch): Boolean = {
    // a later state can always supersede a previous state
    true
  }

  def mergeLeaderAndIsrPartitionState(newState: LeaderAndIsrPartitionStateWithBrokerEpoch,
    queuedStates: util.LinkedList[LeaderAndIsrPartitionStateWithBrokerEpoch]): Unit = {
    // keep merging requests from the tail of the queued States
    while (!queuedStates.isEmpty && isReplaceable(newState, queuedStates.getLast)) {
      queuedStates.pollLast()
    }
    val inserted = queuedStates.offerLast(newState)
    if (!inserted) {
      error(s"Unable to insert LeaderAndIsrPartitionState $newState to the merger queue")
    }
  }

  // TODO: use type parameter to avoid duplicate code
  def mergeUpdateMetadataPartitionState(newState: UpdateMetadataPartitionStateWithBrokerEpoch,
    queuedStates: util.LinkedList[UpdateMetadataPartitionStateWithBrokerEpoch]): Unit = {
    // keep merging requests from the tail of the queued States
    while (!queuedStates.isEmpty && isReplaceable(newState, queuedStates.getLast)) {
      queuedStates.pollLast()
    }
    val inserted = queuedStates.offerLast(newState)
    if (!inserted) {
      error(s"Unable to insert LeaderAndIsrPartitionState $newState to the merger queue")
    }
  }

  // builder ---- build ---> request
  private def addLeaderAndIsrRequest(request: LeaderAndIsrRequest.Builder): Unit = {
    request.partitionStates().forEach{partitionState => {
      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())
      val currentStates = leaderAndIsrPartitionStates.getOrElseUpdate(topicPartition,
        new util.LinkedList[LeaderAndIsrPartitionStateWithBrokerEpoch]())

      mergeLeaderAndIsrPartitionState(new LeaderAndIsrPartitionStateWithBrokerEpoch(partitionState, request.maxBrokerEpoch()),
        currentStates
      )
    }}
    leaderAndIsrLiveLeaders = request.liveLeaders()
  }

  private def addUpdateMetadataRequest(request: UpdateMetadataRequest.Builder): Unit = {
    request.partitionStates().forEach{partitionState => {
      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())
      val currentStates = updateMetadataPartitionStates.getOrElseUpdate(topicPartition,
        new util.LinkedList[UpdateMetadataPartitionStateWithBrokerEpoch]())
      mergeUpdateMetadataPartitionState(new UpdateMetadataPartitionStateWithBrokerEpoch(partitionState, request.maxBrokerEpoch()),
        currentStates
      )
    }}

    updateMetadataLiveBrokers = request.liveBrokers()
  }

  // TODO: support adding StopReplica requests
  def pollLatestRequest(): LiCombinedControlRequest.Builder = {

  }
}