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

import kafka.utils.{LiDecomposedControlResponse, LiDecomposedControlResponseUtils, Logging}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.{LeaderAndIsrRequestData, LiCombinedControlRequestData, UpdateMetadataRequestData}
import org.apache.kafka.common.message.LiCombinedControlRequestData.{LeaderAndIsrPartitionState, StopReplicaPartitionState, UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractResponse, LeaderAndIsrRequest, LiCombinedControlRequest, LiCombinedControlResponse, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.LiCombinedControlRequestUtils

import scala.collection.mutable

class RequestControllerState(val controllerId: Int, val controllerEpoch: Int)

class ControllerRequestMerger extends Logging {
  val leaderAndIsrPartitionStates: mutable.Map[TopicPartition, util.LinkedList[LeaderAndIsrPartitionState]] = mutable.HashMap.empty
  var leaderAndIsrLiveLeaders: util.Collection[Node] = new util.ArrayList[Node]()

  val updateMetadataPartitionStates: mutable.Map[TopicPartition, UpdateMetadataPartitionState] = mutable.HashMap.empty
  var updateMetadataLiveBrokers: util.List[UpdateMetadataBroker] = new util.ArrayList[UpdateMetadataBroker]()

  // the values in the stopReplicaPartitionStates corresponds to the deletePartitions flag
  val stopReplicaPartitionStates: mutable.Map[TopicPartition, util.LinkedList[StopReplicaPartitionState]] = mutable.HashMap.empty

  // is it safe to always use the latest controller state?
  // If there is a request with a higher controllerEpoch, we should clear all requests with a higher controller epoch
  // If there is a request with a higher maxBrokerEpoch, we shouldn't try to send a previous request with a newer max broker epoch
  //    for a given broker, what happens when different partitions have different maxBroker epochs?
  //    the broker should make decisions on the per-partition granularity to determine if the partition should be ignored or kept
  var currentControllerState : RequestControllerState = null

  // here we store one callback for the LeaderAndIsr response and one for the StopReplica response
  // given all the requests of the same type sent to the same broker have the same callback
  var leaderAndIsrCallback: AbstractResponse => Unit = null
  var stopReplicaCallback: AbstractResponse => Unit = null

  def addRequest(request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
    callback: AbstractResponse => Unit = null): Unit = {
    currentControllerState = new RequestControllerState(request.controllerId(), request.controllerEpoch())

    request match {
      case leaderAndIsrRequest : LeaderAndIsrRequest.Builder => addLeaderAndIsrRequest(leaderAndIsrRequest, callback)
      case updateMetadataRequest : UpdateMetadataRequest.Builder => addUpdateMetadataRequest(updateMetadataRequest)
      case stopReplicaRequest: StopReplicaRequest.Builder => addStopReplicaRequest(stopReplicaRequest, callback)
    }
  }

  def isReplaceable(newState: LeaderAndIsrPartitionState, currentState: LeaderAndIsrPartitionState): Boolean = {
    newState.leaderEpoch() > currentState.leaderEpoch()
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

  private def addLeaderAndIsrRequest(request: LeaderAndIsrRequest.Builder,
    callback: AbstractResponse => Unit): Unit = {
    request.partitionStates().forEach{partitionState => {
      val combinedRequestPartitionState = LiCombinedControlRequestUtils.transformLeaderAndIsrPartition(partitionState, request.maxBrokerEpoch())

      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())
      val currentStates = leaderAndIsrPartitionStates.getOrElseUpdate(topicPartition,
        new util.LinkedList[LeaderAndIsrPartitionState]())

      mergeLeaderAndIsrPartitionState(combinedRequestPartitionState, currentStates)

      // one LeaderAndIsr request renders the previous StopReplica requests non-applicable
      clearStopReplicaPartitionState(topicPartition)
    }}
    leaderAndIsrLiveLeaders = request.liveLeaders()
    leaderAndIsrCallback = callback
  }

  private def clearLeaderAndIsrPartitionState(topicPartition: TopicPartition): Unit = {
    leaderAndIsrPartitionStates.remove(topicPartition)
  }

  private def addUpdateMetadataRequest(request: UpdateMetadataRequest.Builder): Unit = {
    request.partitionStates().forEach{partitionState => {
      val combinedRequestPartitionState = LiCombinedControlRequestUtils.transformUpdateMetadataPartition(partitionState)

      val topicPartition = new TopicPartition(partitionState.topicName(), partitionState.partitionIndex())

      updateMetadataPartitionStates.put(topicPartition, combinedRequestPartitionState)
    }}

    def getCombinedRequestBroker(broker: UpdateMetadataRequestData.UpdateMetadataBroker): UpdateMetadataBroker = {
      val originalEndpoints = broker.endpoints()
      val endpoints = new util.ArrayList[UpdateMetadataEndpoint](originalEndpoints.size())
      originalEndpoints.forEach{endpoint =>
        endpoints.add(new UpdateMetadataEndpoint()
          .setPort(endpoint.port())
          .setHost(endpoint.host())
          .setListener(endpoint.listener())
          .setSecurityProtocol(endpoint.securityProtocol())
        )
      }

      new UpdateMetadataBroker()
        .setId(broker.id())
        .setV0Host(broker.v0Host())
        .setV0Port(broker.v0Port())
        .setEndpoints(endpoints)
        .setRack(broker.rack())
    }
    updateMetadataLiveBrokers.clear()
    request.liveBrokers().forEach{liveBroker =>
      updateMetadataLiveBrokers.add(getCombinedRequestBroker(liveBroker))
    }
  }


  private def addStopReplicaRequest(request: StopReplicaRequest.Builder,
    callback: AbstractResponse => Unit): Unit = {
    request.partitions().forEach{partition => {
      val currentPartitionStates = stopReplicaPartitionStates.getOrElseUpdate(partition,
        new util.LinkedList[StopReplicaPartitionState]())
      val deletePartitions = request.deletePartitions()
      val combinedRequestPartitionState = new StopReplicaPartitionState()
        .setTopicName(partition.topic())
        .setPartitionIndex(partition.partition())
        .setDeletePartitions(deletePartitions)
        .setMaxBrokerEpoch(request.maxBrokerEpoch())

      val inserted = currentPartitionStates.offerLast(combinedRequestPartitionState)
      if (!inserted) {
        error(s"Unable to insert StopReplica partition $partition with deletePartitions flag $deletePartitions")
      }

      // one stop replica request renders all previous LeaderAndIsr requests non-applicable
      clearLeaderAndIsrPartitionState(partition)
    }}

    stopReplicaCallback = callback
  }
  private def clearStopReplicaPartitionState(topicPartition: TopicPartition): Unit = {
    stopReplicaPartitionStates.remove(topicPartition)
  }

  private def pollLatestLeaderAndIsrInfo() : util.List[LiCombinedControlRequestData.LeaderAndIsrPartitionState] = {
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

  private def pollLatestStopReplicaPartitionStates(): util.List[StopReplicaPartitionState] = {
    val latestPartitionStates = new util.ArrayList[StopReplicaPartitionState]()
    for (partition <- stopReplicaPartitionStates.keySet) {
      val partitionStates = stopReplicaPartitionStates.get(partition).get
      val latestState = partitionStates.poll()
      if (partitionStates.isEmpty) {
        stopReplicaPartitionStates.remove(partition)
      }

      latestPartitionStates.add(latestState)
    }
    latestPartitionStates
  }

  private def pollLatestUpdateMetadataInfo(): (util.List[UpdateMetadataPartitionState], util.List[UpdateMetadataBroker]) = {
    // since we don't maintain multiple versions of data for the UpdateMetadata partition states or the live brokers
    // it's guaranteed that, after each pollLatestRequest, the updateMetadata info becomes empty
    val latestPartitionStates = new util.ArrayList[UpdateMetadataPartitionState](updateMetadataPartitionStates.size)
    updateMetadataPartitionStates.foreach{
      case (_, latestState) => latestPartitionStates.add(latestState)
    }
    updateMetadataPartitionStates.clear()

    val liveBrokers = new util.ArrayList[UpdateMetadataBroker](updateMetadataLiveBrokers)
    updateMetadataLiveBrokers.clear()

    (latestPartitionStates, liveBrokers)
  }

  private def hasPendingLeaderAndIsrRequests: Boolean = !leaderAndIsrPartitionStates.isEmpty
  private def hasPendingUpdateMetadataRequests: Boolean = {
    (!updateMetadataPartitionStates.isEmpty) || (!updateMetadataLiveBrokers.isEmpty)
  }
  private def hasPendingStopReplicaRequests: Boolean = !stopReplicaPartitionStates.isEmpty

  def hasPendingRequests(): Boolean = {
    hasPendingLeaderAndIsrRequests || hasPendingUpdateMetadataRequests || hasPendingStopReplicaRequests
  }

  // TODO: support adding StopReplica requests
  def pollLatestRequest(): LiCombinedControlRequest.Builder = {
    if (currentControllerState == null) {
      throw new IllegalStateException("No request has been added to the merger")
    } else {
      val (latestUpdateMetadataPartitions, liveBrokers) = pollLatestUpdateMetadataInfo()

      new LiCombinedControlRequest.Builder(0, currentControllerState.controllerId, currentControllerState.controllerEpoch,
        pollLatestLeaderAndIsrInfo(), leaderAndIsrLiveLeaders,
        latestUpdateMetadataPartitions, liveBrokers,
        pollLatestStopReplicaPartitionStates()
      )
    }
  }

  def triggerCallback(response: AbstractResponse): Unit = {
    // trigger the callback for the LeaderAndIsr response
    val LiDecomposedControlResponse(leaderAndIsrResponse, _, stopReplicaResponse) =
      LiDecomposedControlResponseUtils.decomposeResponse(response.asInstanceOf[LiCombinedControlResponse])
    if (leaderAndIsrCallback != null) {
      leaderAndIsrCallback(leaderAndIsrResponse)
    }
    if (stopReplicaCallback != null) {
      stopReplicaCallback(stopReplicaResponse)
    }
    // no need to trigger the callback for the updateMetadataResponse since the callback is always null
  }
}
