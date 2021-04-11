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

package org.apache.kafka.common.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LiCombinedControlRequestData;
import org.apache.kafka.common.message.LiCombinedControlResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;


public class LiCombinedControlRequestUtils {
  /**
   * the LeaderAndIsrPartitionState in the LiCombinedControlRequest has one more field
   * than the LeaderAndIsrPartitionState in the LeaderAndIsr request, i.e. an extra maxBroker epoch field.
   * Since one LiCombinedControlRequest may contain LeaderAndIsr partition states scattered across
   * multiple different max broker epochs, we need to add the maxBrokerEpoch field to the partition level.
   * @param partitionState the original partition state
   * @param maxBrokerEpoch the max broker epoch in the original LeaderAndIsr request
   * @return the transformed partition state in the LiCombinedControlRequest
   */
  public static LiCombinedControlRequestData.LeaderAndIsrPartitionState transformLeaderAndIsrPartition(
      LeaderAndIsrRequestData.LeaderAndIsrPartitionState partitionState, long maxBrokerEpoch) {
    return new LiCombinedControlRequestData.LeaderAndIsrPartitionState()
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
        .setIsNew(partitionState.isNew());
  }

  public static LeaderAndIsrRequestData.LeaderAndIsrPartitionState restoreLeaderAndIsrPartition(LiCombinedControlRequestData.LeaderAndIsrPartitionState partitionState) {
    return new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
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
        .setIsNew(partitionState.isNew());
  }

  public static LiCombinedControlRequestData.UpdateMetadataPartitionState transformUpdateMetadataPartition(
      UpdateMetadataRequestData.UpdateMetadataPartitionState partitionState) {
    return new LiCombinedControlRequestData.UpdateMetadataPartitionState()
        .setTopicName(partitionState.topicName())
        .setPartitionIndex(partitionState.partitionIndex())
        .setControllerEpoch(partitionState.controllerEpoch())
        .setLeader(partitionState.leader())
        .setLeaderEpoch(partitionState.leaderEpoch())
        .setIsr(partitionState.isr())
        .setZkVersion(partitionState.zkVersion())
        .setReplicas(partitionState.replicas())
        .setOfflineReplicas(partitionState.offlineReplicas());
  }

  public static List<LiCombinedControlResponseData.LeaderAndIsrPartitionError> transformLeaderAndIsrPartitionErrors(
          List<LeaderAndIsrResponseData.LeaderAndIsrPartitionError> errors
  ) {
    List<LiCombinedControlResponseData.LeaderAndIsrPartitionError> transformedErrors = new ArrayList<>();
    for (LeaderAndIsrResponseData.LeaderAndIsrPartitionError error: errors) {
      transformedErrors.add(new LiCombinedControlResponseData.LeaderAndIsrPartitionError()
      .setTopicName(error.topicName())
      .setPartitionIndex(error.partitionIndex())
      .setErrorCode(error.errorCode()));
    }
    return transformedErrors;
  }

  public static UpdateMetadataRequestData.UpdateMetadataPartitionState  restoreUpdateMetadataPartition(
       LiCombinedControlRequestData.UpdateMetadataPartitionState partitionState) {
    return new UpdateMetadataRequestData.UpdateMetadataPartitionState ()
        .setTopicName(partitionState.topicName())
        .setPartitionIndex(partitionState.partitionIndex())
        .setControllerEpoch(partitionState.controllerEpoch())
        .setLeader(partitionState.leader())
        .setLeaderEpoch(partitionState.leaderEpoch())
        .setIsr(partitionState.isr())
        .setZkVersion(partitionState.zkVersion())
        .setReplicas(partitionState.replicas())
        .setOfflineReplicas(partitionState.offlineReplicas());
  }

  public static UpdateMetadataRequestData.UpdateMetadataBroker restoreUpdateMetadataBroker(LiCombinedControlRequestData.UpdateMetadataBroker broker) {
    List<UpdateMetadataRequestData.UpdateMetadataEndpoint> endpoints = new ArrayList<>();
    broker.endpoints().forEach(endpoint ->
        endpoints.add(new UpdateMetadataRequestData.UpdateMetadataEndpoint()
            .setHost(endpoint.host())
            .setPort(endpoint.port())
            .setListener(endpoint.listener())
            .setSecurityProtocol(endpoint.securityProtocol())));

    return new UpdateMetadataRequestData.UpdateMetadataBroker()
        .setId(broker.id())
        .setV0Host(broker.v0Host())
        .setV0Port(broker.v0Port())
        .setEndpoints(endpoints)
        .setRack(broker.rack());
  }
}
