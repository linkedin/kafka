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

package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.LiCombinedControlRequestData;
import org.apache.kafka.common.message.LiCombinedControlResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.MappedIterator;
import org.apache.kafka.common.utils.Utils;


public class LiCombinedControlRequest extends AbstractControlRequest {
    public static class Builder extends AbstractControlRequest.Builder<LiCombinedControlRequest> {
        // fields from the LeaderAndISRRequest
        private final List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> leaderAndIsrPartitionStates;
        private final Collection<Node> leaderAndIsrLiveLeaders;

        // fields from the UpdateMetadataRequest
        private final List<LiCombinedControlRequestData.UpdateMetadataPartitionState> updateMetadataPartitionStates;
        private final List<LiCombinedControlRequestData.UpdateMetadataBroker> updateMetadataLiveBrokers;

        // fields from the StopReplicaRequest
        private final boolean deletePartitions;
        private final Collection<TopicPartition> partitions;

        public Builder(short version, int controllerId, int controllerEpoch, long maxBrokerEpoch,
            List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> leaderAndIsrPartitionStates, Collection<Node> leaderAndIsrLiveLeaders,
            List<LiCombinedControlRequestData.UpdateMetadataPartitionState> updateMetadataPartitionStates, List<LiCombinedControlRequestData.UpdateMetadataBroker> updateMetadataLiveBrokers,
            boolean deletePartitions,
            Collection<TopicPartition> partitions) {
            super(ApiKeys.LI_COMBINED_CONTROL, version, controllerId, controllerEpoch, -1, maxBrokerEpoch);
            this.leaderAndIsrPartitionStates = leaderAndIsrPartitionStates;
            this.leaderAndIsrLiveLeaders = leaderAndIsrLiveLeaders;
            this.updateMetadataPartitionStates = updateMetadataPartitionStates;
            this.updateMetadataLiveBrokers = updateMetadataLiveBrokers;
            this.deletePartitions = deletePartitions;
            this.partitions = partitions;
        }

        @Override
        public LiCombinedControlRequest build(short version) {
            LiCombinedControlRequestData data = new LiCombinedControlRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setMaxBrokerEpoch(maxBrokerEpoch);

            // setting the LeaderAndIsr fields
            List<LiCombinedControlRequestData.LeaderAndIsrLiveLeader> leaders = leaderAndIsrLiveLeaders.stream().map(n -> new LiCombinedControlRequestData.LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            data.setLiveLeaders(leaders);

            Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> leaderAndIsrTopicStateMap = groupByLeaderAndIsrTopic(
                leaderAndIsrPartitionStates);
            data.setLeaderAndIsrTopicStates(new ArrayList<>(leaderAndIsrTopicStateMap.values()));

            // setting the UpdateMetadata fields
            data.setLiveBrokers(updateMetadataLiveBrokers);
            Map<String, LiCombinedControlRequestData.UpdateMetadataTopicState> updateMetadataTopicStateMap = groupByUpdateMetadataTopic(
                updateMetadataPartitionStates);
            data.setUpdateMetadataTopicStates(new ArrayList<>(updateMetadataTopicStateMap.values()));

            // setting the StopReplica fields
            data.setDeletePartitions(deletePartitions);
            Map<String, List<Integer>> topicPartitionsMap = CollectionUtils.groupPartitionsByTopic(partitions);
            List<LiCombinedControlRequestData.StopReplicaTopic> topics = topicPartitionsMap.entrySet().stream().map(entry ->
                new LiCombinedControlRequestData.StopReplicaTopic()
                    .setName(entry.getKey())
                    .setPartitionIndexes(entry.getValue())
            ).collect(Collectors.toList());
            data.setStopReplicaTopics(topics);

            return new LiCombinedControlRequest(data, version);
        }

        private static Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> groupByLeaderAndIsrTopic(List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> partitionStates) {
            Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version > 0
            for (LiCombinedControlRequestData.LeaderAndIsrPartitionState partition : partitionStates) {
                LiCombinedControlRequestData.LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LiCombinedControlRequestData.LeaderAndIsrTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        private static Map<String, LiCombinedControlRequestData.UpdateMetadataTopicState> groupByUpdateMetadataTopic(List<LiCombinedControlRequestData.UpdateMetadataPartitionState> partitionStates) {
            Map<String, LiCombinedControlRequestData.UpdateMetadataTopicState> topicStates = new HashMap<>();
            for (LiCombinedControlRequestData.UpdateMetadataPartitionState partition : partitionStates) {
                // We don't null out the topic name in UpdateMetadataTopicState since it's ignored by the generated
                // code if version > 0
                LiCombinedControlRequestData.UpdateMetadataTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LiCombinedControlRequestData.UpdateMetadataTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            // HOTFIX: LIKAFKA-24478
            // large cluster with large metadata can create really large string
            // potentially causing OOM, thus we don't print out the UpdateMetadata PartitionStates
            bld.append("(type=LiCombinedControlRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", maxBrokerEpoch=").append(maxBrokerEpoch)
                .append(", leaderAndIsrPartitionStates=").append(leaderAndIsrPartitionStates)
                .append(", leaderAndIsrLiveLeaders=(").append(Utils.join(leaderAndIsrLiveLeaders, ", ")).append(")")
                .append(", updateMetadataLiveBrokers=").append(Utils.join(updateMetadataLiveBrokers, ", "))
                .append(", stopReplicaPartitions=").append(Utils.join(partitions, ","))
                .append(")");
            return bld.toString();

        }

    }

    private final LiCombinedControlRequestData data;

    LiCombinedControlRequest(LiCombinedControlRequestData data, short version) {
        super(ApiKeys.LI_COMBINED_CONTROL, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalizeLeaderAndIsr();
        normalizeUpdateMetadata();
    }

    private void normalizeLeaderAndIsr() {
        for (LiCombinedControlRequestData.LeaderAndIsrTopicState topicState : data.leaderAndIsrTopicStates()) {
            for (LiCombinedControlRequestData.LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
                // Set the topic name so that we can always present the ungrouped view to callers
                partitionState.setTopicName(topicState.topicName());
            }
        }
    }

    private void normalizeUpdateMetadata() {
        for (LiCombinedControlRequestData.UpdateMetadataTopicState topicState : data.updateMetadataTopicStates()) {
            for (LiCombinedControlRequestData.UpdateMetadataPartitionState partitionState : topicState.partitionStates()) {
                // Set the topic name so that we can always present the ungrouped view to callers
                partitionState.setTopicName(topicState.topicName());
            }
        }
    }

    public LiCombinedControlRequest(Struct struct, short version) {
        this(new LiCombinedControlRequestData(struct, version), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public LiCombinedControlResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiCombinedControlResponseData responseData = new LiCombinedControlResponseData();
        Errors error = Errors.forException(e);

        // below we populate the error code to all the error fields and the partition error fields
        // 1. populate LeaderAndIsr error code
        responseData.setLeaderAndIsrErrorCode(error.code());
        List<LiCombinedControlResponseData.LeaderAndIsrPartitionError> leaderAndIsrPartitionErrors = new ArrayList<>();
        for (LiCombinedControlRequestData.LeaderAndIsrPartitionState partition : leaderAndIsrPartitionStates()) {
            leaderAndIsrPartitionErrors.add(new LiCombinedControlResponseData.LeaderAndIsrPartitionError()
                .setTopicName(partition.topicName())
                .setPartitionIndex(partition.partitionIndex())
                .setErrorCode(error.code()));
        }
        responseData.setLeaderAndIsrPartitionErrors(leaderAndIsrPartitionErrors);

        // 2. populate the UpdateMetadata error code
        responseData.setUpdateMetadataErrorCode(error.code());

        // 3. populate the StopReplica error code
        responseData.setStopReplicaErrorCode(error.code());
        List<LiCombinedControlResponseData.StopReplicaPartitionError> stopReplicaPartitions = new ArrayList<>();
        for (TopicPartition tp : stopReplicaPartitions()) {
            stopReplicaPartitions.add(new LiCombinedControlResponseData.StopReplicaPartitionError()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition())
                .setErrorCode(error.code()));
        }
        responseData.setStopReplicaPartitionErrors(stopReplicaPartitions);

        return new LiCombinedControlResponse(responseData);
    }

    public boolean deletePartitions() {
        return data.deletePartitions();
    }

    /**
     * Note that this method has allocation overhead per iterated element, so callers should copy the result into
     * another collection if they need to iterate more than once.
     *
     * Implementation note: we should strive to avoid allocation overhead per element, see
     * `UpdateMetadataRequest.partitionStates()` for the preferred approach. That's not possible in this case and
     * StopReplicaRequest should be relatively rare in comparison to other request types.
     */
    private Iterable<TopicPartition> stopReplicaPartitions() {
        return () -> new FlattenedIterator<>(data.stopReplicaTopics().iterator(), topic ->
            new MappedIterator<>(topic.partitionIndexes().iterator(), partition ->
                new TopicPartition(topic.name(), partition)));
    }


    @Override
    public int controllerId() {
        return data.controllerId();
    }

    @Override
    public int controllerEpoch() {
        return data.controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        return -1; // the broker epoch field is no longer used
    }

    @Override
    public long maxBrokerEpoch() {
        return data.maxBrokerEpoch();
    }

    public Iterable<LiCombinedControlRequestData.LeaderAndIsrPartitionState> leaderAndIsrPartitionStates() {
        return () -> new FlattenedIterator<>(data.leaderAndIsrTopicStates().iterator(),
            topicState -> topicState.partitionStates().iterator());
    }

    public List<LiCombinedControlRequestData.LeaderAndIsrLiveLeader> liveLeaders() {
        return Collections.unmodifiableList(data.liveLeaders());
    }

    public Iterable<LiCombinedControlRequestData.UpdateMetadataPartitionState> updateMetadataPartitionStates() {
        return () -> new FlattenedIterator<>(data.updateMetadataTopicStates().iterator(),
            topicState -> topicState.partitionStates().iterator());
    }

    public List<LiCombinedControlRequestData.UpdateMetadataBroker> liveBrokers() {
        return data.liveBrokers();
    }

    public static LiCombinedControlRequest parse(ByteBuffer buffer, short version) {
        return new LiCombinedControlRequest(ApiKeys.LI_COMBINED_CONTROL.parseRequest(version, buffer), version);
    }
}
