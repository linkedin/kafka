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
import org.apache.kafka.common.message.LiCombinedControlRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Utils;


class LiCombinedControlRequest extends AbstractControlRequest {
    public static class Builder extends AbstractControlRequest.Builder<LiCombinedControlRequest> {
        // fields from the LeaderAndISRRequest
        private final List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> partitionStates;
        private final Collection<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long maxBrokerEpoch,
                       List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> partitionStates, Collection<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, -1, maxBrokerEpoch);
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }
        @Override
        public LiCombinedControlRequest build(short version) {
            LiCombinedControlRequestData data = new LiCombinedControlRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setMaxBrokerEpoch(maxBrokerEpoch);

            // setting the LeaderAndIsr request fields
            List<LiCombinedControlRequestData.LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LiCombinedControlRequestData.LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            data.setLiveLeaders(leaders);

            Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> topicStatesMap = groupByTopic(partitionStates);
            data.setLeaderAndIsrTopicStates(new ArrayList<>(topicStatesMap.values()));

            // TODO: set the UpdateMetadata fields
            // TODO: set the StopReplica fields
            return new LiCombinedControlRequest(data, version);
        }

        private static Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> groupByTopic(List<LiCombinedControlRequestData.LeaderAndIsrPartitionState> partitionStates) {
            Map<String, LiCombinedControlRequestData.LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version >= 2
            for (LiCombinedControlRequestData.LeaderAndIsrPartitionState partition : partitionStates) {
                LiCombinedControlRequestData.LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LiCombinedControlRequestData.LeaderAndIsrTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            // TODO: revise the string representation of the builder
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", maxBrokerEpoch=").append(maxBrokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
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

    }

    private void normalizeLeaderAndIsr() {
        for (LiCombinedControlRequestData.LeaderAndIsrTopicState topicState : data.leaderAndIsrTopicStates()) {
            for (LiCombinedControlRequestData.LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
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
        // TODO: return the response
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

    public Iterable<LiCombinedControlRequestData.LeaderAndIsrPartitionState> partitionStates() {
        return () -> new FlattenedIterator<>(data.leaderAndIsrTopicStates().iterator(),
            topicState -> topicState.partitionStates().iterator());
    }

    public List<LiCombinedControlRequestData.LeaderAndIsrLiveLeader> liveLeaders() {
        return Collections.unmodifiableList(data.liveLeaders());
    }

    public static LiCombinedControlRequest parse(ByteBuffer buffer, short version) {
        return new LiCombinedControlRequest(ApiKeys.LI_COMBINED_CONTROL.parseRequest(version, buffer), version);
    }
}
