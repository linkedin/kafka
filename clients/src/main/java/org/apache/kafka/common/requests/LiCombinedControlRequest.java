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

class LiCombinedControlRequest extends AbstractControlRequest {
    public static class Builder extends AbstractControlRequest.Builder<LiCombinedControlRequest> {
        // fields from the LeaderAndISRRequest
        private final List<LeaderAndIsrPartitionState> partitionStates;
        private final Collection<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long maxBrokerEpoch,
                       List<LeaderAndIsrPartitionState> partitionStates, Collection<Node> liveLeaders) {
            val brokerEpoch = -1 // the broker epoch field has been replaced by maxBrokerEpoch, and it is no longer used
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch);
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }
        @Override
        public LiCombinedControlRequest build(short version) {
            LiCombinedControlRequestData data = new LiCombinedControlRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setMaxBrokerEpoch(maxBrokerEpoch);

            // setting the LeaderAndIsr request fields
            List<LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            data.setLiveLeaders(leaders);

            Map<String, LeaderAndIsrTopicState> topicStatesMap = groupByTopic(partitionStates);
            data.setTopicStates(new ArrayList<>(topicStatesMap.values()));

            // TODO: set the UpdateMetadata fields
            // TODO: set the StopReplica fields
            return new LiCombinedControlRequest(data, version);
        }

        private static Map<String, LeaderAndIsrTopicState> groupByTopic(List<LeaderAndIsrPartitionState> partitionStates) {
            Map<String, LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version >= 2
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LeaderAndIsrTopicState().setTopicName(partition.topicName()));
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
        for (LeaderAndIsrTopicState topicState : data.topicStates()) {
            for (LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
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

    public Iterable<LeaderAndIsrPartitionState> partitionStates() {
        return () -> new FlattenedIterator<>(data.topicStates().iterator(),
            topicState -> topicState.partitionStates().iterator());
    }

    public List<LeaderAndIsrLiveLeader> liveLeaders() {
        return Collections.unmodifiableList(data.liveLeaders());
    }

    public static LiCombinedControlRequest parse(ByteBuffer buffer, short version) {
        return new LiCombinedControlRequest(ApiKeys.LI_COMBINED_CONTROL.parseRequest(version, buffer), version);
    }
}
