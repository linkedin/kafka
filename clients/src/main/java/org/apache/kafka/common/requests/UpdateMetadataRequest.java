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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataEndpoint;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataTopicState;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class UpdateMetadataRequest extends AbstractControlRequest {
    private final ReentrantLock bodyBufferLock = new ReentrantLock();
    private byte[] bodyBuffer;

    public static class Builder extends AbstractControlRequest.Builder<UpdateMetadataRequest> {
        private final List<UpdateMetadataPartitionState> partitionStates;
        private final List<UpdateMetadataBroker> liveBrokers;
        private Lock buildLock = new ReentrantLock();

        // LIKAFKA-18349 - Cache the UpdateMetadataRequest Objects to reduce memory usage
        private final Map<Short, UpdateMetadataRequest> requestCache = new HashMap<>();

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch, long maxBrokerEpoch,
                       List<UpdateMetadataPartitionState> partitionStates, List<UpdateMetadataBroker> liveBrokers) {
            super(ApiKeys.UPDATE_METADATA, version, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch);
            this.partitionStates = partitionStates;
            this.liveBrokers = liveBrokers;
        }

        @Override
        public UpdateMetadataRequest build(short version) {
            // the following inner blocks are not indented in order to make hotfix cherry-picking easier
            buildLock.lock();
            try {
            UpdateMetadataRequest updateMetadataRequest = requestCache.get(version);
            if (updateMetadataRequest == null) {
            if (version < 3) {
                for (UpdateMetadataBroker broker : liveBrokers) {
                    if (version == 0) {
                        if (broker.endpoints().size() != 1)
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0 requires a single endpoint");
                        if (broker.endpoints().get(0).securityProtocol() != SecurityProtocol.PLAINTEXT.id)
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0 only handles PLAINTEXT endpoints");
                        // Don't null out `endpoints` since it's ignored by the generated code if version >= 1
                        UpdateMetadataEndpoint endpoint = broker.endpoints().get(0);
                        broker.setV0Host(endpoint.host());
                        broker.setV0Port(endpoint.port());
                    } else {
                        if (broker.endpoints().stream().anyMatch(endpoint -> !endpoint.listener().isEmpty() &&
                                !endpoint.listener().equals(listenerNameFromSecurityProtocol(endpoint)))) {
                            throw new UnsupportedVersionException("UpdateMetadataRequest v0-v3 does not support custom " +
                                "listeners, request version: " + version + ", endpoints: " + broker.endpoints());
                        }
                    }
                }
            }

            UpdateMetadataRequestData data = new UpdateMetadataRequestData()
                    .setControllerId(controllerId)
                    .setControllerEpoch(controllerEpoch)
                    .setBrokerEpoch(brokerEpoch)
                    .setMaxBrokerEpoch(maxBrokerEpoch)
                    .setLiveBrokers(liveBrokers);

            if (version >= 5) {
                Map<String, UpdateMetadataTopicState> topicStatesMap = groupByTopic(partitionStates);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            updateMetadataRequest = new UpdateMetadataRequest(data, version);
            requestCache.put(version, updateMetadataRequest);
            }
            return updateMetadataRequest;
            } finally {
                buildLock.unlock();
            }
        }

        private static Map<String, UpdateMetadataTopicState> groupByTopic(List<UpdateMetadataPartitionState> partitionStates) {
            Map<String, UpdateMetadataTopicState> topicStates = new HashMap<>();
            for (UpdateMetadataPartitionState partition : partitionStates) {
                // We don't null out the topic name in UpdateMetadataTopicState since it's ignored by the generated
                // code if version >= 5
                UpdateMetadataTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new UpdateMetadataTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            // HOTFIX: LIKAFKA-24478
            // large cluster with large metadata can create really large string
            // potentially causing OOM
            bld.append("(type: UpdateMetadataRequest=").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", brokerEpoch=").append(brokerEpoch).
                append(", maxBrokerEpoch=").append(maxBrokerEpoch).
                append(", liveBrokers=").append(Utils.join(liveBrokers, ", ")).
                append(")");

            // bld.append("(type: UpdateMetadataRequest=").
            //   append(", controllerId=").append(controllerId).
            //   append(", controllerEpoch=").append(controllerEpoch).
            //   append(", brokerEpoch=").append(brokerEpoch).
            //   append(", partitionStates=").append(partitionStates).
            //   append(", liveBrokers=").append(Utils.join(liveBrokers, ", ")).
            //   append(")");
            return bld.toString();
        }

        public List<UpdateMetadataPartitionState> partitionStates() {
            return partitionStates;
        }

        public List<UpdateMetadataBroker> liveBrokers() {
            return liveBrokers;
        }

        public long maxBrokerEpoch() {
            return maxBrokerEpoch;
        }
    }

    private final UpdateMetadataRequestData data;
    // LIKAFKA-18349 - Cache the UpdateMetadataRequest struct to reduce memory usage
    private Struct struct = null;
    private Lock structLock = new ReentrantLock();

    UpdateMetadataRequest(UpdateMetadataRequestData data, short version) {
        super(ApiKeys.UPDATE_METADATA, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        // Version 0 only supported a single host and port and the protocol was always plaintext
        // Version 1 added support for multiple endpoints, each with its own security protocol
        // Version 2 added support for rack
        // Version 3 added support for listener name, which we can infer from the security protocol for older versions
        if (version() < 3) {
            for (UpdateMetadataBroker liveBroker : data.liveBrokers()) {
                // Set endpoints so that callers can rely on it always being present
                if (version() == 0 && liveBroker.endpoints().isEmpty()) {
                    SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
                    liveBroker.setEndpoints(singletonList(new UpdateMetadataEndpoint()
                        .setHost(liveBroker.v0Host())
                        .setPort(liveBroker.v0Port())
                        .setSecurityProtocol(securityProtocol.id)
                        .setListener(ListenerName.forSecurityProtocol(securityProtocol).value())));
                } else {
                    for (UpdateMetadataEndpoint endpoint : liveBroker.endpoints()) {
                        // Set listener so that callers can rely on it always being present
                        if (endpoint.listener().isEmpty())
                            endpoint.setListener(listenerNameFromSecurityProtocol(endpoint));
                    }
                }
            }
        }

        if (version() >= 5) {
            for (UpdateMetadataTopicState topicState : data.topicStates()) {
                for (UpdateMetadataPartitionState partitionState : topicState.partitionStates()) {
                    // Set the topic name so that we can always present the ungrouped view to callers
                    partitionState.setTopicName(topicState.topicName());
                }
            }
        }
    }

    private static String listenerNameFromSecurityProtocol(UpdateMetadataEndpoint endpoint) {
        SecurityProtocol securityProtocol = SecurityProtocol.forId(endpoint.securityProtocol());
        return ListenerName.forSecurityProtocol(securityProtocol).value();
    }

    public UpdateMetadataRequest(Struct struct, short version) {
        this(new UpdateMetadataRequestData(struct, version), version);
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
        return data.brokerEpoch();
    }

    @Override
    public long maxBrokerEpoch() {
        return data.maxBrokerEpoch();
    }

    @Override
    public UpdateMetadataResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        UpdateMetadataResponseData data = new UpdateMetadataResponseData()
                .setErrorCode(Errors.forException(e).code());
        return new UpdateMetadataResponse(data);
    }

    public Iterable<UpdateMetadataPartitionState> partitionStates() {
        if (version() >= 5) {
            return () -> new FlattenedIterator<>(data.topicStates().iterator(),
                topicState -> topicState.partitionStates().iterator());
        }
        return data.ungroupedPartitionStates();
    }

    public List<UpdateMetadataBroker> liveBrokers() {
        return data.liveBrokers();
    }

    @Override
    public Send toSend(String destination, RequestHeader header) {
        // For UpdateMetadataRequest, the toSend method on the same object will be called many times, each time with a different destination
        // value and a header containing a different correlation id.
        ByteBuffer headerBuffer = serialize(header);
        bodyBufferLock.lock();
        try {
            if (bodyBuffer == null) {
                bodyBuffer = RequestUtils.serialize(toStruct()).array();
            }
        } finally {
            bodyBufferLock.unlock();
        }
        return new NetworkSend(destination, new ByteBuffer[]{headerBuffer, ByteBuffer.wrap(bodyBuffer)});
    }

    @Override
    protected Struct toStruct() {
        // the following inner blocks are not indented in order to make hotfix cherry-picking easier
        structLock.lock();
        try {
        if (struct == null) {
        struct = data.toStruct(version());
        }
        return struct;
        } finally {
            structLock.unlock();
        }
    }

    // Visible for testing
    UpdateMetadataRequestData data() {
        return data;
    }

    public static UpdateMetadataRequest parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataRequest(ApiKeys.UPDATE_METADATA.parseRequest(version, buffer), version);
    }
}
