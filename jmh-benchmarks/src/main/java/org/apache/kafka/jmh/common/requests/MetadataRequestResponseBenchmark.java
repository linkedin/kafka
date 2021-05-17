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
package org.apache.kafka.jmh.common.requests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;


@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
public class MetadataRequestResponseBenchmark {
    @State(Scope.Thread)
    public static class BenchState {

        public void populateBrokerNodeList(int numNodes) {
            brokerNodeList = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                brokerNodeList.add(new Node(i, "localhost", 2194));
            }
        }

        public void populateTopicMetadataList(int maxTopics, int maxPartitionsPerTopic) {
            topicMetadataList = new ArrayList<>();
            for (int topicCount = 0; topicCount < maxTopics; topicCount++) {
                final String topic = "topic-" + topicCount;
                final List<MetadataResponsePartition> topicPartitionMetadata = new ArrayList<>();
                for (int i = 0; i < maxPartitionsPerTopic; i++) {
                    topicPartitionMetadata.add(
                        new MetadataResponsePartition()
                            .setErrorCode(Errors.NONE.code())
                            .setLeaderEpoch(100)
                            .setLeaderId(1000)
                            .setReplicaNodes(Arrays.asList(1000, 1001, 1002))
                            .setIsrNodes(Arrays.asList(1001, 1002))
                            .setOfflineReplicas(Collections.singletonList(1000))
                            .setPartitionIndex(i)
                    );
                }
                topicMetadataList.add(
                    new MetadataResponseTopic()
                        .setIsInternal(false)
                        .setName(topic)
                        .setTopicId(Uuid.randomUuid())
                        .setTopicAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED)
                        .setPartitions(topicPartitionMetadata)
                        .setErrorCode(Errors.NONE.code())
                );
            }
        }

        @Setup(Level.Iteration)
        public void setUp() {
            populateBrokerNodeList(10);
            populateTopicMetadataList(20, 8);
        }

        public List<Node> brokerNodeList;
        public List<MetadataResponseTopic> topicMetadataList;
        public String clusterId = "XYZ";
        int controllerId = 290230;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void responseConstructor(Blackhole blackhole, BenchState state) {
        blackhole.consume(MetadataResponse.prepareResponse(ApiKeys.METADATA.latestVersion(), -1, state.brokerNodeList, state.clusterId, state.controllerId,
            state.topicMetadataList, MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));
    }
}
