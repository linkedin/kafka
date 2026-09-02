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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.LiListFederatedTopicZnodesResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LiControlledShutdownSkipSafetyCheckRequest;
import org.apache.kafka.common.requests.LiControlledShutdownSkipSafetyCheckResponse;
import org.apache.kafka.common.requests.LiCreateFederatedTopicZnodesRequest;
import org.apache.kafka.common.requests.LiCreateFederatedTopicZnodesResponse;
import org.apache.kafka.common.requests.LiDeleteFederatedTopicZnodesRequest;
import org.apache.kafka.common.requests.LiDeleteFederatedTopicZnodesResponse;
import org.apache.kafka.common.requests.LiListFederatedTopicZnodesRequest;
import org.apache.kafka.common.requests.LiListFederatedTopicZnodesResponse;
import org.apache.kafka.common.requests.LiMoveControllerRequest;
import org.apache.kafka.common.requests.LiMoveControllerResponse;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LiKafkaAdminClientTest {
    private static final String TOPIC = "orders";
    private static final String NAMESPACE = "west";

    private static AdminClientUnitTestEnv clientEnv() {
        Node controller = new Node(0, "localhost", 8121);
        Cluster cluster = new Cluster(
            "bridge-test",
            Collections.singleton(controller),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            controller
        );
        AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster);
        env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
        return env;
    }

    @Test
    public void testSkipShutdownSafetyCheckBuildsRequestAndCompletesResult() throws Exception {
        try (AdminClientUnitTestEnv env = clientEnv()) {
            env.kafkaClient().prepareResponse(request -> {
                if (!(request instanceof LiControlledShutdownSkipSafetyCheckRequest)) {
                    return false;
                }
                LiControlledShutdownSkipSafetyCheckRequest bridgeRequest =
                    (LiControlledShutdownSkipSafetyCheckRequest) request;
                assertEquals(7, bridgeRequest.data().brokerId());
                assertEquals(19L, bridgeRequest.data().brokerEpoch());
                return true;
            }, LiControlledShutdownSkipSafetyCheckResponse.prepareResponse(Errors.NONE));

            env.adminClient().skipShutdownSafetyCheck(
                new SkipShutdownSafetyCheckOptions().brokerId(7).brokerEpoch(19L).timeoutMs(5_000)
            ).all().get();
        }
    }

    @Test
    public void testMoveControllerBuildsRequestAndCompletesResult() throws Exception {
        try (AdminClientUnitTestEnv env = clientEnv()) {
            env.kafkaClient().prepareResponse(
                request -> request instanceof LiMoveControllerRequest,
                LiMoveControllerResponse.prepareResponse(Errors.NONE, (short) 0)
            );

            env.adminClient().moveController(new MoveControllerOptions().timeoutMs(5_000)).all().get();
        }
    }

    @Test
    public void testCreateAndDeleteFederatedTopicBuildRequests() throws Exception {
        Map<String, String> topics = Collections.singletonMap(TOPIC, NAMESPACE);
        try (AdminClientUnitTestEnv env = clientEnv()) {
            env.kafkaClient().prepareResponse(request -> {
                if (!(request instanceof LiCreateFederatedTopicZnodesRequest)) {
                    return false;
                }
                LiCreateFederatedTopicZnodesRequest bridgeRequest = (LiCreateFederatedTopicZnodesRequest) request;
                assertEquals(TOPIC, bridgeRequest.data().topics().get(0).name());
                assertEquals(NAMESPACE, bridgeRequest.data().topics().get(0).namespace());
                assertTrue(bridgeRequest.data().timeoutMs() > 0);
                return true;
            }, LiCreateFederatedTopicZnodesResponse.prepareResponse(Errors.NONE, 0, (short) 0));

            env.adminClient().createFederatedTopicZnodes(
                topics, new CreateFederatedTopicZnodesOptions().timeoutMs(5_000)
            ).all().get(TOPIC).get();

            env.kafkaClient().prepareResponse(request -> {
                if (!(request instanceof LiDeleteFederatedTopicZnodesRequest)) {
                    return false;
                }
                LiDeleteFederatedTopicZnodesRequest bridgeRequest = (LiDeleteFederatedTopicZnodesRequest) request;
                assertEquals(TOPIC, bridgeRequest.data().topics().get(0).name());
                assertEquals(NAMESPACE, bridgeRequest.data().topics().get(0).namespace());
                assertTrue(bridgeRequest.data().timeoutMs() > 0);
                return true;
            }, LiDeleteFederatedTopicZnodesResponse.prepareResponse(Errors.NONE, 0, (short) 0));

            env.adminClient().deleteFederatedTopicZnodes(
                topics, new DeleteFederatedTopicZnodesOptions().timeoutMs(5_000)
            ).all().get(TOPIC).get();
        }
    }

    @Test
    public void testListFederatedTopicsReturnsResponseNames() throws Exception {
        try (AdminClientUnitTestEnv env = clientEnv()) {
            env.kafkaClient().prepareResponse(request -> {
                if (!(request instanceof LiListFederatedTopicZnodesRequest)) {
                    return false;
                }
                LiListFederatedTopicZnodesRequest bridgeRequest = (LiListFederatedTopicZnodesRequest) request;
                assertEquals(TOPIC, bridgeRequest.data().topics().get(0).name());
                assertEquals(NAMESPACE, bridgeRequest.data().topics().get(0).namespace());
                return true;
            }, new LiListFederatedTopicZnodesResponse(
                new LiListFederatedTopicZnodesResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setTopics(Collections.singletonList("orders/west")),
                (short) 0
            ));

            List<String> topics = env.adminClient().listFederatedTopicZnodes(
                Collections.singletonMap(TOPIC, NAMESPACE),
                new ListFederatedTopicZnodesOptions().timeoutMs(5_000)
            ).topics().get();
            assertEquals(Collections.singletonList("orders/west"), topics);
        }
    }

    @Test
    public void testFederatedTopicErrorCompletesEachTopicFuture() throws Exception {
        try (AdminClientUnitTestEnv env = clientEnv()) {
            env.kafkaClient().prepareResponse(
                request -> request instanceof LiCreateFederatedTopicZnodesRequest,
                LiCreateFederatedTopicZnodesResponse.prepareResponse(Errors.REQUEST_TIMED_OUT, 0, (short) 0)
            );

            ExecutionException exception = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> env.adminClient().createFederatedTopicZnodes(
                    Collections.singletonMap(TOPIC, NAMESPACE),
                    new CreateFederatedTopicZnodesOptions().timeoutMs(5_000)
                ).all().get(TOPIC).get()
            );
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }
}
