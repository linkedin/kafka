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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MoveControllerOptions;
import org.apache.kafka.clients.admin.SkipShutdownSafetyCheckOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Exercises retained operational APIs against a running mixed cluster. */
public final class LiBridgePrivateApiSmoke {
    private LiBridgePrivateApiSmoke() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected <bootstrap-server> <suffix>");
        }
        String bootstrapServer = args[0];
        String topic = "federated-smoke-" + args[1];
        String namespace = "bridge-smoke";
        Map<String, String> federatedTopic = Collections.singletonMap(topic, namespace);

        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Duration.ofSeconds(30).toMillis());

        try (Admin admin = Admin.create(config)) {
            admin.createFederatedTopicZnodes(federatedTopic).all().get(topic).get(30, TimeUnit.SECONDS);
            List<String> topics = admin.listFederatedTopicZnodes().topics().get(30, TimeUnit.SECONDS);
            String expected = "/" + namespace + "/" + topic;
            if (!topics.contains(expected)) {
                throw new AssertionError("Federated topic " + expected + " missing from " + topics);
            }
            admin.deleteFederatedTopicZnodes(federatedTopic).all().get(topic).get(30, TimeUnit.SECONDS);
            topics = admin.listFederatedTopicZnodes().topics().get(30, TimeUnit.SECONDS);
            if (topics.contains(expected)) {
                throw new AssertionError("Federated topic " + expected + " remained after deletion");
            }

            // A maximum epoch is intentionally used only by this disposable cluster. API 1000
            // rejects stale epochs and accepts this value for the currently registered broker.
            admin.skipShutdownSafetyCheck(new SkipShutdownSafetyCheckOptions()
                .brokerId(1)
                .brokerEpoch(Long.MAX_VALUE)
                .timeoutMs(30000)).all().get(30, TimeUnit.SECONDS);
            admin.moveController(new MoveControllerOptions().timeoutMs(30000))
                .all().get(30, TimeUnit.SECONDS);
        }
    }
}
