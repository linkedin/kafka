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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Read-only inventory. Uses real effective broker/topic configs; never emits client credentials. */
public final class LiBridgeLiveInventory {
    private LiBridgeLiveInventory() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <bootstrap-server> <client.properties> <output.json>");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[1]))) {
            properties.load(input);
        }
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "60000");
        properties.put("request.timeout.ms", "30000");
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("contract_version", 2);
        // Time denotes the start of collection so a slow scan cannot look artificially fresh.
        evidence.put("collected_at_utc", Instant.now().toString());
        try (Admin admin = Admin.create(properties)) {
            String clusterId = admin.describeCluster().clusterId().get(60, TimeUnit.SECONDS);
            Collection<Node> nodes = admin.describeCluster().nodes().get(60, TimeUnit.SECONDS);
            Set<String> topicNames = new TreeSet<>(admin.listTopics(new ListTopicsOptions().listInternal(true))
                .names().get(60, TimeUnit.SECONDS));
            evidence.put("cluster_id", clusterId);
            List<Map<String, Object>> brokers = new ArrayList<>();
            for (Node node : nodes) {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                Map<String, String> config = collect(admin, Collections.singletonList(resource)).get(resource.name());
                config.put("broker.id", node.idString());
                Map<String, Object> broker = new LinkedHashMap<>();
                broker.put("broker_id", node.idString());
                broker.put("source", "AdminClient.describeConfigs(includeSynonyms=true)");
                broker.put("properties", config);
                brokers.add(broker);
            }
            evidence.put("brokers", brokers);
            Map<String, Map<String, String>> topics = new TreeMap<>();
            List<String> names = new ArrayList<>(topicNames);
            for (int start = 0; start < names.size(); start += 100) {
                List<ConfigResource> batch = names.subList(start, Math.min(names.size(), start + 100)).stream()
                    .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name)).collect(Collectors.toList());
                topics.putAll(collect(admin, batch));
            }
            evidence.put("topic_names", topicNames);
            evidence.put("topics", topics);
            Set<Integer> ids = nodes.stream().map(Node::id).collect(Collectors.toSet());
            if (!ids.equals(admin.describeCluster().nodes().get(60, TimeUnit.SECONDS).stream()
                    .map(Node::id).collect(Collectors.toSet())) ||
                    !clusterId.equals(admin.describeCluster().clusterId().get(60, TimeUnit.SECONDS)) ||
                    !topicNames.equals(admin.listTopics(new ListTopicsOptions().listInternal(true))
                        .names().get(60, TimeUnit.SECONDS))) {
                throw new IllegalStateException("Inventory changed during collection; retry the read-only scan");
            }
        }
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(args[2]).toFile(), evidence);
    }

    private static Map<String, Map<String, String>> collect(Admin admin, List<ConfigResource> resources) throws Exception {
        Map<ConfigResource, Config> results = admin.describeConfigs(resources,
            new DescribeConfigsOptions().includeSynonyms(true)).all().get(60, TimeUnit.SECONDS);
        Map<String, Map<String, String>> configs = new TreeMap<>();
        for (ConfigResource resource : resources) {
            Map<String, String> values = new TreeMap<>();
            results.get(resource).entries().forEach(entry -> {
                String name = entry.name();
                if (!entry.isSensitive() && entry.value() != null && (name.startsWith("li.protocol.bridge.") ||
                        name.equals("broker.id") || name.equals("inter.broker.protocol.version") ||
                        name.equals("process.roles") || name.equals("li.async.fetcher.enable") ||
                        name.equals("li.combined.control.request.enable") || name.equals("li.drop.corrupted.files.enable") ||
                        name.equals("li.leader.election.on.corruption.wait.ms") || name.equals("li.zookeeper.pagination.enable") ||
                        name.equals("li.num.controller.init.threads") ||
                        name.equals("remote.log.storage.system.enable") || name.equals("remote.log.storage.enable") ||
                        name.equals("remote.storage.enable"))) {
                    values.put(name, entry.value());
                }
            });
            configs.put(resource.name(), values);
        }
        return configs;
    }
}
