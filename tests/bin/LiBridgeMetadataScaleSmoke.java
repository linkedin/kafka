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
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Creates configurable metadata volume before mixed-controller failover testing. */
public final class LiBridgeMetadataScaleSmoke {
    private LiBridgeMetadataScaleSmoke() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <bootstrap-server> <topic-count> <partitions-per-topic>");
        }
        String bootstrapServer = args[0];
        int topicCount = Integer.parseInt(args[1]);
        int partitionCount = Integer.parseInt(args[2]);
        if (topicCount < 0 || partitionCount < 1) {
            throw new IllegalArgumentException("Topic count must be non-negative and partition count must be positive");
        }
        if (topicCount == 0) {
            return;
        }

        Collection<NewTopic> topics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            topics.add(new NewTopic("bridge-scale-" + i, partitionCount, (short) 2));
        }
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) Duration.ofMinutes(5).toMillis());

        long startNanos = System.nanoTime();
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(topics).all().get(5, TimeUnit.MINUTES);
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        System.out.printf("Created %d scale topics with %d partitions each in %d ms%n",
            topicCount, partitionCount, elapsedMillis);
    }
}
