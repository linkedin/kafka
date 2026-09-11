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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Continuous old-Admin metadata mutations; never relies on a quiet controller queue. */
public final class LiBridgeMetadataChurn {
    private LiBridgeMetadataChurn() { }

    static boolean retryMutation(Throwable cause) {
        return retryTransientFailure(cause) || cause instanceof TopicExistsException ||
            cause instanceof InvalidReplicaAssignmentException;
    }

    static boolean retryDeletion(Throwable cause) {
        return retryTransientFailure(cause) || cause instanceof UnknownTopicOrPartitionException;
    }

    private static boolean retryTransientFailure(Throwable cause) {
        // These data errors inherit RetriableException, but must fail qualification.
        if (cause instanceof KafkaStorageException || cause instanceof CorruptRecordException) {
            return false;
        }
        // A deliberate controller move can fence a 3.9 ZooKeeper config write.
        // Old clients map that response to a non-retriable ControllerMovedException.
        return cause instanceof RetriableException || cause instanceof ControllerMovedException;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected <bootstrap-servers> <control-directory>");
        }
        Path directory = Path.of(args[1]);
        String topic = "bridge-continuous-mutation";
        TopicPartition partition = new TopicPartition(topic, 0);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("client.id", "bridge-metadata-churn");
        properties.put("default.api.timeout.ms", "30000");
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", args[0]);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("acks", "all");
        producerProperties.put("enable.idempotence", "false");
        producerProperties.put("delivery.timeout.ms", "30000");
        long cycles = 0;
        try (Admin admin = Admin.create(properties);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            while (!Files.exists(directory.resolve("stop"))) {
                try {
                    // Deliberately reuse the name and alternate assignment, expansion and shrink.
                    Files.writeString(directory.resolve("stage"), "create");
                    admin.createTopics(Collections.singleton(new NewTopic(topic,
                        Collections.singletonMap(0, Collections.singletonList((int) (cycles % 2))))))
                        .all().get(40, TimeUnit.SECONDS);
                    long end = admin.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest()))
                        .all().get(40, TimeUnit.SECONDS).get(partition).offset();
                    if (end != 0) {
                        throw new AssertionError("Recreated topic retained old records: log end=" + end);
                    }
                    producer.send(new ProducerRecord<>(topic, 0, "cycle", "cycle-" + cycles))
                        .get(40, TimeUnit.SECONDS);
                    Files.writeString(directory.resolve("stage"), "expand");
                    admin.alterPartitionReassignments(Collections.singletonMap(partition,
                        Optional.of(new NewPartitionReassignment(Arrays.asList(0, 1)))))
                        .all().get(40, TimeUnit.SECONDS);
                    try {
                        Files.writeString(directory.resolve("stage"), "cancel");
                        admin.alterPartitionReassignments(Collections.singletonMap(partition, Optional.empty()))
                            .all().get(40, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        if (!(e.getCause() instanceof NoReassignmentInProgressException)) {
                            throw e;
                        }
                    }
                    Files.writeString(directory.resolve("stage"), "shrink");
                    admin.alterPartitionReassignments(Collections.singletonMap(partition,
                        Optional.of(new NewPartitionReassignment(Collections.singletonList(1)))))
                        .all().get(40, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    System.err.println(java.time.Instant.now() + " mutation retry at " + Files.readString(directory.resolve("stage")) + ": " + cause);
                    if (!retryMutation(cause)) {
                        throw e;
                    }
                }
                try {
                    Files.writeString(directory.resolve("stage"), "delete");
                    admin.deleteTopics(Collections.singleton(topic)).all().get(40, TimeUnit.SECONDS);
                    Files.writeString(directory.resolve("stage"), "wait-for-deletion");
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (admin.listTopics().names().get(40, TimeUnit.SECONDS).contains(topic)) {
                        if (System.nanoTime() > deadline) {
                            throw new AssertionError("Metadata churn deletion did not complete");
                        }
                        Thread.sleep(100);
                    }
                    cycles++;
                    Path temporary = directory.resolve("progress.tmp");
                    Files.writeString(temporary, Long.toString(cycles));
                    Files.move(temporary, directory.resolve("progress"), StandardCopyOption.REPLACE_EXISTING);
                } catch (ExecutionException e) {
                    System.err.println(java.time.Instant.now() + " deletion retry: " + e.getCause());
                    if (!retryDeletion(e.getCause())) {
                        throw e;
                    }
                }
                Thread.sleep(1000);
            }
        }
    }
}
