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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** One unchanged old-client process survives every migration and rollback phase.
 * The orchestrator writes checkpoint requests, but ordinary traffic continues between them.
 */
public final class LiBridgeContinuousClients {
    private static final List<String> TOPICS = Arrays.asList("bridge-live", "bridge-live-tx", "bridge-live-stream-out");
    private final String bootstrap;
    private final Path directory;
    private final Map<String, String> attempted = new HashMap<>();
    private final Map<String, String> acknowledged = new HashMap<>();
    private final long intervalMs;
    private int sequence;

    private LiBridgeContinuousClients(String bootstrap, Path directory, long intervalMs) {
        this.bootstrap = bootstrap;
        this.directory = directory;
        this.intervalMs = intervalMs;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--self-test")) {
            selfTest();
            return;
        }
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <bootstrap-servers> <control-directory> <interval-ms>");
        }
        long interval = Long.parseLong(args[2]);
        if (interval < 20) {
            throw new IllegalArgumentException("The bounded functional ledger requires at least 20 ms between records");
        }
        new LiBridgeContinuousClients(args[0], Path.of(args[1]), interval).run();
    }

    private static void selfTest() {
        LiBridgeContinuousClients test = new LiBridgeContinuousClients("unused", Path.of("unused"), 100);
        test.attempted.put("bridge-live:0", "record-0");
        Map<String, Long> seen = new HashMap<>();
        test.validate(new ConsumerRecord<>("bridge-live", 0, 0L, "0", "record-0"), seen);
        expectFailure(() -> test.validate(new ConsumerRecord<>("bridge-live", 0, 1L, "0", "record-0"), seen));
        expectFailure(() -> test.validate(new ConsumerRecord<>("bridge-live", 0, 0L, "0", "corrupted"), null));
        expectFailure(() -> test.validate(new ConsumerRecord<>("bridge-live-tx", 0, 0L, "abort-0", "must-not-be-visible"), null));
        System.out.println("Committed-record corruption, duplication and aborted-record assertions passed");
    }

    private static void expectFailure(Runnable assertion) {
        try {
            assertion.run();
        } catch (AssertionError expected) {
            return;
        }
        throw new IllegalStateException("Negative ledger assertion did not fail");
    }

    private Properties config() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("request.timeout.ms", "30000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("isolation.level", "read_committed");
        props.put("fetch.max.bytes", "1048576");
        props.put("enable.idempotence", "true");
        props.put("acks", "all");
        props.put("delivery.timeout.ms", "90000");
        props.put("max.block.ms", "90000");
        return props;
    }

    private void run() throws Exception {
        Properties transactional = config();
        transactional.put("transactional.id", "bridge-live-transaction");
        Properties consumerProperties = config();
        consumerProperties.put("group.id", "bridge-live-group");
        Properties streamProperties = config();
        streamProperties.put("application.id", "bridge-live-streams");
        streamProperties.put("processing.guarantee", "exactly_once_v2");
        streamProperties.put("replication.factor", "2");
        streamProperties.put("commit.interval.ms", "100");
        streamProperties.put("cache.max.bytes.buffering", "0");
        streamProperties.put("state.dir", directory.resolve("streams-state").toString());
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("bridge-live-stream-in", Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(value -> "processed-" + value)
            .to("bridge-live-stream-out", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), streamProperties);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config());
             KafkaProducer<String, String> tx = new KafkaProducer<>(transactional);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
             Admin admin = Admin.create(config())) {
            tx.initTransactions();
            consumer.subscribe(TOPICS);
            streams.start();
            Files.writeString(directory.resolve("ready"), "ready");
            while (!Files.exists(directory.resolve("stop"))) {
                if (streams.state() == KafkaStreams.State.ERROR) {
                    throw new AssertionError("Old Streams client entered ERROR");
                }
                String key = Integer.toString(sequence++);
                send(producer, "bridge-live", key, "record-" + key, "bridge-live", "record-" + key);
                send(producer, "bridge-live-stream-in", key, "record-" + key, "bridge-live-stream-out", "processed-record-" + key);
                // This subscribed consumer and its group stay alive through coordinator changes.
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                    validate(record, null);
                }
                Path request = directory.resolve("request");
                if (Files.exists(request)) {
                    String phase = Files.readString(request).trim();
                    if (!Files.exists(directory.resolve(phase + ".done"))) {
                        checkpoint(phase, tx, consumer, admin);
                    }
                }
                // poll() often returns immediately when records are ready. It is not a rate
                // limiter: an explicit interval keeps the retained functional ledger bounded.
                Thread.sleep(intervalMs);
            }
            verifyHistory();
        } finally {
            if (!streams.close(Duration.ofSeconds(30))) {
                throw new AssertionError("Old Streams client failed to close");
            }
        }
    }

    private void send(KafkaProducer<String, String> producer, String topic, String key, String value,
                      String observedTopic, String observedValue) throws Exception {
        String identity = observedTopic + ":" + key;
        attempted.put(identity, observedValue);
        try {
            producer.send(new ProducerRecord<>(topic, Integer.parseInt(key) % 2, key, value))
                .get(100, TimeUnit.SECONDS);
            acknowledged.put(identity, observedValue);
        } catch (ExecutionException e) {
            // A timeout has an ambiguous outcome. Validate any resulting record, but never claim
            // it as an acknowledged write. Fatal/authentication/protocol errors must fail the run.
            if (!(e.getCause() instanceof RetriableException)) {
                throw e;
            }
        }
    }

    private void checkpoint(String phase, KafkaProducer<String, String> tx,
                            KafkaConsumer<String, String> consumer, Admin admin) throws Exception {
        tx.beginTransaction();
        tx.send(new ProducerRecord<>("bridge-live-tx", "abort-" + phase, "must-not-be-visible"))
            .get(60, TimeUnit.SECONDS);
        tx.abortTransaction();
        String identity = "bridge-live-tx:" + phase;
        attempted.put(identity, "committed-" + phase);
        tx.beginTransaction();
        tx.send(new ProducerRecord<>("bridge-live-tx", phase, "committed-" + phase)).get(60, TimeUnit.SECONDS);
        tx.commitTransaction();
        acknowledged.put(identity, "committed-" + phase);
        if (!admin.listTopics().names().get(60, TimeUnit.SECONDS).containsAll(TOPICS)) {
            throw new AssertionError("Old AdminClient listTopics lost topics");
        }
        long assignmentDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        do {
            consumer.poll(Duration.ofMillis(100)).forEach(record -> validate(record, null));
        } while (consumer.assignment().isEmpty() && System.nanoTime() < assignmentDeadline);
        if (consumer.assignment().isEmpty()) {
            throw new AssertionError("Old subscribed group never acquired an assignment");
        }
        consumer.commitSync(Duration.ofSeconds(30));
        for (TopicPartition partition : consumer.assignment()) {
            var committed = consumer.committed(partition);
            if (committed == null || committed.offset() != consumer.position(partition)) {
                throw new AssertionError("Group offset did not survive coordinator access for " + partition);
            }
        }
        verifyFencing(phase);
        verifyHistory();
        Map<String, Object> evidence = new HashMap<>();
        evidence.put("phase", phase);
        evidence.put("pid", ProcessHandle.current().pid());
        evidence.put("acknowledged_records", acknowledged.size());
        evidence.put("aborted_records_visible", false);
        evidence.put("passed", true);
        Path temporary = directory.resolve(phase + ".tmp");
        new ObjectMapper().writeValue(temporary.toFile(), evidence);
        Files.move(temporary, directory.resolve(phase + ".done"), StandardCopyOption.REPLACE_EXISTING);
    }

    private void verifyFencing(String phase) throws Exception {
        Properties props = config();
        props.put("transactional.id", "bridge-fencing-" + phase);
        try (KafkaProducer<String, String> first = new KafkaProducer<>(props);
             KafkaProducer<String, String> replacement = new KafkaProducer<>(props)) {
            first.initTransactions();
            first.beginTransaction();
            first.send(new ProducerRecord<>("bridge-live-tx", "abort-fenced-" + phase, "must-not-be-visible"))
                .get(60, TimeUnit.SECONDS);
            replacement.initTransactions();
            boolean fenced = false;
            try {
                first.commitTransaction();
            } catch (ProducerFencedException | InvalidProducerEpochException expected) {
                fenced = true;
            }
            if (!fenced) {
                throw new AssertionError("Old transactional client was not fenced after producer replacement");
            }
            String key = "fenced-" + phase;
            attempted.put("bridge-live-tx:" + key, key);
            replacement.beginTransaction();
            replacement.send(new ProducerRecord<>("bridge-live-tx", key, key)).get(60, TimeUnit.SECONDS);
            replacement.commitTransaction();
            acknowledged.put("bridge-live-tx:" + key, key);
        }
    }

    private void verifyHistory() {
        // Re-read persisted history at each boundary, rather than trusting data read before a
        // downgrade. This independent verifier does not restart or reconfigure the application clients.
        try (KafkaConsumer<String, String> verifier = new KafkaConsumer<>(config())) {
            List<TopicPartition> partitions = new ArrayList<>();
            TOPICS.forEach(topic -> verifier.partitionsFor(topic).forEach(info ->
                partitions.add(new TopicPartition(topic, info.partition()))));
            verifier.assign(partitions);
            verifier.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = verifier.endOffsets(partitions);
            Map<String, Long> seen = new HashMap<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < deadline) {
                verifier.poll(Duration.ofMillis(200)).forEach(record -> validate(record, seen));
                boolean caughtUp = ends.entrySet().stream().allMatch(entry -> verifier.position(entry.getKey()) >= entry.getValue());
                if (caughtUp && seen.keySet().containsAll(acknowledged.keySet())) {
                    return;
                }
                // Streams may commit output after the initial snapshot. Keep consuming until all
                // acknowledged inputs have their exact output, subject to the same bounded deadline.
            }
            throw new AssertionError("Acknowledged history missing after transition: expected=" + acknowledged.size() +
                ", found=" + seen.size());
        }
    }

    private void validate(ConsumerRecord<String, String> record, Map<String, Long> seen) {
        String identity = record.topic() + ":" + record.key();
        if (!record.value().equals(attempted.get(identity))) {
            throw new AssertionError("Unexpected, corrupted or aborted record: " + identity);
        }
        if (seen != null && seen.putIfAbsent(identity, record.offset()) != null) {
            throw new AssertionError("Duplicate committed record: " + identity);
        }
    }
}
