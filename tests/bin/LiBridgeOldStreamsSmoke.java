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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Exercises an unchanged 3.0 Kafka Streams artifact against the mixed bridge cluster. */
public final class LiBridgeOldStreamsSmoke {
    private static final int MESSAGE_COUNT = 10;

    private LiBridgeOldStreamsSmoke() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected <bootstrap-server> <input-topic> <output-topic>");
        }
        String bootstrapServer = args[0];
        String inputTopic = args[1];
        String outputTopic = args[2];

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(value -> "streams-processed-" + value)
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "bridge-old-streams-application");
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        streamsProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        try {
            streams.start();
            waitForRunning(streams);
            produceInput(bootstrapServer, inputTopic);
            consumeOutput(bootstrapServer, outputTopic);
        } finally {
            if (!streams.close(Duration.ofSeconds(30))) {
                throw new AssertionError("Kafka Streams did not close within 30 seconds");
            }
        }
    }

    private static void waitForRunning(KafkaStreams streams) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (streams.state() != KafkaStreams.State.RUNNING && System.nanoTime() < deadlineNanos) {
            Thread.sleep(100);
        }
        if (streams.state() != KafkaStreams.State.RUNNING) {
            throw new AssertionError("Kafka Streams did not reach RUNNING state: " + streams.state());
        }
    }

    private static void produceInput(String bootstrapServer, String inputTopic) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-" + i, "message-" + i))
                    .get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void consumeOutput(String bootstrapServer, String outputTopic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bridge-old-streams-verifier");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        int received = 0;
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(outputTopic));
            while (received < MESSAGE_COUNT && System.nanoTime() < deadlineNanos) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().startsWith("streams-processed-message-")) {
                        received++;
                    }
                }
            }
        }
        if (received != MESSAGE_COUNT) {
            throw new AssertionError("Expected " + MESSAGE_COUNT + " Streams records, received " + received);
        }
    }
}
