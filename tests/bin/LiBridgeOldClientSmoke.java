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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Exercises an unchanged 3.0 client artifact against the mixed bridge cluster. */
public final class LiBridgeOldClientSmoke {
    private static final int MESSAGE_COUNT = 10;

    private LiBridgeOldClientSmoke() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected <bootstrap-server> <topic>");
        }
        String bootstrapServer = args[0];
        String topic = args[1];

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "bridge-old-client-transaction");
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "old-client-message-" + i))
                    .get(30, TimeUnit.SECONDS);
            }
            producer.commitTransaction();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "bridge-old-client-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        int received = 0;
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singleton(topic));
            while (received < MESSAGE_COUNT && System.nanoTime() < deadlineNanos) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().startsWith("old-client-message-")) {
                        received++;
                    }
                }
            }
            if (received != MESSAGE_COUNT) {
                throw new AssertionError("Expected " + MESSAGE_COUNT + " committed records, received " + received);
            }
            consumer.commitSync(Duration.ofSeconds(10));
        }
    }
}
