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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Deterministic single-partition recovery ledger; detects loss, duplicates and changed bytes. */
public final class LiBridgeRecords {
    private LiBridgeRecords() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("Expected produce|verify <bootstrap> <topic> <start> <count> <size>");
        }
        String mode = args[0];
        String topic = args[2];
        int start = Integer.parseInt(args[3]);
        int count = Integer.parseInt(args[4]);
        int size = Integer.parseInt(args[5]);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[1]);
        properties.put("request.timeout.ms", "30000");
        if (mode.equals("produce")) {
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "all");
            properties.put("enable.idempotence", "true");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                for (int index = start; index < start + count; index++) {
                    producer.send(new ProducerRecord<>(topic, 0, Integer.toString(index), value(index, size)))
                        .get(60, TimeUnit.SECONDS);
                }
            }
        } else if (mode.equals("verify")) {
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("enable.auto.commit", "false");
            TopicPartition partition = new TopicPartition(topic, 0);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                consumer.assign(Collections.singleton(partition));
                consumer.seek(partition, start);
                long end = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                if (end != start + count) {
                    throw new AssertionError("Expected log end " + (start + count) + ", found " + end);
                }
                int expected = start;
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                while (expected < end && System.nanoTime() < deadline) {
                    for (var record : consumer.poll(Duration.ofSeconds(1))) {
                        if (record.offset() != expected || !Integer.toString(expected).equals(record.key()) ||
                                !value(expected, size).equals(record.value())) {
                            throw new AssertionError("Record mismatch at " + expected + ": " + record.offset());
                        }
                        expected++;
                    }
                }
                if (expected != end) {
                    throw new AssertionError("Missing records: read through " + expected + ", expected " + end);
                }
            }
        } else {
            throw new IllegalArgumentException("Unknown mode " + mode);
        }
    }

    private static String value(int index, int size) {
        String prefix = index + ":";
        return prefix + "x".repeat(Math.max(0, size - prefix.length()));
    }
}
