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
package org.apache.kafka.storage.log.metrics;

import com.yammer.metrics.core.MetricName;

import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerTopicMetricsTest {
    @Test
    public void testCumulativeIngressMetricsRequireLegacyGate() {
        BrokerTopicMetrics disabled = new BrokerTopicMetrics(false, false);
        try {
            assertFalse(disabled.messagesInTotal().isPresent());
            assertFalse(disabled.bytesInTotal().isPresent());
        } finally {
            disabled.close();
        }

        BrokerTopicMetrics enabled = new BrokerTopicMetrics(false, true);
        try {
            enabled.markMessagesIn(3);
            enabled.markBytesIn(100);
            assertEquals(3L, enabled.messagesInTotal().get().count());
            assertEquals(100L, enabled.bytesInTotal().get().count());
        } finally {
            enabled.close();
        }
    }

    @Test
    public void testCloseMetricClosesCumulativeIngressCounters() {
        Set<MetricName> metricsBeforeTest = new HashSet<>(KafkaYammerMetrics.defaultRegistry().allMetrics().keySet());
        BrokerTopicMetrics metrics = new BrokerTopicMetrics("counter-close-test", false, true);
        try {
            metrics.messagesInTotal();
            metrics.bytesInTotal();
            Set<MetricName> createdCounters = new HashSet<>(KafkaYammerMetrics.defaultRegistry().allMetrics().keySet());
            createdCounters.removeAll(metricsBeforeTest);
            createdCounters.removeIf(name -> !name.getName().equals(BrokerTopicMetrics.MESSAGES_IN_TOTAL) &&
                !name.getName().equals(BrokerTopicMetrics.BYTES_IN_TOTAL));
            assertEquals(2, createdCounters.size());

            metrics.closeMetric(BrokerTopicMetrics.MESSAGES_IN_TOTAL);
            metrics.closeMetric(BrokerTopicMetrics.BYTES_IN_TOTAL);
            assertTrue(Collections.disjoint(
                createdCounters, KafkaYammerMetrics.defaultRegistry().allMetrics().keySet()));
        } finally {
            metrics.close();
        }
    }
}
