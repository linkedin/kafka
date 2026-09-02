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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
}
