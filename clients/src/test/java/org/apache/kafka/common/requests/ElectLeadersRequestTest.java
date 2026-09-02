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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElectLeadersRequestTest {
    @Test
    public void testRecommendedLeaderBuilder() {
        TopicPartition partition = new TopicPartition("orders", 3);
        Map<TopicPartition, Integer> recommendations = Collections.singletonMap(partition, 2);
        ElectLeadersRequest request = new ElectLeadersRequest.Builder(99L, recommendations, 1234)
            .build((short) 2);

        ElectLeadersRequestData data = request.data();
        assertEquals(ElectionType.RECOMMENDED.value, data.electionType());
        assertEquals(99L, data.brokerEpoch());
        assertEquals(1234, data.timeoutMs());
        ElectLeadersRequestData.TopicPartitions topic = data.topicPartitions().find("orders");
        assertEquals(3, topic.partitions().get(0));
        assertEquals(3, topic.recommendedPartitionLeaders().get(0).partitionIndex());
        assertEquals(2, topic.recommendedPartitionLeaders().get(0).recommendedLeader());
    }
}
