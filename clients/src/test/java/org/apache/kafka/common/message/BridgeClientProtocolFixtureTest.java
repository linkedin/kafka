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
package org.apache.kafka.common.message;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fixtures generated from compiled 3.0-li-3.9-bridge classes. */
public class BridgeClientProtocolFixtureTest {
    private static final byte[] ELECT_LEADERS_V2 = bytes(
        "0202076f7264657273020000000301000a02000000030000000200000004d20100080000000000000063");
    private static final byte[] METADATA_V12 = bytes("00000001000101");

    @Test
    public void testRecommendedElectionV2Fixture() {
        ElectLeadersRequestData data = new ElectLeadersRequestData(
            new ByteBufferAccessor(ByteBuffer.wrap(ELECT_LEADERS_V2)), (short) 2);
        assertEquals(2, data.electionType());
        assertEquals(1234, data.timeoutMs());
        assertEquals(99L, data.brokerEpoch());
        ElectLeadersRequestData.TopicPartitions topic = data.topicPartitions().find("orders");
        assertEquals(3, topic.partitions().get(0));
        assertEquals(3, topic.recommendedPartitionLeaders().get(0).partitionIndex());
        assertEquals(2, topic.recommendedPartitionLeaders().get(0).recommendedLeader());
        assertFixture(ELECT_LEADERS_V2, data, (short) 2);
    }

    @Test
    public void testMetadataExcludePartitionsV12Fixture() {
        MetadataRequestData data = new MetadataRequestData(
            new ByteBufferAccessor(ByteBuffer.wrap(METADATA_V12)), (short) 12);
        assertNull(data.topics());
        assertTrue(data.excludePartitions());
        assertFixture(METADATA_V12, data, (short) 12);
    }

    private static void assertFixture(byte[] expected, Message message, short version) {
        ByteBuffer buffer = MessageUtil.toByteBuffer(message, version);
        byte[] actual = new byte[buffer.remaining()];
        buffer.get(actual);
        assertArrayEquals(expected, actual);
    }

    private static byte[] bytes(String hex) {
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }
}
