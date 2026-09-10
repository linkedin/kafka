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

/** Wire fixtures generated from the compiled Kafka 3.9-li bridge classes. */
public class BridgePrivateApiFixtureTest {
    @Test
    public void testControlledShutdownOverride() {
        byte[] fixture = bytes("00000007000000000000006300");
        LiControlledShutdownSkipSafetyCheckRequestData data =
            new LiControlledShutdownSkipSafetyCheckRequestData(accessor(fixture), (short) 0);
        assertEquals(7, data.brokerId());
        assertEquals(99L, data.brokerEpoch());
        assertFixture(fixture, data);
    }

    @Test
    public void testMoveController() {
        byte[] fixture = bytes("00");
        assertFixture(fixture, new LiMoveControllerRequestData(accessor(fixture), (short) 0));
    }

    @Test
    public void testCreateFederatedTopic() {
        byte[] fixture = bytes("02076f7264657273057765737400000004d200");
        LiCreateFederatedTopicZnodesRequestData data =
            new LiCreateFederatedTopicZnodesRequestData(accessor(fixture), (short) 0);
        assertFederatedTopic(data.topics().get(0).name(), data.topics().get(0).namespace());
        assertEquals(1234, data.timeoutMs());
        assertFixture(fixture, data);
    }

    @Test
    public void testDeleteFederatedTopic() {
        byte[] fixture = bytes("02076f7264657273057765737400000004d200");
        LiDeleteFederatedTopicZnodesRequestData data =
            new LiDeleteFederatedTopicZnodesRequestData(accessor(fixture), (short) 0);
        assertFederatedTopic(data.topics().get(0).name(), data.topics().get(0).namespace());
        assertEquals(1234, data.timeoutMs());
        assertFixture(fixture, data);
    }

    @Test
    public void testListFederatedTopic() {
        byte[] fixture = bytes("02076f726465727305776573740000");
        LiListFederatedTopicZnodesRequestData data =
            new LiListFederatedTopicZnodesRequestData(accessor(fixture), (short) 0);
        assertFederatedTopic(data.topics().get(0).name(), data.topics().get(0).namespace());
        assertFixture(fixture, data);
    }

    private static void assertFederatedTopic(String name, String namespace) {
        assertEquals("orders", name);
        assertEquals("west", namespace);
    }

    private static ByteBufferAccessor accessor(byte[] fixture) {
        return new ByteBufferAccessor(ByteBuffer.wrap(fixture));
    }

    private static void assertFixture(byte[] expected, Message message) {
        ByteBuffer buffer = MessageUtil.toByteBuffer(message, (short) 0);
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
