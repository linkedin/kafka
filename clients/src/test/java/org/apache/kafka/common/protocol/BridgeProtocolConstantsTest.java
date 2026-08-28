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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BridgeProtocolConstantsTest {
    @Test
    public void testLinkedInProtocolConstants() {
        assertEquals(1000, ApiKeys.LI_CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK.id);
        assertEquals(1002, ApiKeys.LI_MOVE_CONTROLLER.id);
        assertEquals(1003, ApiKeys.LI_CREATE_FEDERATED_TOPIC_ZNODES.id);
        assertEquals(1004, ApiKeys.LI_DELETE_FEDERATED_TOPIC_ZNODES.id);
        assertEquals(1005, ApiKeys.LI_LIST_FEDERATED_TOPIC_ZNODES.id);
        assertEquals(2, ElectionType.RECOMMENDED.value);
        assertEquals(-104L, ListOffsetsRequest.LI_EARLIEST_LOCAL_TIMESTAMP);
        assertEquals(Errors.OFFSET_MOVED_TO_TIERED_STORAGE, Errors.forCode((short) 1107));
        assertEquals(Errors.NOT_ENOUGH_PREFERRED_CONTROLLERS, Errors.forCode((short) 2000));
    }
}
