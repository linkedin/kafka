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

package kafka.zk

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets.UTF_8

class DeleteTopicFlagZNodeTest {
  @Test
  def testEncodingAndStrictDecoding(): Unit = {
    assertEquals(Some(true), DeleteTopicFlagZNode.decode(DeleteTopicFlagZNode.encode(true)))
    assertEquals(Some(false), DeleteTopicFlagZNode.decode(" FALSE ".getBytes(UTF_8)))
    assertTrue(DeleteTopicFlagZNode.decode("enabled".getBytes(UTF_8)).isEmpty)
    assertTrue(DeleteTopicFlagZNode.decode(null).isEmpty)
  }
}
