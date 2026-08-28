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

package kafka.server

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

class LiProtocolBridgeConfigTest {
  @Test
  def testEveryCompatibilityGateDefaultsToFalseAndCanBeEnabled(): Unit = {
    val defaults = KafkaConfig.fromProps(new Properties)
    assertEquals(19, KafkaConfig.LiProtocolBridgeEnableProps.distinct.size)
    KafkaConfig.LiProtocolBridgeEnableProps.foreach { name =>
      assertFalse(defaults.getBoolean(name), s"$name must default to false")
    }

    val props = new Properties
    KafkaConfig.LiProtocolBridgeEnableProps.foreach(props.put(_, "true"))
    val enabled = KafkaConfig.fromProps(props)
    KafkaConfig.LiProtocolBridgeEnableProps.foreach { name =>
      assertTrue(enabled.getBoolean(name), s"$name was not enabled")
    }
  }
}
