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

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.server.config.{ReplicationConfigs, ZkConfigs}
import org.junit.jupiter.api.Assertions.{assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

class LiProtocolBridgeConfigTest {
  @Test
  def testBridgeModeDefaultsToFalseAndCanBeEnabledAt30Ibp(): Unit = {
    val defaultProps = new Properties
    defaultProps.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    assertFalse(KafkaConfig.fromProps(defaultProps).liProtocolBridgeModeEnable)

    val enabledProps = new Properties
    enabledProps.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    enabledProps.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "3.0")
    enabledProps.put(KafkaConfig.LiProtocolBridgeModeEnableProp, "true")
    assertTrue(KafkaConfig.fromProps(enabledProps).liProtocolBridgeModeActive)
  }

  @Test
  def testBridgeModeRejectsMetadataThatLeaderAndIsrV2CannotRepresent(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.put(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, "3.9")
    props.put(KafkaConfig.LiProtocolBridgeModeEnableProp, "true")

    assertThrows(classOf[ConfigException], () => KafkaConfig.fromProps(props))
  }
}
