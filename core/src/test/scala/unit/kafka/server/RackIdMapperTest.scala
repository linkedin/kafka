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

import kafka.admin.RackAwareReplicaAssignmentRackIdMapper
import org.apache.kafka.server.config.ZkConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Properties

class RackIdMapperTest {
  @Test
  def testMapperIsIdentityWhenCompatibilityIsDisabled(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.put(KafkaConfig.LiRackIdMapperClassNameForRackAwareReplicaAssignmentProp,
      classOf[TestRackIdMapper].getName)

    val config = KafkaConfig.fromProps(props)
    assertEquals("rack-a", config.rackIdMapperForReplicaAssignment.apply("rack-a"))
  }

  @Test
  def testConfiguredMapperIsUsedWhenCompatibilityIsEnabled(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.put(KafkaConfig.LiProtocolBridgeRackIdMapperEnableProp, "true")
    props.put(KafkaConfig.LiRackIdMapperClassNameForRackAwareReplicaAssignmentProp,
      classOf[TestRackIdMapper].getName)

    val config = KafkaConfig.fromProps(props)
    assertEquals("fault-domain-rack-a", config.rackIdMapperForReplicaAssignment.apply("rack-a"))
  }
}

class TestRackIdMapper extends RackAwareReplicaAssignmentRackIdMapper {
  override def apply(rackId: String): String = s"fault-domain-$rackId"
}
