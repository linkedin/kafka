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

import kafka.server.instrumentation.ProduceRequestInstrumentation.Stage
import kafka.server.instrumentation.{ProduceRequestInstrumentation, ProduceRequestInstrumentationLogger}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.config.ZkConfigs

import java.util.Properties
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

class ProduceRequestInstrumentationTest {
  @Test
  def testDisabledInstrumentationDoesNotRetainRequestPartitions(): Unit = {
    val disabled = ProduceRequestInstrumentation.Disabled
    disabled.appliedTopicPartitions = Seq(new TopicPartition("uncollected", 0))
    try assertTrue(disabled.appliedTopicPartitions.isEmpty)
    finally disabled.appliedTopicPartitions = Seq.empty
  }

  @Test
  def testActivationDoesNotLogAnUncollectedRequest(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    val disabled = KafkaConfig.fromProps(props)
    val logger = new ProduceRequestInstrumentationLogger(
      disabled, new MockTime, new scala.util.Random(1), mock(classOf[ReplicaManager]))
    props.put(KafkaConfig.LiProtocolBridgeProduceRequestInstrumentationEnableProp, "true")
    props.put(KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp, "0")
    props.put(KafkaConfig.LiLongTailProduceRequestLogRatioProp, "1.0")
    logger.reconfigure(disabled, KafkaConfig.fromProps(props))
    // No request fields may be read when this request has no collected instrumentation.
    logger.maybeLog(null, ProduceRequestInstrumentation.Disabled)
  }

  @Test
  def testStageBreakdownUsesMarkOrderAndElapsedTime(): Unit = {
    val time = new MockTime
    val instrumentation = new ProduceRequestInstrumentation(time)
    val partitions = Seq(new TopicPartition("collected", 0))
    instrumentation.appliedTopicPartitions = partitions
    assertEquals(partitions, instrumentation.appliedTopicPartitions)
    time.sleep(100)
    instrumentation.markStage(Stage.Authorization)
    time.sleep(200)
    instrumentation.markStage(Stage.AppendToLocalLog)
    time.sleep(300)

    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    val logger = new ProduceRequestInstrumentationLogger(
      KafkaConfig.fromProps(props), time, new scala.util.Random(1), mock(classOf[ReplicaManager]))

    assertEquals(
      "{\"Init\":100, \"Authorization\":200, \"AppendToLocalLog\":300}",
      logger.toTimeTakenInEachStageMessage(instrumentation))
  }
}
