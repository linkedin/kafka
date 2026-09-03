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

import kafka.utils.TestUtils
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ControlledShutdownResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class KafkaActionsTest extends QuorumTestHarness {
  private var brokers = Seq.empty[KafkaServer]

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    brokers = Seq.empty
    super.tearDown()
  }

  @Test
  def testControlledShutdownReportsResultBeforeReturning(): Unit = {
    val controllerConfig = brokerConfig(0)
    val controller = createBroker(controllerConfig).asInstanceOf[KafkaServer]
    brokers :+= controller
    assertEquals(0, TestUtils.waitUntilControllerElected(zkClient))

    val actions = new RecordingKafkaActions
    val target = new KafkaServer(brokerConfig(1), Time.SYSTEM, None,
      enableForwarding = false, kafkaActions = actions)
    brokers :+= target
    target.startup()
    TestUtils.waitUntilBrokerMetadataIsPropagated(brokers)
    TestUtils.createTopic(zkClient, "kafka-actions", 1, 2, brokers)

    target.shutdown()

    val results = actions.results.asScala.toSeq
    assertEquals(1, results.size)
    assertTrue(results.head.safeToShutdown)
    assertEquals(Errors.NONE, results.head.response.error())
    assertTrue(results.head.remainingRetries >= 0)
  }

  private def brokerConfig(brokerId: Int): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true")
    KafkaConfig.fromProps(props)
  }
}

case class ControlledShutdownResult(safeToShutdown: Boolean,
                                    response: ControlledShutdownResponse,
                                    remainingRetries: Long)

class RecordingKafkaActions extends KafkaActions {
  val results = new ConcurrentLinkedQueue[ControlledShutdownResult]

  override def notifyControlledShutdownStatus(safeToShutdown: Boolean,
                                              controlledShutdownResponse: ControlledShutdownResponse,
                                              remainingRetries: Long): Unit = {
    results.add(ControlledShutdownResult(safeToShutdown, controlledShutdownResponse, remainingRetries))
  }
}
