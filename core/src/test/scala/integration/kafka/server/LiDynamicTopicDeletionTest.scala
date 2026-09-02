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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}
import kafka.utils.TestUtils
import kafka.zk.DeleteTopicFlagZNode
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DeleteTopicsOptions}
import org.apache.kafka.common.errors.TopicDeletionDisabledException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.collection.Seq
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

class LiDynamicTopicDeletionTest extends QuorumTestHarness {
  private var brokers = Seq.empty[KafkaServer]

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    brokers = Seq.empty
    super.tearDown()
  }

  private def startBroker(deleteEnabled: Boolean = true, bridgeEnabled: Boolean = true): KafkaServer = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put("delete.topic.enable", deleteEnabled.toString)
    props.put(KafkaConfig.LiProtocolBridgeDynamicTopicDeletionEnableProp, bridgeEnabled.toString)
    val broker = createBroker(KafkaConfig.fromProps(props)).asInstanceOf[KafkaServer]
    brokers :+= broker
    TestUtils.waitUntilTrue(() => broker.kafkaController.isActive, "Controller was not elected")
    awaitControllerEvents(broker)
    broker
  }

  private def awaitControllerEvents(broker: KafkaServer): Unit = {
    val ready = Promise[Unit]()
    broker.kafkaController.listPartitionReassignments(None, _ => ready.success(()))
    Await.result(ready.future, 10.seconds)
  }

  private def awaitDeletionEnabled(broker: KafkaServer, enabled: Boolean): Unit =
    TestUtils.waitUntilTrue(() => broker.kafkaController.isTopicDeletionEnabled == enabled,
      s"Topic deletion did not become $enabled")

  @Test
  def testNewFlagRemainsWatchedAndControlsDeleteApi(): Unit = {
    val broker = startBroker()
    assertEquals(Some(true), zkClient.getTopicDeletionFlag)
    TestUtils.createTopic(zkClient, "deletion-toggle", 1, 1, brokers)
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      s"localhost:${broker.boundPort(broker.config.interBrokerListenerName)}")
    val admin = Admin.create(props)
    try {
      zkClient.setTopicDeletionFlag(false)
      awaitDeletionEnabled(broker, enabled = false)
      val error = assertThrows(classOf[ExecutionException], () => admin.deleteTopics(
        Collections.singletonList("deletion-toggle"), new DeleteTopicsOptions().timeoutMs(5000))
        .all().get(10, TimeUnit.SECONDS))
      assertInstanceOf(classOf[TopicDeletionDisabledException], error.getCause)
      assertTrue(zkClient.topicExists("deletion-toggle"))
      assertTrue(zkClient.getTopicDeletions.isEmpty)

      zkClient.setTopicDeletionFlag(true)
      awaitDeletionEnabled(broker, enabled = true)
      admin.deleteTopics(Collections.singletonList("deletion-toggle"))
        .all().get(10, TimeUnit.SECONDS)
    } finally admin.close()
  }

  @Test
  def testDeletionAndRecreationRearmWatch(): Unit = {
    zkClient.setTopicDeletionFlag(false)
    val broker = startBroker()
    assertFalse(broker.kafkaController.isTopicDeletionEnabled)
    zkClient.deletePath(DeleteTopicFlagZNode.path)
    awaitDeletionEnabled(broker, enabled = true)
    zkClient.setTopicDeletionFlag(false)
    awaitDeletionEnabled(broker, enabled = false)
    zkClient.setTopicDeletionFlag(true)
    awaitDeletionEnabled(broker, enabled = true)
  }

  @Test
  def testFlagCanEnableDeletionWhenStaticDefaultIsFalse(): Unit = {
    zkClient.setTopicDeletionFlag(true)
    val broker = startBroker(deleteEnabled = false)
    assertTrue(broker.kafkaController.isTopicDeletionEnabled)
    TestUtils.createTopic(zkClient, "enabled-by-zookeeper", 1, 1, brokers)
    zkClient.createDeleteTopicPath("enabled-by-zookeeper")
    TestUtils.waitUntilTrue(() => !zkClient.topicExists("enabled-by-zookeeper"),
      "The controller did not use the effective deletion flag")
  }

  @Test
  def testInvalidFlagKeepsLastValidValue(): Unit = {
    zkClient.setTopicDeletionFlag(false)
    val broker = startBroker()
    zkClient.currentZooKeeper.setData(DeleteTopicFlagZNode.path, "invalid".getBytes(UTF_8), -1)
    // A following valid write proves that the watch survives invalid input.
    zkClient.setTopicDeletionFlag(false)
    awaitControllerEvents(broker)
    assertFalse(broker.kafkaController.isTopicDeletionEnabled)
    zkClient.setTopicDeletionFlag(true)
    awaitDeletionEnabled(broker, enabled = true)
  }

  @Test
  def testDisabledBridgeUsesStaticConfig(): Unit = {
    zkClient.setTopicDeletionFlag(false)
    val broker = startBroker(bridgeEnabled = false)
    assertTrue(broker.kafkaController.isTopicDeletionEnabled)
  }
}
