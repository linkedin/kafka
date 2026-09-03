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
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.message.{ControlledShutdownRequestData, LiControlledShutdownSkipSafetyCheckRequestData}
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, ControlledShutdownRequest, ControlledShutdownResponse, LiControlledShutdownSkipSafetyCheckRequest, LiControlledShutdownSkipSafetyCheckResponse}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.config.{ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Properties
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class LiControllerOperationsTest extends QuorumTestHarness {
  private var brokers = Seq.empty[KafkaServer]

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(brokers)
    brokers = Seq.empty
    super.tearDown()
  }

  @Test
  def testPreferredControllerTakesOverAndFallbackRestoresAvailability(): Unit = {
    brokers = createBrokers(Seq((0, false), (1, true), (2, false)), allowFallback = true)

    ensureControllerIsOneOf(Seq(1))
    brokers(1).shutdown()
    ensureControllerIsOneOf(Seq(0, 2))

    brokers(1).startup()
    ensureControllerIsOneOf(Seq(1))
  }

  @Test
  def testNoControllerIsElectedWithoutPreferredBrokerOrFallback(): Unit = {
    brokers = createBrokers(Seq((0, false), (1, false), (2, false)), allowFallback = false)
    ensureControllerIsOneOf(Seq.empty, 5000L)
  }

  @Test
  def testShutdownSafetyOverrideAllowsPreviouslyRejectedControlledShutdown(): Unit = {
    brokers = (0 to 1).map { brokerId =>
      val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
      props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "true")
      props.put(KafkaConfig.ControlledShutdownSafetyCheckEnableProp, "true")
      props.put(KafkaConfig.LiProtocolBridgeShutdownSafetyOverrideEnableProp, "true")
      props.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
      createBroker(KafkaConfig.fromProps(props)).asInstanceOf[KafkaServer]
    }

    TestUtils.createTopic(zkClient, "shutdown-safety", 1, 2, brokers)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = brokers.find(_.config.brokerId == controllerId).get
    val target = brokers.find(_.config.brokerId != controllerId).get
    val brokerEpoch = target.kafkaController.brokerEpoch

    val rejected = sendRequest(controller, target,
      new ControlledShutdownRequest.Builder(
        new ControlledShutdownRequestData()
          .setBrokerId(target.config.brokerId)
          .setBrokerEpoch(brokerEpoch),
        3)).asInstanceOf[ControlledShutdownResponse]
    assertEquals(Errors.NOT_ENOUGH_REPLICAS, rejected.error())

    val overrideResponse = sendRequest(controller, target,
      new LiControlledShutdownSkipSafetyCheckRequest.Builder(
        new LiControlledShutdownSkipSafetyCheckRequestData()
          .setBrokerId(target.config.brokerId)
          .setBrokerEpoch(brokerEpoch),
        0)).asInstanceOf[LiControlledShutdownSkipSafetyCheckResponse]
    assertEquals(Errors.NONE, Errors.forCode(overrideResponse.data.errorCode))

    val accepted = sendRequest(controller, target,
      new ControlledShutdownRequest.Builder(
        new ControlledShutdownRequestData()
          .setBrokerId(target.config.brokerId)
          .setBrokerEpoch(brokerEpoch),
        3)).asInstanceOf[ControlledShutdownResponse]
    assertEquals(Errors.NONE, accepted.error())
  }

  private def createBrokers(brokerConfigs: Seq[(Int, Boolean)],
                            allowFallback: Boolean): Seq[KafkaServer] = {
    brokerConfigs.map { case (brokerId, preferredController) =>
      val props: Properties = TestUtils.createBrokerConfig(brokerId, zkConnect)
      props.put(KafkaConfig.LiProtocolBridgePreferredControllerEnableProp, "true")
      props.put(KafkaConfig.PreferredControllerProp, preferredController.toString)
      props.put(KafkaConfig.AllowPreferredControllerFallbackProp, allowFallback.toString)
      createBroker(KafkaConfig.fromProps(props)).asInstanceOf[KafkaServer]
    }
  }

  private def ensureControllerIsOneOf(expectedBrokerIds: Seq[Int], timeoutMs: Long = 15000L): Unit = {
    val (controllerId, _) = TestUtils.computeUntilTrue(zkClient.getControllerId, waitTime = timeoutMs) {
      _.exists(controllerId => expectedBrokerIds.isEmpty || expectedBrokerIds.contains(controllerId))
    }
    if (expectedBrokerIds.isEmpty)
      assertTrue(controllerId.isEmpty, "no broker should be elected controller")
    else
      assertTrue(expectedBrokerIds.contains(controllerId.getOrElse(fail("controller was not elected"))),
        s"controller should be one of $expectedBrokerIds")
  }

  private def sendRequest(controller: KafkaServer,
                          from: KafkaServer,
                          requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): AbstractResponse = {
    val config = from.config
    val time = Time.SYSTEM
    val logContext = new LogContext
    val metadataUpdater = new ManualMetadataUpdater
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext)
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      from.metrics,
      time,
      "li-controller-operations-test",
      Map.empty[String, String].asJava,
      false,
      channelBuilder,
      logContext)
    val networkClient = new NetworkClient(
      selector,
      metadataUpdater,
      config.brokerId.toString,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.requestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      false,
      new ApiVersions,
      logContext,
      MetadataRecoveryStrategy.NONE)

    try {
      val node = from.metadataCache
        .getAliveBrokerNode(controller.config.brokerId, config.interBrokerListenerName)
        .getOrElse(fail("controller was not visible in broker metadata"))
      metadataUpdater.setNodes(Seq(node).asJava)
      assertTrue(NetworkClientUtils.awaitReady(networkClient, node, time, 10000L))
      val request = networkClient.newClientRequest(
        controller.config.brokerId.toString,
        requestBuilder,
        time.milliseconds(),
        true)
      NetworkClientUtils.sendAndReceive(networkClient, request, time).responseBody
    } finally {
      networkClient.close()
      selector.close()
    }
  }
}
