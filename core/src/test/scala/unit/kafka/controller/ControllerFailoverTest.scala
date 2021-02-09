/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metrics.Metrics
import org.apache.log4j.Logger
import org.junit.{After, Test}
import org.junit.Assert._
import org.scalatest.Assertions.fail

class ControllerFailoverTest extends KafkaServerTestHarness with Logging {
  val log = Logger.getLogger(classOf[ControllerFailoverTest])
  val numNodes = 2
  val numParts = 1
  val msgQueueSize = 1
  val topic = "topic1"
  val overridingProps = new Properties()
  val metrics = new Metrics()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  override def generateConfigs = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    this.metrics.close()
  }

  /**
   * See @link{https://issues.apache.org/jira/browse/KAFKA-2300}
   * for the background of this test case
   */
  @Test
  def testHandleIllegalStateException(): Unit = {
    val initialController = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    val initialEpoch = initialController.epoch
    // Create topic with one partition
    createTopic(topic, 1, 1)
    val topicPartition = new TopicPartition("topic1", 0)
    TestUtils.waitUntilTrue(() =>
      initialController.controllerContext.partitionsInState(OnlinePartition).contains(topicPartition),
      s"Partition $topicPartition did not transition to online state")

    // Wait until we have verified that we have resigned
    val latch = new CountDownLatch(1)
    val exceptionThrown = new AtomicReference[Throwable]()
    val illegalStateEvent = new MockEvent(ControllerState.BrokerChange) {
      override def process(): Unit = {
        try initialController.handleIllegalState(new IllegalStateException("Thrown for test purposes"))
        catch {
          case t: Throwable => exceptionThrown.set(t)
        }
        latch.await()
      }
    }
    initialController.eventManager.put(illegalStateEvent)
    // Check that we have shutdown the scheduler (via onControllerResigned)
    TestUtils.waitUntilTrue(() => !initialController.kafkaScheduler.isStarted, "Scheduler was not shutdown")
    TestUtils.waitUntilTrue(() => !initialController.isActive, "Controller did not become inactive")
    latch.countDown()
    TestUtils.waitUntilTrue(() => Option(exceptionThrown.get()).isDefined, "handleIllegalState did not throw an exception")
    assertTrue(s"handleIllegalState should throw an IllegalStateException, but $exceptionThrown was thrown",
      exceptionThrown.get.isInstanceOf[IllegalStateException])

    TestUtils.waitUntilTrue(() => {
      servers.exists { server =>
        server.kafkaController.isActive && server.kafkaController.epoch > initialEpoch
      }
    }, "Failed to find controller")

  }

  @Test
  def testTopicDeletionOnResignedController(): Unit = {
    val initialController = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    val initialEpoch = initialController.epoch

    createTopic(topic, 1, 1)

    // halt the initial controller so that it won't detect a controllership switch
    val latch = new CountDownLatch(1)
    val haltControllerEvent = new MockEvent(ControllerState.BrokerChange) {
      override def process(): Unit = {
        latch.await()
      }
    }
    initialController.eventManager.put(haltControllerEvent)

    // delete the controller znode so that a new controller can be elected
    zkClient.deleteController(initialController.controllerContext.epochZkVersion)
    // wait until a new controller has been elected
    TestUtils.waitUntilTrue(() => {
      servers.exists { server =>
        server.kafkaController.isActive && server.kafkaController.epoch > initialEpoch
      }
    }, "Failed to find controller")

    // enqueue a topic for deletion, and verify that the ControllerMovedException will be triggered
    var receivedControllerMovedException = false
    try {
      initialController.topicDeletionManager.enqueueTopicsForDeletion(Set(topic))
    } catch {
      case _: ControllerMovedException => {
        receivedControllerMovedException = true
      }
    }
    assertTrue("The ControllerMovedException is never received", receivedControllerMovedException)
    latch.countDown()

    // verify that the replicaStateMachine's controllerBrokerRequestBatch is empty
    val controllerBrokerRequestBatch = initialController.replicaStateMachine.asInstanceOf[ZkReplicaStateMachine].controllerBrokerRequestBatch
    assertTrue(controllerBrokerRequestBatch.stopReplicaRequestMap.isEmpty)
    assertTrue(controllerBrokerRequestBatch.updateMetadataRequestPartitionInfoMap.isEmpty)
    assertTrue(controllerBrokerRequestBatch.leaderAndIsrRequestMap.isEmpty)
  }
}
