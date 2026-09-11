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
package kafka.controller

import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import com.yammer.metrics.core.Timer
import kafka.server.KafkaConfig
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataBroker
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.mockito.Mockito._
import scala.collection.mutable

@Timeout(15)
class RequestSendThreadBridgeTest {
  private def sender(queue: LinkedBlockingDeque[QueueItem], enabled: AtomicBoolean): RequestSendThread = {
    val props = new java.util.Properties
    props.put("broker.id", "0")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("inter.broker.protocol.version", "3.0")
    props.put("li.combined.control.request.enable", "true")
    val config = spy(KafkaConfig.fromProps(props))
    doAnswer(_ => Boolean.box(enabled.get())).when(config).liProtocolBridgeModeEnable
    val manager = mock(classOf[ControllerChannelManager])
    when(manager.brokerResponseSensors).thenReturn(mutable.Map(
      ApiKeys.UPDATE_METADATA -> mock(classOf[BrokerResponseTimeStats]),
      ApiKeys.STOP_REPLICA -> mock(classOf[BrokerResponseTimeStats])))
    val result = new RequestSendThread(0, new ControllerContext, queue, mock(classOf[NetworkClient]),
      new Node(1, "localhost", 9092), config, Time.SYSTEM, mock(classOf[Timer]),
      new StateChangeLogger(0, true, None), "bridge-cutoff-test", manager)
    result.firstUpdateMetadataWithPartitionsSent = true
    result
  }

  private def item(callback: AbstractResponse => Unit): QueueItem = {
    val request = new UpdateMetadataRequest.Builder(5.toShort, 0, 1, 5L, 5L,
      Collections.emptyList(), Collections.singletonList(new UpdateMetadataBroker().setId(1)),
      Collections.emptyMap())
    QueueItem(ApiKeys.UPDATE_METADATA, request, callback, 0L)
  }

  @Test
  def testActivationWhileBlockedDoesNotMergeNewWork(): Unit = {
    val enabled = new AtomicBoolean(false)
    val blocked = new CountDownLatch(1)
    val queue = new LinkedBlockingDeque[QueueItem] {
      override def take(): QueueItem = {
        blocked.countDown()
        super.take()
      }
    }
    val thread = sender(queue, enabled)
    val executor = Executors.newSingleThreadExecutor()
    var callbacks = 0
    val callback: AbstractResponse => Unit = _ => callbacks += 1
    try {
      val result = executor.submit(new java.util.concurrent.Callable[ApiKeys] {
        override def call(): ApiKeys = {
          val (request, receivedCallback) = thread.nextRequestAndCallback()
          assertSame(callback, receivedCallback)
          receivedCallback(null)
          request.apiKey
        }
      })
      assertTrue(blocked.await(5, TimeUnit.SECONDS))
      enabled.set(true)
      queue.put(item(callback))
      assertEquals(ApiKeys.UPDATE_METADATA, result.get(5, TimeUnit.SECONDS))
      assertEquals(1, callbacks)
      assertTrue(queue.isEmpty)
    } finally {
      executor.shutdownNow()
      thread.removeMetric("maxRequestAge", Map("broker-id" -> "1"))
    }
  }

  @Test
  def testAdmittedDeletionCallbackDrainsOnceAfterActivation(): Unit = {
    val enabled = new AtomicBoolean(false)
    val queue = new LinkedBlockingDeque[QueueItem] {
      override def isEmpty: Boolean = {
        enabled.set(true) // first item is already merged when the drain loop checks the queue
        super.isEmpty
      }
    }
    val thread = sender(queue, enabled)
    var callbacks = 0
    val state = new StopReplicaTopicState().setTopicName("deleting")
      .setPartitionStates(Collections.singletonList(new StopReplicaPartitionState().setPartitionIndex(0)
        .setLeaderEpoch(2).setDeletePartition(true)))
    val builder = new StopReplicaRequest.Builder(1.toShort, 0, 1, 5L, 5L, true, Collections.singletonList(state))
    queue.put(QueueItem(ApiKeys.STOP_REPLICA, builder, _ => callbacks += 1, 0L))
    try {
      val (request, callback) = thread.nextRequestAndCallback()
      assertEquals(ApiKeys.LI_COMBINED_CONTROL, request.apiKey)
      callback(request.build().getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception()))
      assertEquals(1, callbacks)
      queue.put(item(_ => ()))
      assertEquals(ApiKeys.UPDATE_METADATA, thread.nextRequestAndCallback()._1.apiKey)
      assertEquals(1, callbacks)
    } finally {
      thread.removeMetric("maxRequestAge", Map("broker-id" -> "1"))
    }
  }

  @Test
  def testActivationDuringDrainLeavesNewWorkAndCallbacksInQueue(): Unit = {
    val enabled = new AtomicBoolean(false)
    val admitted = new CountDownLatch(1)
    val resume = new CountDownLatch(1)
    val queue = new LinkedBlockingDeque[QueueItem] {
      override def isEmpty: Boolean = {
        // Called after the first item is merged. Hold the sender at the drain boundary.
        admitted.countDown()
        assertTrue(resume.await(5, TimeUnit.SECONDS))
        super.isEmpty
      }
    }
    val thread = sender(queue, enabled)
    queue.put(item(null))
    val executor = Executors.newSingleThreadExecutor()
    var callbacks = 0
    val callback: AbstractResponse => Unit = _ => callbacks += 1
    try {
      val first = executor.submit(new java.util.concurrent.Callable[ApiKeys] {
        override def call(): ApiKeys = thread.nextRequestAndCallback()._1.apiKey
      })
      assertTrue(admitted.await(5, TimeUnit.SECONDS))
      (1 to 100).foreach(_ => queue.put(item(callback)))
      enabled.set(true)
      resume.countDown()
      assertEquals(ApiKeys.LI_COMBINED_CONTROL, first.get(5, TimeUnit.SECONDS))
      assertEquals(100, queue.size())
      (1 to 100).foreach { _ =>
        val (request, receivedCallback) = thread.nextRequestAndCallback()
        assertEquals(ApiKeys.UPDATE_METADATA, request.apiKey)
        receivedCallback(null)
      }
      assertEquals(100, callbacks)
      assertEquals(0, queue.size())
    } finally {
      resume.countDown()
      executor.shutdownNow()
      thread.removeMetric("maxRequestAge", Map("broker-id" -> "1"))
    }
  }
}
