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

import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ElectLeadersRequest, ElectLeadersResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap

trait LeaderTransferManager {
  def start(): Unit
  def shutdown(): Unit
  def submit(partition: TopicPartition, recommendedLeader: Int): Unit
}

object LeaderTransferManager {
  object NoOp extends LeaderTransferManager {
    override def start(): Unit = ()
    override def shutdown(): Unit = ()
    override def submit(partition: TopicPartition, recommendedLeader: Int): Unit = ()
  }

  def noOp: LeaderTransferManager = NoOp

  def apply(config: KafkaConfig,
            controllerNodeProvider: ControllerNodeProvider,
            scheduler: Scheduler,
            time: Time,
            metrics: Metrics,
            threadNamePrefix: String,
            brokerEpochSupplier: () => Long): LeaderTransferManager = {
    val channelManager = new NodeToControllerChannelManagerImpl(
      controllerNodeProvider,
      time,
      metrics,
      config,
      "transfer-leader",
      threadNamePrefix,
      Long.MaxValue)
    new DefaultLeaderTransferManager(channelManager, scheduler, brokerEpochSupplier)
  }
}

private[server] class DefaultLeaderTransferManager(
  channelManager: NodeToControllerChannelManager,
  scheduler: Scheduler,
  brokerEpochSupplier: () => Long
) extends LeaderTransferManager with Logging {
  private val pending = new ConcurrentHashMap[TopicPartition, Integer]
  private val requestInFlight = new AtomicBoolean(false)

  override def start(): Unit = channelManager.start()

  override def shutdown(): Unit = channelManager.shutdown()

  override def submit(partition: TopicPartition, recommendedLeader: Int): Unit = {
    pending.putIfAbsent(partition, recommendedLeader)
    maybeSend()
  }

  private def maybeSend(): Unit = {
    if (!pending.isEmpty && requestInFlight.compareAndSet(false, true)) {
      val recommendations = new java.util.HashMap[TopicPartition, Integer]
      pending.forEach((partition, leader) => recommendations.put(partition, leader))
      val request = new ElectLeadersRequest.Builder(
        brokerEpochSupplier(), recommendations, Int.MaxValue)
      channelManager.sendRequest(request, new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          val retry = try {
            if (response.authenticationException != null || response.versionMismatch != null) {
              true
            } else {
              handleResponse(response.responseBody.asInstanceOf[ElectLeadersResponse])
            }
          } finally {
            requestInFlight.set(false)
          }
          if (retry) scheduleRetry() else maybeSend()
        }

        override def onTimeout(): Unit = {
          requestInFlight.set(false)
          scheduleRetry()
        }
      })
    }
  }

  private def handleResponse(response: ElectLeadersResponse): Boolean = {
    val topLevelError = Errors.forCode(response.data.errorCode)
    if (topLevelError != Errors.NONE) {
      warn(s"Controller rejected recommended leader transfers with $topLevelError; retrying")
      true
    } else {
      response.data.replicaElectionResults.forEach { topicResult =>
        topicResult.partitionResult.forEach { partitionResult =>
          val partition = new TopicPartition(topicResult.topic, partitionResult.partitionId)
          val error = Errors.forCode(partitionResult.errorCode)
          if (error == Errors.NONE)
            info(s"Successfully transferred leadership for $partition")
          else
            warn(s"Controller rejected leadership transfer for $partition with $error")
          pending.remove(partition)
        }
      }
      false
    }
  }

  private def scheduleRetry(): Unit =
    scheduler.scheduleOnce("retry-transfer-leader", () => maybeSend(), 50L)
}
