/*
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
package kafka.server

import kafka.api.LeaderAndIsr
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AlterPartitionRequest, RequestHeader}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.NodeToControllerChannelManager
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify}

class BridgeAlterPartitionRetryTest {
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testRetryAcrossControllerVersionsRequiresBridgeMode(bridgeEnabled: Boolean): Unit = {
    val channel = mock(classOf[NodeToControllerChannelManager])
    val time = new MockTime
    var bridgeActive = false
    val manager = new DefaultAlterPartitionManager(channel, new MockScheduler(time), time,
      1, () => 101L, () => MetadataVersion.IBP_3_0_IV1, () => bridgeActive)
    val states = List(new BrokerState().setBrokerId(1).setBrokerEpoch(101L),
      new BrokerState().setBrokerId(2).setBrokerEpoch(102L))
    manager.submit(new TopicIdPartition(Uuid.randomUuid(), 0, "bridge-retry"),
      LeaderAndIsr(1, 1, LeaderRecoveryState.RECOVERED, states, 10), 0)
    val capture = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[AlterPartitionRequest]])
    verify(channel).sendRequest(capture.capture(), any())
    val builder = capture.getValue

    // Activation must also protect requests queued before the flag changed.
    builder.build(3.toShort)
      .serializeWithHeader(new RequestHeader(ApiKeys.ALTER_PARTITION, 3.toShort, "bridge", 0))
    bridgeActive = bridgeEnabled

    // The same queued request can move between old and new controllers.
    val oldRequest = builder.build(1.toShort)
    val oldData = oldRequest.data().duplicate()
    oldRequest.serializeWithHeader(new RequestHeader(ApiKeys.ALTER_PARTITION, 1.toShort, "bridge", 0))
    if (bridgeEnabled) {
      for (version <- Seq(3.toShort, 1.toShort, 3.toShort)) {
        val request = builder.build(version)
        request.serializeWithHeader(new RequestHeader(ApiKeys.ALTER_PARTITION, version, "bridge", 0))
        val partition = request.data().topics().get(0).partitions().get(0)
        if (version >= 3) {
          assertEquals(java.util.Arrays.asList(states: _*), partition.newIsrWithEpochs())
          assertTrue(partition.newIsr().isEmpty)
        } else {
          assertEquals(java.util.Arrays.asList(Int.box(1), Int.box(2)), partition.newIsr())
          assertTrue(partition.newIsrWithEpochs().isEmpty)
        }
      }
      assertEquals(oldData, oldRequest.data(), "a retry must not mutate an earlier request")
    } else {
      assertThrows(classOf[UnsupportedVersionException], () => builder.build(3.toShort)
        .serializeWithHeader(new RequestHeader(ApiKeys.ALTER_PARTITION, 3.toShort, "bridge", 0)))
    }
  }
}
