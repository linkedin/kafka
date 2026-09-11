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

package kafka.network

import java.util.{Collections, Properties}
import kafka.server.{BrokerMetadataStats, KafkaConfig}
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, RequestHeader}
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class MetadataOutgoingBytesTest {
  @Test
  def testOnlyMetadataResponsesContributeToMetadataTraffic(): Unit = {
    val props = new Properties
    props.put("zookeeper.connect", "localhost:2181")
    props.put(KafkaConfig.LiProtocolBridgeLegacyRequestMetricsEnableProp, "true")
    val metrics = new RequestChannel.Metrics(Seq(ApiKeys.METADATA, ApiKeys.API_VERSIONS),
      Some(KafkaConfig.fromProps(props)))
    val channel = new RequestChannel(10, "", Time.SYSTEM, metrics)
    val meter = BrokerMetadataStats.outgoingBytesRate
    val initialCount = meter.count

    def sendResponse(apiKey: ApiKeys): Unit = {
      val request = mock(classOf[RequestChannel.Request])
      val response = mock(classOf[AbstractResponse])
      val send = mock(classOf[Send])
      when(request.header).thenReturn(new RequestHeader(apiKey, 0.toShort, "test", 0))
      when(response.errorCounts()).thenReturn(Collections.emptyMap[Errors, Integer]())
      when(send.size()).thenReturn(42L)
      when(request.buildResponseSend(response)).thenReturn(send)
      when(request.callbackRequestDequeueTimeNanos).thenReturn(None)
      when(request.responseNode(response)).thenReturn(None)
      channel.sendResponse(request, response, None)
    }

    try {
      sendResponse(ApiKeys.API_VERSIONS)
      assertEquals(initialCount, meter.count)
      sendResponse(ApiKeys.METADATA)
      assertEquals(initialCount + 42L, meter.count)
    } finally channel.shutdown()
  }
}
