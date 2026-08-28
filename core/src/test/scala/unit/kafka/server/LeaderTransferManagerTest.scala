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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{AbstractRequest, ElectLeadersRequest}
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.util.Scheduler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, verify}

class LeaderTransferManagerTest {
  @Test
  def testSubmitBuildsRecommendedElectionWithBrokerEpoch(): Unit = {
    val channel = mock(classOf[NodeToControllerChannelManager])
    val manager = new DefaultLeaderTransferManager(channel, mock(classOf[Scheduler]), () => 99L)
    val partition = new TopicPartition("orders", 3)

    manager.submit(partition, 2)

    val requestCaptor = ArgumentCaptor.forClass(classOf[AbstractRequest.Builder[_]])
    val callbackCaptor = ArgumentCaptor.forClass(classOf[ControllerRequestCompletionHandler])
    verify(channel).sendRequest(requestCaptor.capture(), callbackCaptor.capture())
    val request = requestCaptor.getValue.build(2).asInstanceOf[ElectLeadersRequest]
    assertEquals(99L, request.data.brokerEpoch)
    val topic = request.data.topicPartitions.find("orders")
    assertEquals(3, topic.recommendedPartitionLeaders.get(0).partitionIndex)
    assertEquals(2, topic.recommendedPartitionLeaders.get(0).recommendedLeader)
  }
}
