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
package kafka.api

import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.message.ElectLeadersRequestData
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.requests.ElectLeadersRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.Arrays

class ElectLeadersRequestOpsTest {

  @Test
  def testRecommendedLeadersFrom30LiRequest(): Unit = {
    val topic = new ElectLeadersRequestData.TopicPartitions()
      .setTopic("topic")
      .setPartitions(Arrays.asList(1, 2))
      .setRecommendedPartitionLeaders(Arrays.asList(
        new ElectLeadersRequestData.RecommendedPartitionLeaderState()
          .setPartitionIndex(1)
          .setRecommendedLeader(4),
        new ElectLeadersRequestData.RecommendedPartitionLeaderState()
          .setPartitionIndex(2)
          .setRecommendedLeader(5)
      ))
    val topics = new ElectLeadersRequestData.TopicPartitionsCollection()
    topics.add(topic)
    val data = new ElectLeadersRequestData()
      .setElectionType(ElectionType.RECOMMENDED.value)
      .setBrokerEpoch(10L)
      .setTopicPartitions(topics)

    val version: Short = 2
    val request = ElectLeadersRequest.parse(MessageUtil.toByteBuffer(data, version), version)

    assertEquals(ElectionType.RECOMMENDED, request.electionType)
    assertEquals(Map(new TopicPartition("topic", 1) -> 4, new TopicPartition("topic", 2) -> 5),
      request.partitionRecommendedLeaders)
    assertEquals(10L, request.data.brokerEpoch)
  }
}
