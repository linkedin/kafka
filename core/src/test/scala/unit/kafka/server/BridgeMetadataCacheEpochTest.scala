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
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataPartitionState, UpdateMetadataTopicState}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class BridgeMetadataCacheEpochTest {
  private def cache = MetadataCache.zkMetadataCache(1, MetadataVersion.IBP_3_0_IV1)

  private def request(epoch: Int, topics: String*): UpdateMetadataRequest = {
    val states = topics.map { topic =>
      new UpdateMetadataTopicState().setTopicName(topic).setPartitionStates(
        List(new UpdateMetadataPartitionState().setTopicName(topic).setPartitionIndex(0)
          .setLeader(1).setLeaderEpoch(1).setControllerEpoch(epoch)
          .setReplicas(List(Int.box(1)).asJava).setIsr(List(Int.box(1)).asJava)).asJava)
    }
    val data = new UpdateMetadataRequestData().setControllerId(1)
      .setControllerEpoch(epoch).setBrokerEpoch(1L).setTopicStates(states.asJava)
    UpdateMetadataRequest.parse(MessageUtil.toByteBuffer(data, 5.toShort), 5.toShort)
  }

  @Test
  def testLostDeletionIsReconciledOnlyWhenEnabled(): Unit = {
    Seq(false, true).foreach { enabled =>
      val metadata = cache
      metadata.updateMetadata(0, request(1, "deleted", "kept"), enabled)
      // The old controller deleted the znode but died before delivering its tombstone.
      val deleted = metadata.updateMetadata(1, request(2, "kept"), enabled)
      assertEquals(!enabled, metadata.contains("deleted"))
      assertTrue(metadata.contains("kept"))
      assertEquals(if (enabled) Seq(new TopicPartition("deleted", 0)) else Seq.empty, deleted)
    }
  }

  @Test
  def testSameEpochUpdatesStayIncrementalIncludingAfterFlagActivation(): Unit = {
    val metadata = cache
    metadata.updateMetadata(0, request(1, "first", "second"))
    metadata.updateMetadata(1, request(1, "third"), reconcileOnControllerChange = true)
    assertEquals(Set("first", "second", "third"), metadata.getAllTopics())
    metadata.updateMetadata(2, request(2, "first", "third"), reconcileOnControllerChange = true)
    assertEquals(Set("first", "third"), metadata.getAllTopics())
    metadata.updateMetadata(3, request(2, "fourth"), reconcileOnControllerChange = true)
    assertEquals(Set("first", "third", "fourth"), metadata.getAllTopics())
  }

  @Test
  def testEmptySnapshotDropsOldEntries(): Unit = {
    val metadata = cache
    metadata.updateMetadata(0, request(1, "deleted"), reconcileOnControllerChange = true)
    assertEquals(Seq(new TopicPartition("deleted", 0)),
      metadata.updateMetadata(1, request(2), reconcileOnControllerChange = true))
    assertTrue(metadata.getAllTopics().isEmpty)
  }
}
