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

import java.io.File
import kafka.api.LeaderAndIsr
import org.apache.kafka.common.errors.ControllerMovedException
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataPartitionState, UpdateMetadataTopicState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import kafka.utils.{MockScheduler, MockTime}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.mock

import scala.jdk.CollectionConverters._

class BridgeStrayLogDeletionTest {
  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testMetadataDeletionRetiresOnlyUnhostedLogsWhenEnabled(enabled: Boolean): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, enabled.toString)
    val config = KafkaConfig.fromProps(props)
    val time = new MockTime
    val metrics = new Metrics
    val quotas = QuotaFactory.instantiate(config, metrics, time, "")
    val logs = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val manager = new ReplicaManager(config, metrics, time, None, new MockScheduler(time), logs, None,
      new java.util.concurrent.atomic.AtomicBoolean(false), quotas, new BrokerTopicStats,
      MetadataCache.zkMetadataCache(config.brokerId), new LogDirFailureChannel(config.logDirs.size),
      mock(classOf[AlterIsrManager]), mock(classOf[TransferLeaderManager]))
    val stray = new TopicPartition("bridge-stray", 0)
    val hosted = new TopicPartition("bridge-hosted", 0)
    val unrelated = new TopicPartition("bridge-unrelated", 0)
    def update(epoch: Int, deleted: Boolean): UpdateMetadataRequest = {
      val states = Seq(stray, hosted).map { tp =>
        val partition = new UpdateMetadataPartitionState().setPartitionIndex(0)
          .setLeader(if (deleted) LeaderAndIsr.LeaderDuringDelete else 1)
          .setReplicas(java.util.Arrays.asList(Int.box(1)))
        new UpdateMetadataTopicState().setTopicName(tp.topic)
          .setPartitionStates(java.util.Collections.singletonList(partition))
      }
      val data = new UpdateMetadataRequestData().setControllerId(0).setControllerEpoch(epoch)
        .setBrokerEpoch(1L).setTopicStates(states.asJava)
      UpdateMetadataRequest.parse(MessageUtil.toByteBuffer(data, 5.toShort), 5.toShort)
    }
    try {
      Seq(stray, hosted, unrelated).foreach { tp =>
        val log = logs.getOrCreateLog(tp, isNew = true, topicId = None)
        log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE,
          new SimpleRecord("old-generation".getBytes(java.nio.charset.StandardCharsets.UTF_8))), 0)
        assertEquals(1L, log.logEndOffset)
      }
      manager.createPartition(hosted)
      manager.maybeUpdateMetadataCache(0, update(1, deleted = false))
      assertThrows(classOf[ControllerMovedException], () =>
        manager.maybeUpdateMetadataCache(1, update(0, deleted = true)))
      assertTrue(logs.getLog(stray).isDefined, "stale controllers cannot delete logs")
      manager.maybeUpdateMetadataCache(2, update(1, deleted = true))
      assertEquals(!enabled, logs.getLog(stray).isDefined)
      assertTrue(logs.getLog(hosted).isDefined, "hosted replicas still await StopReplica")
      assertTrue(logs.getLog(unrelated).isDefined, "an incremental deletion is not a full log image")
      if (enabled)
        assertEquals(0L, logs.getOrCreateLog(stray, isNew = true, topicId = None).logEndOffset)
    } finally {
      manager.shutdown(checkpointHW = false)
      logs.shutdown()
      quotas.shutdown()
      metrics.close()
      TestUtils.clearYammerMetrics()
    }
  }
}
