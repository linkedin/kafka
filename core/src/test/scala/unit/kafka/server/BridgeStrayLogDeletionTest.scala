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
import java.util.Properties
import kafka.api.LeaderAndIsr
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.errors.ControllerMovedException
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
  private def withManager(enabled: Boolean)(test: (KafkaConfig, ReplicaManager) => Unit): Unit = {
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
    try test(config, manager)
    finally {
      manager.shutdown(checkpointHW = false)
      logs.shutdown()
      quotas.shutdown()
      metrics.close()
      TestUtils.clearYammerMetrics()
    }
  }

  private def update(epoch: Int, states: Seq[(TopicPartition, Int, List[Int])]): UpdateMetadataRequest = {
    val topics = states.map { case (tp, leader, replicas) =>
      val partition = new UpdateMetadataPartitionState().setPartitionIndex(tp.partition)
        .setLeader(leader).setReplicas(replicas.map(Int.box).asJava)
      new UpdateMetadataTopicState().setTopicName(tp.topic)
        .setPartitionStates(java.util.Collections.singletonList(partition))
    }
    val data = new UpdateMetadataRequestData().setControllerId(0).setControllerEpoch(epoch)
      .setBrokerEpoch(1L).setTopicStates(topics.asJava)
    UpdateMetadataRequest.parse(MessageUtil.toByteBuffer(data, 5.toShort), 5.toShort)
  }

  private def addRecord(manager: ReplicaManager, tp: TopicPartition): Unit = {
    val log = manager.logManager.getOrCreateLog(tp, isNew = true, topicId = None)
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("old-generation".getBytes(java.nio.charset.StandardCharsets.UTF_8))), 0)
    assertEquals(1L, log.logEndOffset)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testMetadataDeletionRetiresOnlyUnhostedLogsWhenEnabled(enabled: Boolean): Unit = withManager(enabled) { (_, manager) =>
    val logs = manager.logManager
    val stray = new TopicPartition("bridge-stray", 0)
    val hosted = new TopicPartition("bridge-hosted", 0)
    val unrelated = new TopicPartition("bridge-unrelated", 0)
    Seq(stray, hosted, unrelated).foreach(tp => addRecord(manager, tp))
    manager.createPartition(hosted)
    manager.maybeUpdateMetadataCache(0, update(1, Seq(stray, hosted, unrelated).map(tp => (tp, 1, List(1)))))
    val deletions = Seq(stray, hosted).map(tp => (tp, LeaderAndIsr.LeaderDuringDelete, List(1)))
    assertThrows(classOf[ControllerMovedException], () => manager.maybeUpdateMetadataCache(1, update(0, deletions)))
    assertTrue(logs.getLog(stray).isDefined, "stale controllers cannot delete logs")
    manager.maybeUpdateMetadataCache(2, update(1, deletions))
    assertEquals(!enabled, logs.getLog(stray).isDefined)
    assertTrue(logs.getLog(hosted).isDefined, "hosted replicas still await StopReplica")
    assertTrue(logs.getLog(unrelated).isDefined, "an incremental deletion is not a full log image")
    if (enabled)
      assertEquals(0L, logs.getOrCreateLog(stray, isNew = true, topicId = None).logEndOffset)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testFullImageRetiresUnassignedLogsAndRetainsLeaderlessAssignments(enabled: Boolean): Unit = withManager(enabled) { (config, manager) =>
    val logs = manager.logManager
    val assigned = new TopicPartition("bridge-assigned-without-leader", 0)
    val reassigned = new TopicPartition("bridge-reassigned-away", 0)
    val deleted = new TopicPartition("bridge-missed-deletion", 0)
    val later = new TopicPartition("bridge-after-initial-image", 0)
    Seq(assigned, reassigned, deleted).foreach(tp => addRecord(manager, tp))
    val image = Seq((assigned, LeaderAndIsr.NoLeader, List(1)), (reassigned, 0, List(0)))
    manager.maybeUpdateMetadataCache(0, update(1, image))
    assertTrue(logs.getLog(assigned).isDefined, "a leaderless assignment still owns its log")
    assertEquals(!enabled, logs.getLog(reassigned).isDefined)
    assertEquals(!enabled, logs.getLog(deleted).isDefined)

    // Enabling the gate after an image was accepted must not make an incremental
    // update look like another full image in the same controller epoch.
    val props = new Properties
    props.putAll(config.originals)
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, "true")
    config.updateCurrentConfig(KafkaConfig.fromProps(props))
    addRecord(manager, later)
    manager.maybeUpdateMetadataCache(1, update(1, Seq.empty))
    assertTrue(logs.getLog(later).isDefined)
    assertTrue(logs.getLog(assigned).isDefined)
    manager.maybeUpdateMetadataCache(2, update(2, image))
    Seq(reassigned, deleted, later).foreach(tp => assertTrue(logs.getLog(tp).isEmpty, tp.toString))
    assertEquals(1L, logs.getLog(assigned).get.logEndOffset)
  }
}
