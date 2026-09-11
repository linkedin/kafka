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
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.{ControllerMovedException, KafkaStorageException}
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataPartitionState, UpdateMetadataTopicState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, UpdateMetadataRequest}
import org.apache.kafka.server.util.{MockScheduler, MockTime}
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._

class BridgeStrayLogDeletionTest {
  private val currentTopicId = Uuid.randomUuid()
  private def withManager(enabled: Boolean)(test: (KafkaConfig, ReplicaManager) => Unit): Unit = {
    val props = TestUtils.createBrokerConfig(1, TestUtils.MockZkConnect)
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, enabled.toString)
    props.put("log.dirs", TestUtils.tempDir().getAbsolutePath + "," + TestUtils.tempDir().getAbsolutePath)
    val config = KafkaConfig.fromProps(props)
    val time = new MockTime
    val metrics = new Metrics
    val quotas = QuotaFactory.instantiate(config, metrics, time, "")
    val logs = TestUtils.createLogManager(config.logDirs.map(new File(_)))
    val zk = mock(classOf[KafkaZkClient])
    when(zk.getTopicIdentities(any[Set[String]])).thenAnswer { invocation =>
      invocation.getArgument[Set[String]](0).map(_ -> Some(currentTopicId)).toMap
    }
    val manager = new ReplicaManager(config = config, metrics = metrics, time = time, zkClient = Some(zk),
      scheduler = new MockScheduler(time), logManager = logs, quotaManagers = quotas,
      metadataCache = MetadataCache.zkMetadataCache(config.brokerId, config.interBrokerProtocolVersion),
      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size),
      alterPartitionManager = mock(classOf[AlterPartitionManager]))
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

  private def addRecord(manager: ReplicaManager, tp: TopicPartition,
                        id: Option[Uuid] = Some(currentTopicId)): Unit = {
    val log = manager.logManager.getOrCreateLog(tp, isNew = true, topicId = id)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
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
  def testBridgeLeaderRequestPersistsIdentityBeforeAcceptingRecords(enabled: Boolean): Unit = withManager(enabled) { (_, manager) =>
    val partition = new TopicPartition("bridge-new-log", 0)
    val state = new LeaderAndIsrPartitionState().setTopicName(partition.topic).setPartitionIndex(0)
      .setControllerEpoch(1).setLeader(1).setLeaderEpoch(0).setIsNew(true)
      .setReplicas(java.util.Arrays.asList(Int.box(1))).setIsr(java.util.Arrays.asList(Int.box(1)))
    val request = new LeaderAndIsrRequest.Builder(2.toShort, 0, 1, 1L,
      java.util.Collections.singletonList(state), java.util.Collections.emptyMap[String, Uuid](),
      java.util.Collections.emptySet[org.apache.kafka.common.Node]()).build()
    manager.becomeLeaderOrFollower(0, request, (_, _) => ())
    val log = manager.logManager.getLog(partition).get
    assertEquals(if (enabled) Some(currentTopicId) else None, log.topicId)
    assertEquals(0L, log.logEndOffset)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testReplicaRequestRechecksAnExistingHostedLog(enabled: Boolean): Unit = withManager(enabled) { (_, manager) =>
    val partition = new TopicPartition("bridge-new-hosted-generation", 0)
    val oldId = Uuid.randomUuid()
    addRecord(manager, partition, Some(oldId))
    manager.createPartition(partition)
    val state = new LeaderAndIsrPartitionState().setTopicName(partition.topic).setPartitionIndex(0)
      .setControllerEpoch(1).setLeader(1).setLeaderEpoch(0).setIsNew(false)
      .setReplicas(java.util.Arrays.asList(Int.box(1))).setIsr(java.util.Arrays.asList(Int.box(1)))
    val request = new LeaderAndIsrRequest.Builder(2.toShort, 0, 1, 1L,
      java.util.Collections.singletonList(state), java.util.Collections.emptyMap[String, Uuid](),
      java.util.Collections.emptySet[org.apache.kafka.common.Node]()).build()
    manager.becomeLeaderOrFollower(0, request, (_, _) => ())
    val log = manager.logManager.getLog(partition).get
    assertEquals(Some(if (enabled) currentTopicId else oldId), log.topicId)
    assertEquals(if (enabled) 0L else 1L, log.logEndOffset)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testAssignedOldTopicIdentityIsNotReused(enabled: Boolean): Unit = withManager(enabled) { (_, manager) =>
    val partition = new TopicPartition("bridge-recreated-assigned", 0)
    addRecord(manager, partition, Some(Uuid.randomUuid()))
    manager.maybeUpdateMetadataCache(0, update(1, Seq((partition, 1, List(1)))))
    assertEquals(!enabled, manager.logManager.getLog(partition).isDefined)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testUnknownNonemptyLogIdentityIsRetainedForRecovery(enabled: Boolean): Unit = withManager(enabled) { (_, manager) =>
    val partition = new TopicPartition("bridge-unidentified-log", 0)
    addRecord(manager, partition, None)
    val request = update(1, Seq((partition, 1, List(1))))
    if (enabled) {
      assertThrows(classOf[KafkaStorageException], () => manager.maybeUpdateMetadataCache(0, request))
      assertThrows(classOf[KafkaStorageException], () => manager.maybeUpdateMetadataCache(1, request))
    } else manager.maybeUpdateMetadataCache(0, request)
    assertEquals(1L, manager.logManager.getLog(partition).get.logEndOffset)
  }

  @org.junit.jupiter.api.Test
  def testDeletedTopicDoesNotBlockAnUnrelatedLeaderRequest(): Unit = withManager(enabled = true) { (_, manager) =>
    val gone = new TopicPartition("bridge-gone", 0)
    val live = new TopicPartition("bridge-live", 0)
    addRecord(manager, gone)
    when(manager.zkClient.get.getTopicIdentities(Set(gone.topic, live.topic)))
      .thenReturn(Map(live.topic -> Some(currentTopicId)))
    val states = Seq(gone, live).map { tp =>
      new LeaderAndIsrPartitionState().setTopicName(tp.topic).setPartitionIndex(0)
        .setControllerEpoch(1).setLeader(1).setLeaderEpoch(0).setIsNew(true)
        .setReplicas(java.util.Arrays.asList(Int.box(1))).setIsr(java.util.Arrays.asList(Int.box(1)))
    }
    val request = new LeaderAndIsrRequest.Builder(2.toShort, 0, 1, 1L, states.asJava,
      java.util.Collections.emptyMap[String, Uuid](), java.util.Collections.emptySet[org.apache.kafka.common.Node]()).build()
    val response = manager.becomeLeaderOrFollower(0, request, (_, _) => ())
    assertTrue(response.errorCounts().containsKey(org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION))
    assertEquals(HostedPartition.None, manager.getPartition(gone))
    assertEquals(1L, manager.logManager.getLog(gone).get.logEndOffset)
    assertEquals(Some(currentTopicId), manager.logManager.getLog(live).get.topicId)
  }

  @org.junit.jupiter.api.Test
  def testIdentityFailureRetriesTheSameFullImage(): Unit = withManager(enabled = true) { (_, manager) =>
    val partition = new TopicPartition("bridge-retry-identity", 0)
    addRecord(manager, partition, Some(Uuid.randomUuid()))
    when(manager.zkClient.get.getTopicIdentities(Set(partition.topic)))
      .thenReturn(Map(partition.topic -> None), Map(partition.topic -> Some(currentTopicId)))
    val request = update(1, Seq((partition, 1, List(1))))
    assertThrows(classOf[KafkaStorageException], () => manager.maybeUpdateMetadataCache(0, request))
    assertEquals(1L, manager.logManager.getLog(partition).get.logEndOffset)
    manager.maybeUpdateMetadataCache(1, request)
    assertTrue(manager.logManager.getLog(partition).isEmpty)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testConflictingCurrentAndFutureIdentitiesAreRetained(enabled: Boolean): Unit = withManager(enabled) { (config, manager) =>
    val partition = new TopicPartition("bridge-conflicting-copies", 0)
    addRecord(manager, partition)
    val current = manager.logManager.getLog(partition).get
    val otherDirectory = config.logDirs.find(dir => new File(dir).getAbsolutePath != current.dir.getParentFile.getAbsolutePath).get
    manager.logManager.maybeUpdatePreferredLogDir(partition, otherDirectory)
    manager.logManager.getOrCreateLog(partition, isNew = true, isFuture = true, topicId = Some(Uuid.randomUuid()))
    val request = update(1, Seq((partition, 1, List(1))))
    if (enabled)
      assertThrows(classOf[KafkaStorageException], () => manager.maybeUpdateMetadataCache(0, request))
    else manager.maybeUpdateMetadataCache(0, request)
    assertEquals(1L, manager.logManager.getLog(partition).get.logEndOffset)
    assertTrue(manager.logManager.getLog(partition, isFuture = true).isDefined)
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
