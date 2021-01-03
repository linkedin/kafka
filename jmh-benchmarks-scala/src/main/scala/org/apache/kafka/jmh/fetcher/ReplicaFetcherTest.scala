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

package org.apache.kafka.jmh.fetcher

import java.io.File
import java.util
import java.util.{Properties, Random}
import java.util.concurrent.atomic.AtomicBoolean

import kafka.log.LogManager
import kafka.server._
import kafka.utils.{KafkaScheduler, Logging, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.message.LeaderAndIsrRequestData
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.EasyMock.{expect, mock, replay}

import scala.jdk.CollectionConverters.SeqHasAsJava

class ReplicaFetcherTest(partitionCount: Int, brokerCount: Int, numReplicaFetchers: Int) extends Logging {
  private val topic0 = "topic0"
  private val localBrokerId = 100
  private val brokerEpoch: Int = 0
  private val partitionLeaders: util.Map[TopicPartition, Integer] = new util.HashMap[TopicPartition, Integer]()
  private val partitionISRs  = new util.HashMap[TopicPartition, util.List[Integer]]()
  private val leaderEpoch: Int = 0
  private val replicaAssignment: util.List[Integer] = util.Arrays.asList(100, 101, 102)
  private val controllerId: Int = 0
  private val controllerEpoch: Int = 0
  private val zkVersion: Int = 0

  private var scheduler: KafkaScheduler = null
  private var metrics: Metrics = null
  private var logManager : LogManager = null
  private var quotaManagers: kafka.server.QuotaFactory.QuotaManagers = null
  var replicaManager: ReplicaManager = null

  init()

  def init(): Unit = {
    initPartitionStates()

    scheduler = new KafkaScheduler(1, "scheduler-thread", true)
    scheduler.startup()

    val brokerConfig = TestUtils.createBrokerConfig(localBrokerId, TestUtils.MockZkConnect)
    brokerConfig.put(KafkaConfig.NumReplicaFetchersProp, numReplicaFetchers)
    val brokerProperties = KafkaConfig.fromProps(brokerConfig)

    metrics = new Metrics()
    val time = new MockTime()
    val zkClient : KafkaZkClient = mock(classOf[KafkaZkClient])
    expect(zkClient.getEntityConfigs("topics", topic0)).andReturn(new Properties()).times(partitionCount)

    replay(zkClient)

    val files = brokerProperties.logDirs.map(new File(_))

    logManager = TestUtils.createLogManager(files)

    quotaManagers = QuotaFactory.instantiate(brokerProperties, metrics, time, "")

    val brokerTopicStats = new BrokerTopicStats
    val metadataCache = createMetadataCache()

    val failureChannel = new LogDirFailureChannel(brokerProperties.logDirs.size)
    replicaManager = new MockReplicaManager(brokerProperties, metrics, time, zkClient, scheduler, logManager,
      new AtomicBoolean(false), quotaManagers,
      brokerTopicStats, metadataCache, failureChannel, Some("Mock"))

  }

  def becomeLeaderOrFollower(startTime: Long): Unit = {
    val leaderAndIsrRequest = createLeaderAndIsrRequest()
    val correlationId = 0
    replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, (_, _) => {}, startTime)
  }

  private def initPartitionStates(): Unit = {
    // for each partition, randomly choose a leader other than my self
    // other broker ids are in the range [101, 101 + brokerCount - 1)
    val random = new Random(555)
    (0 until partitionCount).foreach(partitionIndex => {
      val leader = 101 + random.nextInt(brokerCount - 1)
      val tp = new TopicPartition(topic0, partitionIndex)
      partitionLeaders.put(tp, leader)

      // choose a 3rd replica so that RF is 3
      var replica = leader
      while (replica == leader) {
        replica = 101 + random.nextInt(brokerCount - 1)
      }

      partitionISRs.put(tp, util.Arrays.asList(leader, replica))
    })
  }

  def shutdown(): Unit = {
    replicaManager.shutdown()
    logManager.shutdown()
    metrics.close()
    scheduler.shutdown()
    quotaManagers.shutdown()
    for (dir <- logManager.liveLogDirs) {
      Utils.delete(dir)
    }
  }

  private def createLeaderAndIsrRequest(): LeaderAndIsrRequest = {
    val version: Short = ApiKeys.LEADER_AND_ISR.latestVersion
    val partitionStates = new util.ArrayList[LeaderAndIsrRequestData.LeaderAndIsrPartitionState]()
    (0 until partitionCount).foreach(partitionIndex => {
      val tp = new TopicPartition(topic0, partitionIndex)
      partitionStates.add(new LeaderAndIsrRequestData.LeaderAndIsrPartitionState()
        .setTopicName(tp.topic)
        .setPartitionIndex(tp.partition)
        .setControllerEpoch(controllerEpoch)
        .setLeader(partitionLeaders.get(tp))
        .setLeaderEpoch(leaderEpoch)
        .setIsr(partitionISRs.get(tp))
        .setZkVersion(zkVersion)
        .setReplicas(replicaAssignment)
        .setIsNew(false))
    })

    val liveLeaders = util.Arrays.asList(new Node(100, "broker100", 9092), new Node(101, "broker101", 9092), new Node(102, "broker102", 9092))
    val leaderAndIsrRequestBuilder: LeaderAndIsrRequest.Builder = new LeaderAndIsrRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch, brokerEpoch, partitionStates, liveLeaders)
    leaderAndIsrRequestBuilder.build
  }

  private def createMetadataCache(): MetadataCache = {
    val cache = new MetadataCache(localBrokerId)

    def endpoints(brokerId: Int): Seq[UpdateMetadataEndpoint] = {
      val host = s"broker$brokerId"
      Seq(
        new UpdateMetadataEndpoint()
          .setHost(host)
          .setPort(9092)
          .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
          .setListener(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT).value)
      )
    }

    val brokers = (100 until (100 + brokerCount)).map { brokerId =>
      new UpdateMetadataBroker()
        .setId(brokerId)
        .setEndpoints(endpoints(brokerId).asJava)
        .setRack("rack1")
    }

    val partitionStates = new util.ArrayList[UpdateMetadataPartitionState]()
    (0 until partitionCount).foreach(partitionIndex => {
      val tp = new TopicPartition(topic0, partitionIndex)

      partitionStates.add(new UpdateMetadataPartitionState()
        .setTopicName(topic0)
        .setPartitionIndex(partitionIndex)
        .setControllerEpoch(controllerEpoch)
        .setLeader(partitionLeaders.get(tp))
        .setLeaderEpoch(leaderEpoch)
        .setIsr(partitionISRs.get(tp))
        .setZkVersion(zkVersion)
        .setReplicas(replicaAssignment))
    })

    val correlationId = 15
    val version = ApiKeys.UPDATE_METADATA.latestVersion
    val updateMetadataRequest = new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      brokerEpoch, partitionStates, brokers.asJava).build()
    cache.updateMetadata(correlationId, updateMetadataRequest)
    cache
  }


}
