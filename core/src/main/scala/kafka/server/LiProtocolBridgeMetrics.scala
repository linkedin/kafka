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

import org.apache.kafka.server.metrics.KafkaMetricsGroup

import scala.jdk.CollectionConverters._

object LiProtocolBridgeMetrics {
  val ModeEnabled = "ModeEnabled"
  val FollowerRecoveryEnabled = "FollowerRecoveryEnabled"
  val RecommendedLeaderElectionEnabled = "RecommendedLeaderElectionEnabled"
  val ExcludePartitionsEnabled = "ExcludePartitionsEnabled"
  val MoveControllerEnabled = "MoveControllerEnabled"
  val ShutdownSafetyOverrideEnabled = "ShutdownSafetyOverrideEnabled"
  val PreferredControllerEnabled = "PreferredControllerEnabled"
  val FederatedTopicsEnabled = "FederatedTopicsEnabled"
  val RackIdMapperEnabled = "RackIdMapperEnabled"
  val ZookeeperPaginationEnabled = "ZookeeperPaginationEnabled"
  val DynamicTopicDeletionEnabled = "DynamicTopicDeletionEnabled"
  val ControllerInitializationThreads = "ControllerInitializationThreads"
  val ProduceRequestInstrumentationEnabled = "ProduceRequestInstrumentationEnabled"
  val RequestMetricBucketsEnabled = "RequestMetricBucketsEnabled"
  val RequestChannelWatchdogEnabled = "RequestChannelWatchdogEnabled"
  val MetricNames: Seq[String] = Seq(ModeEnabled, FollowerRecoveryEnabled,
    RecommendedLeaderElectionEnabled, ExcludePartitionsEnabled, MoveControllerEnabled,
    ShutdownSafetyOverrideEnabled, PreferredControllerEnabled, FederatedTopicsEnabled,
    RackIdMapperEnabled, ZookeeperPaginationEnabled, DynamicTopicDeletionEnabled,
    ControllerInitializationThreads, ProduceRequestInstrumentationEnabled,
    RequestMetricBucketsEnabled, RequestChannelWatchdogEnabled)
}

/** Exposes the effective value of every 3.0-li compatibility flag on each ZooKeeper broker. */
class LiProtocolBridgeMetrics(config: KafkaConfig) extends AutoCloseable {
  import LiProtocolBridgeMetrics._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  private val tags = Map("broker-id" -> config.brokerId.toString).asJava

  metricsGroup.newGauge(ModeEnabled, () => enabled(config.liProtocolBridgeModeActive), tags)
  metricsGroup.newGauge(FollowerRecoveryEnabled,
    () => enabled(config.liProtocolBridgeFollowerRecoveryActive), tags)
  metricsGroup.newGauge(RecommendedLeaderElectionEnabled,
    () => enabled(config.liProtocolBridgeRecommendedElectionActive), tags)
  metricsGroup.newGauge(ExcludePartitionsEnabled,
    () => enabled(config.liProtocolBridgeExcludePartitionsEnable), tags)
  metricsGroup.newGauge(MoveControllerEnabled,
    () => enabled(config.liProtocolBridgeMoveControllerActive), tags)
  metricsGroup.newGauge(ShutdownSafetyOverrideEnabled,
    () => enabled(config.liProtocolBridgeShutdownSafetyOverrideActive), tags)
  metricsGroup.newGauge(PreferredControllerEnabled,
    () => enabled(config.liProtocolBridgePreferredControllerActive), tags)
  metricsGroup.newGauge(FederatedTopicsEnabled,
    () => enabled(config.liProtocolBridgeFederatedTopicsActive), tags)
  metricsGroup.newGauge(RackIdMapperEnabled,
    () => enabled(config.liProtocolBridgeRackIdMapperActive), tags)
  metricsGroup.newGauge(ZookeeperPaginationEnabled,
    () => enabled(config.liZookeeperPaginationEnable), tags)
  metricsGroup.newGauge(DynamicTopicDeletionEnabled,
    () => enabled(config.liProtocolBridgeDynamicTopicDeletionActive), tags)
  metricsGroup.newGauge(ControllerInitializationThreads,
    () => config.liNumControllerInitThreads, tags)
  metricsGroup.newGauge(ProduceRequestInstrumentationEnabled,
    () => enabled(config.liProtocolBridgeProduceRequestInstrumentationActive), tags)
  metricsGroup.newGauge(RequestMetricBucketsEnabled,
    () => enabled(config.liProtocolBridgeRequestMetricBucketsActive), tags)
  metricsGroup.newGauge(RequestChannelWatchdogEnabled,
    () => enabled(config.liProtocolBridgeRequestChannelWatchdogActive), tags)

  private def enabled(value: Boolean): Int = if (value) 1 else 0

  override def close(): Unit = {
    MetricNames.foreach { name =>
      metricsGroup.removeMetric(name, tags)
    }
  }
}
