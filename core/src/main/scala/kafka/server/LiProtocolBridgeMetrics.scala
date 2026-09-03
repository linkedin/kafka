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

import kafka.metrics.KafkaMetricsGroup

import scala.util.Try

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
  val MinimumLogRollEnabled = "MinimumLogRollEnabled"
  val ReassignmentCancellationSafetyEnabled = "ReassignmentCancellationSafetyEnabled"
  val ListOffsetsInstrumentationEnabled = "ListOffsetsInstrumentationEnabled"
  val StaticDefaultQuotasEnabled = "StaticDefaultQuotasEnabled"
  val ReplicaRequestTimeoutEnabled = "ReplicaRequestTimeoutEnabled"
  val OffsetsTopicConfigEnabled = "OffsetsTopicConfigEnabled"
  val LeaderTransferEnabled = "LeaderTransferEnabled"
  val LegacyRequestMetricsEnabled = "LegacyRequestMetricsEnabled"
  val LogTruncationMetricsEnabled = "LogTruncationMetricsEnabled"

  val MetricNames: Seq[String] = Seq(ModeEnabled, FollowerRecoveryEnabled,
    RecommendedLeaderElectionEnabled, ExcludePartitionsEnabled, MoveControllerEnabled,
    ShutdownSafetyOverrideEnabled, PreferredControllerEnabled, FederatedTopicsEnabled,
    RackIdMapperEnabled, ZookeeperPaginationEnabled, DynamicTopicDeletionEnabled,
    ControllerInitializationThreads, ProduceRequestInstrumentationEnabled,
    RequestMetricBucketsEnabled, RequestChannelWatchdogEnabled, MinimumLogRollEnabled,
    ReassignmentCancellationSafetyEnabled, ListOffsetsInstrumentationEnabled,
    StaticDefaultQuotasEnabled, ReplicaRequestTimeoutEnabled, OffsetsTopicConfigEnabled,
    LeaderTransferEnabled, LegacyRequestMetricsEnabled, LogTruncationMetricsEnabled)
}

final class LiProtocolBridgeMetrics(config: KafkaConfig) extends KafkaMetricsGroup with AutoCloseable {
  import LiProtocolBridgeMetrics._

  private val tags = Map("broker-id" -> config.brokerId.toString)

  newGauge(ModeEnabled, () => if (config.liProtocolBridgeModeEnable) 1 else 0, tags)
  // These behaviors are built in and ungated on the 3.0-li line. A value of one tells operators
  // which equivalent default-off settings must be enabled on 3.9-li during the mixed roll.
  Seq(FollowerRecoveryEnabled, RecommendedLeaderElectionEnabled, ExcludePartitionsEnabled,
    MoveControllerEnabled, ShutdownSafetyOverrideEnabled, PreferredControllerEnabled,
    FederatedTopicsEnabled, DynamicTopicDeletionEnabled, RequestMetricBucketsEnabled,
    RequestChannelWatchdogEnabled, ReassignmentCancellationSafetyEnabled,
    ListOffsetsInstrumentationEnabled, ReplicaRequestTimeoutEnabled, OffsetsTopicConfigEnabled,
    LeaderTransferEnabled, LegacyRequestMetricsEnabled, LogTruncationMetricsEnabled).foreach { name =>
    newGauge(name, () => 1, tags)
  }
  newGauge(RackIdMapperEnabled, () => {
    val mapper = config.getString(KafkaConfig.LiRackIdMapperClassNameForRackAwareReplicaAssignmentProp)
    if (mapper == null || mapper.isEmpty) 0 else 1
  }, tags)
  newGauge(ZookeeperPaginationEnabled,
    () => if (config.liZookeeperPaginationEnable) 1 else 0, tags)
  newGauge(ControllerInitializationThreads, () => config.liNumControllerInitThreads, tags)
  newGauge(ProduceRequestInstrumentationEnabled,
    () => if (config.longTailProduceRequestLogRatio > 0.0) 1 else 0, tags)
  newGauge(MinimumLogRollEnabled, () => {
    // This is a topic-level log setting, so KafkaConfig does not validate a broker-level original.
    // Treat a malformed original as disabled rather than letting an MBean read fail.
    val configuredValue = Option(config.originals.get(KafkaConfig.LiMinLogRollTimeMillisProp))
      .flatMap(value => Try(value.toString.toLong).toOption)
      .getOrElse(0L)
    if (configuredValue > 0L) 1 else 0
  }, tags)
  newGauge(StaticDefaultQuotasEnabled, () => {
    val producerDefault = config.producerQuotaBytesPerSecondDefault
    val consumerDefault = config.consumerQuotaBytesPerSecondDefault
    if (producerDefault < Long.MaxValue || consumerDefault < Long.MaxValue) 1 else 0
  }, tags)

  override def close(): Unit = MetricNames.foreach(name => removeMetric(name, tags))
}
