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

import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.server._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters.MapHasAsScala

class MockReplicaFetcherThread(name: String,
                               fetcherId: Int,
                               sourceBroker: BrokerEndPoint,
                               brokerConfig: KafkaConfig,
                               failedPartitions: FailedPartitions,
                               replicaMgr: ReplicaManager,
                               metrics: Metrics,
                               time: Time,
                               quota: ReplicaQuota,
                               leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends ReplicaFetcherThread(name = name,
  fetcherId = fetcherId,
  sourceBroker = sourceBroker,
  brokerConfig = brokerConfig,
  failedPartitions = failedPartitions,
  replicaMgr = replicaMgr,
  metrics = metrics,
  time = time,
  quota = quota,
  leaderEndpointBlockingSend = leaderEndpointBlockingSend) {

  override protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, FetchData)] = {
    val fetchData = new FetchResponse.PartitionData[Records](Errors.NONE, 0L, 0L, 0, null, null)
    fetchRequest.fetchData.asScala.map{case (partition, _) => (partition, fetchData)}.toSeq
  }

  override def processPartitionData(topicPartition: TopicPartition,
                                     fetchOffset: Long,
                                     partitionData: FetchData): Option[LogAppendInfo] = {
    None
  }

}

