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
import kafka.server.{KafkaConfig, ReplicaFetcherManager, ReplicaFetcherThread, ReplicaManager, ReplicationQuotaManager}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

class MockReplicaFetcherManager(brokerConfig: KafkaConfig,
                                replicaManager: ReplicaManager,
                                metrics: Metrics,
                                time: Time,
                                threadNamePrefix: Option[String] = None,
                                quotaManager: ReplicationQuotaManager)
  extends ReplicaFetcherManager(brokerConfig = brokerConfig,
  replicaManager = replicaManager,
  metrics = metrics,
  time = time,
  threadNamePrefix = threadNamePrefix,
  quotaManager = quotaManager) {
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp:").getOrElse("")
    val threadName = s"${prefix}ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    new MockReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig, failedPartitions, replicaManager,
      metrics, time, quotaManager)
  }

}