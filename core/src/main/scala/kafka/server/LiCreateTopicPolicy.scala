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

import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.policy.CreateTopicPolicy

import scala.jdk.CollectionConverters._

/** Enforces the deployment's default replication factor as the minimum for newly created topics. */
class LiCreateTopicPolicy extends CreateTopicPolicy {
  private var minimumReplicationFactor = 0

  override def validate(metadata: CreateTopicPolicy.RequestMetadata): Unit = {
    val assignments = metadata.replicasAssignments()
    val replicationFactor = metadata.replicationFactor()
    if (assignments == null && replicationFactor == null)
      throw new PolicyViolationException(
        s"Topic ${metadata.topic()} is missing both replica assignment and replication factor")

    if (assignments != null) {
      assignments.asScala.foreach { case (partition, replicas) =>
        if (replicas.size < minimumReplicationFactor)
          throw new PolicyViolationException(
            s"Topic ${metadata.topic()} partition $partition has replication factor ${replicas.size}; " +
              s"minimum is $minimumReplicationFactor")
      }
    } else if (replicationFactor < minimumReplicationFactor) {
      throw new PolicyViolationException(
        s"Topic ${metadata.topic()} has replication factor $replicationFactor; minimum is $minimumReplicationFactor")
    }
  }

  override def configure(configs: java.util.Map[String, _]): Unit = {
    minimumReplicationFactor = Option(configs.get(KafkaConfig.DefaultReplicationFactorProp))
      .map(_.toString.toInt)
      .getOrElse(ReplicationConfigs.REPLICATION_FACTOR_DEFAULT)
  }

  override def close(): Unit = ()
}
