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

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException}

private[server] object BridgeTopicIdentity {
  // Bridge control versions omit topic IDs. Read the existing ZooKeeper identity
  // when handling control requests, not on the produce/fetch data path.
  def read(topics: Set[String], client: Option[KafkaZkClient]): Map[String, Uuid] = {
    if (topics.isEmpty) return Map.empty
    val zk = client.getOrElse(throw new IllegalStateException("Bridge topic identity requires a ZooKeeper client"))
    val ids = zk.getTopicIdsForTopics(topics)
    val missing = topics.filter(topic => ids.get(topic).forall(_ == Uuid.ZERO_UUID))
    if (missing.nonEmpty)
      throw new KafkaStorageException(s"Cannot verify bridge topic identity for ${missing.toSeq.sorted.mkString(",")}")
    ids.toMap
  }

  def verifyWireIds(current: Map[String, Uuid], requested: java.util.Map[String, Uuid]): Unit = {
    current.foreach { case (topic, id) =>
      Option(requested.get(topic)).filter(_ != Uuid.ZERO_UUID).foreach { wireId =>
        if (wireId != id)
          throw new InconsistentTopicIdException(s"Controller and ZooKeeper topic identities disagree for $topic")
      }
    }
  }

  def isObsolete(partition: TopicPartition, stored: Option[Uuid], current: Uuid, empty: Boolean): Boolean = {
    stored match {
      case Some(id) if id != Uuid.ZERO_UUID => id != current
      case _ if empty => false
      case _ =>
        throw new KafkaStorageException(s"Cannot verify nonempty log $partition without a stored topic ID; " +
          "retain the log and require an operator recovery decision")
    }
  }
}
