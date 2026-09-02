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

import kafka.cluster.{AlterPartitionListener, DelayedOperations, Partition}
import kafka.log.LogManager
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class LegacyBrokerConstructorTest {
  @Test
  def testPartitionConstructorKeepsItsJvmSignature(): Unit = {
    assertNotNull(classOf[Partition].getConstructor(
      classOf[TopicPartition], java.lang.Long.TYPE, classOf[MetadataVersion], Integer.TYPE,
      classOf[Function0[Long]], classOf[Time], classOf[AlterPartitionListener], classOf[DelayedOperations],
      classOf[MetadataCache], classOf[LogManager], classOf[AlterPartitionManager], classOf[Option[Uuid]]))
  }

  @Test
  def testZooKeeperClientConstructorKeepsItsJvmSignature(): Unit = {
    assertNotNull(classOf[KafkaZkClient].getConstructor(classOf[ZooKeeperClient],
      java.lang.Boolean.TYPE, classOf[Time], java.lang.Boolean.TYPE))
  }
}
