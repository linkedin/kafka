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
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class BridgeTopicIdentityTest {
  @Test
  def testCleanupRequiresAnIbpWithTopicIdentity(): Unit = {
    val props = new java.util.Properties
    props.put("zookeeper.connect", "localhost:2181")
    props.put("inter.broker.protocol.version", "2.7")
    KafkaConfig.fromProps(props)
    props.put(KafkaConfig.LiProtocolBridgeTopicDeletionStateCleanupEnableProp, "true")
    assertThrows(classOf[org.apache.kafka.common.config.ConfigException], () => KafkaConfig.fromProps(props))
    props.put("inter.broker.protocol.version", "3.0")
    assertTrue(KafkaConfig.fromProps(props).liProtocolBridgeTopicDeletionStateCleanupActive)
  }

  @Test
  def testOnlyKnownMismatchedIdentityCanBeDiscarded(): Unit = {
    val partition = new TopicPartition("identity", 0)
    val current = Uuid.randomUuid()
    assertFalse(BridgeTopicIdentity.isObsolete(partition, Some(current), current, empty = false))
    assertTrue(BridgeTopicIdentity.isObsolete(partition, Some(Uuid.randomUuid()), current, empty = false))
    for (unknown <- Seq(None, Some(Uuid.ZERO_UUID))) {
      assertFalse(BridgeTopicIdentity.isObsolete(partition, unknown, current, empty = true))
      assertThrows(classOf[KafkaStorageException], () =>
        BridgeTopicIdentity.isObsolete(partition, unknown, current, empty = false))
    }
  }

  @Test
  def testExistingTopicRequiresAValidIdentity(): Unit = {
    val client = mock(classOf[KafkaZkClient])
    val topics = Set("identity")
    for (id <- Seq(None, Some(Uuid.ZERO_UUID))) {
      when(client.getTopicIdentities(topics)).thenReturn(Map("identity" -> id))
      assertThrows(classOf[KafkaStorageException], () => BridgeTopicIdentity.read(topics, Some(client)))
    }
    val id = Uuid.randomUuid()
    when(client.getTopicIdentities(topics)).thenReturn(Map("identity" -> Some(id)))
    assertEquals(Map("identity" -> id), BridgeTopicIdentity.read(topics, Some(client)))
    when(client.getTopicIdentities(topics)).thenReturn(Map.empty[String, Option[Uuid]])
    assertEquals(Map.empty[String, Uuid], BridgeTopicIdentity.read(topics, Some(client)))
    assertEquals(Map.empty[String, Uuid], BridgeTopicIdentity.read(Set.empty, None))
    assertThrows(classOf[IllegalStateException], () => BridgeTopicIdentity.read(topics, None))
  }

  @Test
  def testWireIdentityMustAgreeWhenPresent(): Unit = {
    val current = Uuid.randomUuid()
    val expected = Map("identity" -> current)
    BridgeTopicIdentity.verifyWireIds(expected, java.util.Collections.emptyMap[String, Uuid]())
    BridgeTopicIdentity.verifyWireIds(expected, java.util.Collections.singletonMap("identity", Uuid.ZERO_UUID))
    BridgeTopicIdentity.verifyWireIds(expected, java.util.Collections.singletonMap("identity", current))
    assertThrows(classOf[InconsistentTopicIdException], () => BridgeTopicIdentity.verifyWireIds(expected,
      java.util.Collections.singletonMap("identity", Uuid.randomUuid())))
  }

  @Test
  def testIdentityReadPreservesZooKeeperFailure(): Unit = {
    val client = mock(classOf[KafkaZkClient])
    val failure = new ConnectionLossException
    when(client.getTopicIdentities(Set("identity"))).thenAnswer(_ => throw failure)
    assertSame(failure, assertThrows(classOf[ConnectionLossException], () =>
      BridgeTopicIdentity.read(Set("identity"), Some(client))))
  }
}
