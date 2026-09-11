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

package kafka.zk

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}
import kafka.server.QuorumTestHarness
import kafka.zookeeper.{StateChangeHandler, ZNodeChildChangeHandler}
import org.apache.jute.BinaryInputArchive
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ConfigType
import org.apache.zookeeper.{KeeperException, ZooDefs, ZooKeeper}
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.common.ZKConfig
import org.apache.zookeeper.data.{ACL, Id}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/** Run with the isolated LinkedIn ZooKeeper task in tests/bin/li_bridge_zookeeper_test.gradle. */
@EnabledIfSystemProperty(named = "li.zookeeper.pagination.test", matches = "true")
@Timeout(120)
class LiZookeeperPaginationTest extends QuorumTestHarness {
  private val maxResponseBytes = 1024 * 1024
  private var paginatedClient: KafkaZkClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    assertNotNull(classOf[ZooKeeper].getMethod("getAllChildrenPaginated", classOf[String], java.lang.Boolean.TYPE))
    assertEquals(maxResponseBytes, BinaryInputArchive.maxBuffer)
    super.setUp(testInfo)
    val clientConfig = new ZKClientConfig
    clientConfig.setProperty(ZKConfig.JUTE_MAXBUFFER, maxResponseBytes.toString)
    paginatedClient = KafkaZkClient(zkConnect, isSecure = false,
      zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests, Time.SYSTEM,
      name = "LiZookeeperPaginationTest", zkClientConfig = clientConfig,
      enableEntityConfigControllerCheck = false, paginateTopics = true)
    assertEquals(maxResponseBytes.toString,
      paginatedClient.currentZooKeeper.getClientConfig.getProperty(ZKConfig.JUTE_MAXBUFFER))
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      if (paginatedClient != null) paginatedClient.close()
    } finally {
      if (implementation != null) super.tearDown()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("topics", "configs", "federated"))
  def testListingsLargerThanResponseLimit(kind: String): Unit = {
    val parentPath = kind match {
      case "topics" => TopicsZNode.path
      case "configs" => ConfigEntityTypeZNode.path(ConfigType.TOPIC)
      case "federated" => FederatedTopicZNode.namespacePath("west")
    }
    val prefix = "topic-" + "x" * 180
    val expected = (0 until 6000).map(index => s"$prefix-$index").toSet
    // Each child uses four bytes for its string length in the ZooKeeper response.
    val responseBytes = expected.iterator.map(_.getBytes(UTF_8).length.toLong + 4).sum + 4
    assertTrue(responseBytes > maxResponseBytes, s"Fixture must exceed the response limit: $responseBytes")
    zkClient.createRecursive(parentPath)
    expected.foreach(name => zkClient.createRecursive(s"$parentPath/$name"))

    val changed = new CountDownLatch(1)
    paginatedClient.registerZNodeChildChangeHandler(new ZNodeChildChangeHandler {
      override val path: String = parentPath
      override def handleChildChange(): Unit = changed.countDown()
    })
    def list(): Set[String] = kind match {
      case "topics" => paginatedClient.getAllTopicsInCluster(registerWatch = true)
      case "configs" => paginatedClient.getAllEntitiesWithConfig(ConfigType.TOPIC).toSet
      case "federated" => paginatedClient.getAllFederatedTopicsInNamespace("west", registerWatch = true)
    }

    assertEquals(expected, list())
    val added = "topic-added-after-paginated-read"
    zkClient.createRecursive(s"$parentPath/$added")
    if (kind != "configs")
      assertTrue(changed.await(10, TimeUnit.SECONDS), s"No child watch delivered for $parentPath")
    assertEquals(expected + added, list())
  }

  @Test
  def testPaginationAndWatchRenewalAfterSessionExpiry(): Unit = {
    zkClient.createRecursive(TopicsZNode.path)
    zkClient.createRecursive(s"${TopicsZNode.path}/before-expiry")
    val reconnected = new CountDownLatch(1)
    val changed = new CountDownLatch(1)
    paginatedClient.registerZNodeChildChangeHandler(new ZNodeChildChangeHandler {
      override val path: String = TopicsZNode.path
      override def handleChildChange(): Unit = changed.countDown()
    })
    paginatedClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = "pagination-watch-renewal"
      override def afterInitializingSession(): Unit = {
        assertEquals(Set("before-expiry"), paginatedClient.getAllTopicsInCluster(registerWatch = true))
        reconnected.countDown()
      }
    })
    assertEquals(Set("before-expiry"), paginatedClient.getAllTopicsInCluster(registerWatch = true))
    val session = paginatedClient.currentZooKeeper.getSessionId
    paginatedClient.currentZooKeeper.getTestable.injectSessionExpiration()
    assertTrue(reconnected.await(30, TimeUnit.SECONDS), "Paginated rescan failed after session expiry")
    assertNotEquals(session, paginatedClient.currentZooKeeper.getSessionId)
    zkClient.createRecursive(s"${TopicsZNode.path}/after-expiry")
    assertTrue(changed.await(10, TimeUnit.SECONDS), "Renewed watch did not fire")
    assertEquals(Set("before-expiry", "after-expiry"), paginatedClient.getAllTopicsInCluster(registerWatch = true))
  }

  @Test
  def testMissingAndUnauthorizedNamespaceResults(): Unit = {
    assertTrue(paginatedClient.getAllFederatedTopicsInNamespace("missing").isEmpty)
    val path = FederatedTopicZNode.namespacePath("restricted")
    zkClient.createRecursive(path)
    zkClient.currentZooKeeper.setACL(path,
      Collections.singletonList(new ACL(ZooDefs.Perms.ADMIN, new Id("world", "anyone"))), -1)
    assertThrows(classOf[KeeperException.NoAuthException],
      () => paginatedClient.getAllFederatedTopicsInNamespace("restricted"))
  }
}
