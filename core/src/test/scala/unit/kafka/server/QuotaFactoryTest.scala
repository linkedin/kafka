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

import kafka.server.QuotaType.{Fetch, Produce}
import org.apache.kafka.common.metrics.{Metrics, Quota}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.apache.kafka.network.Session
import org.apache.kafka.server.config.{ClientQuotaManagerConfig, ZkConfigs}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.net.InetAddress
import java.util.Properties

class QuotaFactoryTest {
  @Test
  def testStaticFetchQuotaCapsRequestSizeAndTracksDynamicOverrides(): Unit = {
    val metrics = new Metrics
    val config = new ClientQuotaManagerConfig(11, 1, 1234L)
    val manager = new ClientQuotaManager(config, metrics, Fetch, Time.SYSTEM, "test-")
    val session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "reader"), InetAddress.getLoopbackAddress)
    try {
      assertEquals(1234.0, manager.quota("reader", "consumer").bound, 0.001)
      assertEquals(12340.0, manager.getMaxValueInQuotaWindow(session, "consumer"), 0.001)
      manager.updateQuota(None, Some("consumer"), Some("consumer"), Some(Quota.upperBound(20.0)))
      assertEquals(200.0, manager.getMaxValueInQuotaWindow(session, "consumer"), 0.001)
      manager.updateQuota(None, Some("consumer"), Some("consumer"), None)
      assertEquals(12340.0, manager.getMaxValueInQuotaWindow(session, "consumer"), 0.001)
    } finally {
      manager.shutdown()
      metrics.close()
    }
  }

  @Test
  def testStaticQuotaDefaultsRequireCompatibilityGate(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.put(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, "1234")
    props.put(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, "5678")

    val disabled = KafkaConfig.fromProps(props)
    assertEquals(Long.MaxValue, QuotaFactory.clientProduceConfig(disabled).quotaDefault)
    assertEquals(Long.MaxValue, QuotaFactory.clientFetchConfig(disabled).quotaDefault)

    props.put(KafkaConfig.LiProtocolBridgeStaticDefaultQuotasEnableProp, "true")
    val enabled = KafkaConfig.fromProps(props)
    val produceConfig = QuotaFactory.clientProduceConfig(enabled)
    assertEquals(1234L, produceConfig.quotaDefault)
    assertEquals(5678L, QuotaFactory.clientFetchConfig(enabled).quotaDefault)

    val metrics = new Metrics
    val manager = new ClientQuotaManager(produceConfig, metrics, Produce, Time.SYSTEM, "test-")
    try assertTrue(manager.quotasEnabled)
    finally {
      manager.shutdown()
      metrics.close()
    }
  }
}
