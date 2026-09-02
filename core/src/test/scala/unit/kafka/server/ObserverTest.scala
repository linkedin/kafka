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

import kafka.network.{RequestChannel, SocketServer}
import kafka.utils.TestUtils
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.security.CredentialProvider
import org.junit.jupiter.api.Assertions.{assertInstanceOf, assertNotNull}
import org.junit.jupiter.api.Test

class ObserverTest {
  @Test
  def testBlankObserverClassUsesNoOpObserver(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.ObserverClassNameProp, "  ")

    val observer = Observer(KafkaConfig.fromProps(props))
    assertInstanceOf(classOf[NoOpObserver], observer)
  }

  @Test
  def testObserverParametersPreserveOldJvmConstructors(): Unit = {
    assertNotNull(classOf[RequestChannel].getConstructor(
      classOf[Int], classOf[String], classOf[Time], classOf[RequestChannel.Metrics]))
    assertNotNull(classOf[SocketServer].getConstructor(
      classOf[KafkaConfig], classOf[Metrics], classOf[Time], classOf[CredentialProvider],
      classOf[ApiVersionManager]))
    assertNotNull(classOf[KafkaServer].getConstructor(
      classOf[KafkaConfig], classOf[Time], classOf[Option[_]], classOf[Boolean]))
  }
}
