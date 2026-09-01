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

package kafka.network

import com.yammer.metrics.core.Counter
import kafka.server.KafkaConfig
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.server.config.ZkConfigs
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties
import scala.jdk.CollectionConverters._

class RequestMetricBucketsTest {
  @Test
  def testSizeMetricAndTotalTimeBucketNames(): Unit = {
    val props = new Properties
    props.put(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    props.put(KafkaConfig.LiProtocolBridgeRequestMetricBucketsEnableProp, "true")
    props.put(KafkaConfig.RequestMetricsSizeBucketsProp, "0,1")
    props.put(KafkaConfig.RequestMetricsTotalTimeBucketsProp, "0,5")
    props.put(KafkaConfig.TotalTimeHistogramEnabledMetricsProp, "Produce0To1MbAcks1")
    val metrics = new RequestChannel.Metrics(Seq(ApiKeys.PRODUCE, ApiKeys.FETCH),
      Some(KafkaConfig.fromProps(props)))

    try {
      assertEquals(Some("Produce0To1MbAcks1"), metrics.requestSizeBucketMetricName(
        metrics.produceRequestAcksSizeMetricNameMap(1), 512 * 1024))
      val requestMetrics = metrics("Produce0To1MbAcks1")
      requestMetrics.totalTimeBucketHist.get.update(8)
      val matchingCounters = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collect {
        case (name, counter: Counter)
          if name.getName == "TotalTime_Bin2_5MsGreater" &&
            name.getMBeanName.contains("request=Produce0To1MbAcks1") => counter.count
      }
      assertEquals(Seq(1L), matchingCounters.toSeq)
    } finally {
      metrics.close()
    }
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.forall(
      !_.getMBeanName.contains("request=Produce0To1MbAcks1")))
  }
}
