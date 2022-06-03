/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.consumer

import java.net.SocketTimeoutException
import java.util
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients._
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.{JmxReporter, MetricConfig, Metrics, MetricsReporter}
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, SystemTime}

import scala.collection.JavaConverters._

/**
 * Simple wrapper around the network client for handling SSL communication
 * @param config The old consumer config
 */
class SSLNetworkClient(config: ConsumerConfig, metadataUpdater: ManualMetadataUpdater) {
  private val socketTimeoutMs = config.socketTimeoutMs
  private val time = new SystemTime
  private val networkClient = {
    val sslConfigs = config.sslConfigs.get
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      SecurityProtocol.SSL,
      null /* contextType n/a for SSL */,
      sslConfigs,
      null /* listenerName n/a for SSL */,
      null /* clientSaslMechanism n/a for SSL */,
      time,
      false /* saslHandshakeRequestEnable n/a for SSL */
    )

    val metricConfig = new MetricConfig().samples(sslConfigs.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
      .timeWindow(sslConfigs.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
    val reporters = new util.ArrayList[MetricsReporter]()
    reporters.add(new JmxReporter("kafka.consumer"))
    val metrics = new Metrics(metricConfig, reporters, time)
    val logContext = new LogContext()
    val selector = new KSelector(
      NetworkReceive.UNLIMITED,
      KSelector.NO_IDLE_TIMEOUT_MS /* idle timeout */,
      metrics,
      time,
      "zkConsumer-fetcher" /* metrics group prefix */,
      Map[String, String]().asJava /* metrics tags */,
      false /* metricsPerConnection */,
      channelBuilder,
      logContext
    )

    new NetworkClient(
      selector,
      metadataUpdater,
      config.clientId,
      1 /* max inflight requests per connection */,
      0 /* reconnect backoff */,
      0 /* reconnect backoffMax */,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      socketTimeoutMs,
      ClientDnsLookup.USE_ALL_DNS_IPS,
      time,
      true,
      new ApiVersions,
      logContext
    )
  }

  private [consumer] def close(): Unit = synchronized {
    networkClient.close()
  }

  private [consumer] def sendRequest(node: Node, apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = synchronized {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, node, time, socketTimeoutMs)) {
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")
      }
      else {
        val clientRequest = networkClient.newClientRequest(node.id.toString, requestBuilder,
          time.milliseconds(), true)
        NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(node.id.toString)
        throw e
    }
  }

  private [consumer] def sendRequest(apiKey: ApiKeys, requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = synchronized {
    val node = networkClient.leastLoadedNode(time.milliseconds())
    sendRequest(node, apiKey, requestBuilder)
  }
}
