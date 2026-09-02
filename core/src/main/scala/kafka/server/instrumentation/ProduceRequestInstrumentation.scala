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

package kafka.server.instrumentation

import kafka.cluster.PendingShrinkIsr
import kafka.network.RequestChannel.Request
import kafka.server.{BrokerReconfigurable, KafkaConfig, ReplicaManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.Time
import org.slf4j.LoggerFactory

import scala.collection.mutable

object ProduceRequestInstrumentation {
  object Stage extends Enumeration {
    val Init, Authorization, BeginAppendRecords, AppendToLocalLog,
        ProcessAppendToLocalLogStatus, PrepareDelayedProduce, EnqueueDelayedProduce,
        BeginResponseCallback, ResponseThrottling, Finish = Value
  }

  val Disabled: ProduceRequestInstrumentation = new ProduceRequestInstrumentation(Time.SYSTEM) {
    override def markStage(stage: Stage.Value): Unit = ()
  }
}

class ProduceRequestInstrumentation(time: Time) {
  import ProduceRequestInstrumentation.Stage

  private[instrumentation] val marks = mutable.Map.empty[Stage.Value, Long]
  @volatile var appliedTopicPartitions: Iterable[TopicPartition] = Seq.empty

  markStage(Stage.Init)

  def markStage(stage: Stage.Value): Unit = marks.synchronized {
    marks.put(stage, time.milliseconds())
  }
}

final class ProduceRequestInstrumentationLogger(config: KafkaConfig,
                                                time: Time,
                                                random: scala.util.Random,
                                                replicaManager: ReplicaManager) extends BrokerReconfigurable {
  import ProduceRequestInstrumentation.Stage

  private val logger = LoggerFactory.getLogger("produce.request.instrumentation.logger")
  @volatile private var enabled = config.liProtocolBridgeProduceRequestInstrumentationActive
  @volatile private var thresholdMs = config.longTailProduceRequestLogThresholdMs
  @volatile private var logRatio = config.longTailProduceRequestLogRatio
  config.dynamicConfig.addBrokerReconfigurable(this)

  override def reconfigurableConfigs: Set[String] = Set(
    KafkaConfig.LiProtocolBridgeProduceRequestInstrumentationEnableProp,
    KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp,
    KafkaConfig.LiLongTailProduceRequestLogRatioProp)

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    if (newConfig.longTailProduceRequestLogThresholdMs < 0)
      throw new ConfigException(s"${KafkaConfig.LiLongTailProduceRequestLogThresholdMsProp} must be non-negative")
    val ratio = newConfig.longTailProduceRequestLogRatio
    if (ratio < 0.0 || ratio > 1.0)
      throw new ConfigException(s"${KafkaConfig.LiLongTailProduceRequestLogRatioProp} must be between 0 and 1")
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    enabled = newConfig.liProtocolBridgeProduceRequestInstrumentationActive
    thresholdMs = newConfig.longTailProduceRequestLogThresholdMs
    logRatio = newConfig.longTailProduceRequestLogRatio
  }

  private def nanosToMs(nanos: Long): Double = {
    val positiveNanos = math.max(nanos, 0L)
    java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble /
      java.util.concurrent.TimeUnit.MILLISECONDS.toMicros(1)
  }

  private[server] def toTimeTakenInEachStageMessage(
    instrumentation: ProduceRequestInstrumentation
  ): String = {
    instrumentation.markStage(Stage.Finish)
    instrumentation.marks.synchronized {
      instrumentation.marks.toSeq
        .sortBy { case (stage, timestamp) => (timestamp, stage.id) }
        .sliding(2)
        .filter(_.size == 2)
        .map { stages =>
          val (stage, start) = stages.head
          val (_, end) = stages.last
          "\"" + stage + "\":" + (end - start)
        }
        .mkString("{", ", ", "}")
    }
  }

  def maybeLog(request: Request, instrumentation: ProduceRequestInstrumentation): Unit = {
    if (!enabled)
      return

    val endTimeNanos = time.nanoseconds()
    val callbackNanos = request.callbackRequestCompleteTimeNanos.getOrElse(0L) -
      request.callbackRequestDequeueTimeNanos.getOrElse(0L)
    val totalTimeMs = nanosToMs(endTimeNanos - request.startTimeNanos)
    if (totalTimeMs < thresholdMs || random.nextDouble() > logRatio)
      return

    val requestQueueTimeMs = nanosToMs(request.requestDequeueTimeNanos - request.startTimeNanos)
    val apiLocalTimeMs = nanosToMs(request.apiLocalCompleteTimeNanos - request.requestDequeueTimeNanos + callbackNanos)
    val apiRemoteTimeMs = nanosToMs(request.responseCompleteTimeNanos - request.apiLocalCompleteTimeNanos - callbackNanos)
    val responseQueueTimeMs = nanosToMs(request.responseDequeueTimeNanos - request.responseCompleteTimeNanos)
    val responseSendTimeMs = nanosToMs(endTimeNanos - request.responseDequeueTimeNanos)
    val metrics = Seq(
      "totalTimeMs" -> Math.round(totalTimeMs),
      "requestQueueTimeMs" -> Math.round(requestQueueTimeMs),
      "apiLocalTimeMs" -> Math.round(apiLocalTimeMs),
      "apiRemoteTimeMs" -> Math.round(apiRemoteTimeMs),
      "apiThrottleTimeMs" -> request.apiThrottleTimeMs.toLong,
      "responseQueueTimeMs" -> Math.round(responseQueueTimeMs),
      "responseSendTimeMs" -> Math.round(responseSendTimeMs),
      "sizeOfBodyInBytes" -> request.sizeOfBodyInBytes.toLong,
      "responseBytes" -> request.responseBytes
    ).map { case (name, value) => "\"" + name + "\":" + value }.mkString("{", ",", "}")

    val topicPartitions = instrumentation.appliedTopicPartitions
      .map(tp => s"${tp.topic}-${tp.partition}").mkString("(", ", ", ")")
    logger.info(
      s"acks=${request.body[ProduceRequest].acks()}; request_metric=$metrics; " +
        s"topic_partitions=$topicPartitions; " +
        s"stage_breakdown_ms=${toTimeTakenInEachStageMessage(instrumentation)}; " +
        s"${classOf[PendingShrinkIsr].getSimpleName}_count=" +
        replicaManager.numPartitionsInIsrState(classOf[PendingShrinkIsr]))
  }
}
