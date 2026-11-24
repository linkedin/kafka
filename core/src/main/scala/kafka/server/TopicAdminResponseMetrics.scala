/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestMetrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

import scala.collection
import scala.jdk.CollectionConverters._

private[server] object TopicAdminResponseMetrics {
  val MetricName = "TopicResponses"
  val Success = "success"
  val TimedOut = "timed-out"
  val Failed = "failed"

  private val SuccessLatency = "SuccessfulTopicResponseTime"
  private val requests = Seq(ApiKeys.CREATE_TOPICS, ApiKeys.DELETE_TOPICS)
  private val outcomes = Seq(Success, TimedOut, Failed)
}

private[server] class TopicAdminResponseMetrics(brokerId: Int, latencyBucketStartsMs: Array[Int]) extends KafkaMetricsGroup {
  import TopicAdminResponseMetrics._

  private val counters = (for {
    api <- requests
    outcome <- outcomes
  } yield (api, outcome) -> newCounter(MetricName, requestTags(api) + ("outcome" -> outcome))).toMap

  private val bucketStarts = latencyBucketStartsMs.distinct.sorted
  private val successLatency = requests.map { api =>
    api -> RequestMetrics.getBucketBoundaryObjectMap(bucketStarts, SuccessLatency, "Ms",
      name => (name, newCounter(name, requestTags(api))), addBinNum = true)
  }.toMap

  private def requestTags(api: ApiKeys): Map[String, String] =
    Map("broker-id" -> brokerId.toString, "request" -> api.name)

  def record(api: ApiKeys, elapsedNanos: Long, errorCounts: collection.Map[Errors, Integer]): Unit = {
    errorCounts.foreach { case (error, count) =>
      val outcome = error match {
        case Errors.NONE => Success
        case Errors.REQUEST_TIMED_OUT => TimedOut
        case _ => Failed
      }
      counters((api, outcome)).inc(count.longValue())

      if (error == Errors.NONE) {
        // Topics in one response share its elapsed time. Count each success
        // in the bucket for that time.
        val elapsedMs = math.min(TimeUnit.NANOSECONDS.toMillis(elapsedNanos), Int.MaxValue).toInt
        val (_, bucket) = RequestMetrics.getBucketObject(successLatency(api), elapsedMs)
        bucket.inc(count.longValue())
      }
    }
  }

  def close(): Unit = {
    counters.keys.foreach { case (api, outcome) => removeMetric(MetricName, requestTags(api) + ("outcome" -> outcome)) }

    successLatency.foreach { case (api, buckets) =>
      buckets.values().asScala.foreach { case (name, _) => removeMetric(name, requestTags(api)) }
    }
  }
}
