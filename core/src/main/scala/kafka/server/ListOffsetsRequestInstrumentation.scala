/*
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

import com.yammer.metrics.core.{Histogram, Meter}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.security.auth.KafkaPrincipal

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.collection.{concurrent, mutable}
import scala.jdk.CollectionConverters._


/**
 * Tracks which services and topics issue ListOffsets requests for explicit timestamps and records
 * the number of partitions requested for each timestamp mode.
 */
class ListOffsetsRequestInstrumentation(enabled: Boolean = true) {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  private val partitionRequestRate = "ListOffsetsPartitionsRequestRate"
  private val partitionsPerRequest = "ListOffsetsPartitionsPerRequest"
  private val eventType = "partitions"
  private val listBy = "listBy"
  private val UNKNOWN = "UNKNOWN"
  private val EARLIEST = "EARLIEST"
  private val LATEST = "LATEST"
  private val LI_EARLIEST_LOCAL = "LI_EARLIEST_LOCAL"
  private val MAX = "MAX"
  private val BY_TIMESTAMP = "BY_TIMESTAMP"

  private def tags(value: String) = Map(listBy -> value).asJava

  private def meter(value: String): Option[Meter] = if (enabled)
    Some(metricsGroup.newMeter(partitionRequestRate, eventType, TimeUnit.SECONDS, tags(value))) else None
  private def histogram(value: String): Option[Histogram] = if (enabled)
    Some(metricsGroup.newHistogram(partitionsPerRequest, true, tags(value))) else None

  private val unknownTimestampMeter = meter(UNKNOWN)
  private val unknownTimestampHist = histogram(UNKNOWN)
  private val earliestTimestampMeter = meter(EARLIEST)
  private val earliestTimestampHist = histogram(EARLIEST)
  private val latestTimestampMeter = meter(LATEST)
  private val latestTimestampHist = histogram(LATEST)
  private val liEarliestLocalTimestampMeter = meter(LI_EARLIEST_LOCAL)
  private val liEarliestLocalTimestampHist = histogram(LI_EARLIEST_LOCAL)
  private val maxTimestampMeter = meter(MAX)
  private val maxTimestampHist = histogram(MAX)
  private val byTimestampMeter = meter(BY_TIMESTAMP)
  private val byTimestampHist = histogram(BY_TIMESTAMP)


  // The object is periodically dumped to the log and cleared by the kafka-server wrapper.
  private var listOffsetsByTimestampApiClientUsers =
    mutable.Map.empty[String, concurrent.Map[String, AtomicInteger]]

  /**
   * Returns the tracked explicit-timestamp callers and starts a new empty collection window.
   */
  def snapshotAndResetListOffsetByTimeStampApiUsers(): mutable.Map[String, concurrent.Map[String, AtomicInteger]] =
    synchronized {
      val old = listOffsetsByTimestampApiClientUsers
      listOffsetsByTimestampApiClientUsers = mutable.Map()
      old
    }

  def close(): Unit = {
    if (enabled) {
      Seq(UNKNOWN, EARLIEST, LATEST, LI_EARLIEST_LOCAL, MAX, BY_TIMESTAMP).foreach { value =>
        metricsGroup.removeMetric(partitionRequestRate, tags(value))
        metricsGroup.removeMetric(partitionsPerRequest, tags(value))
      }
    }
  }

  def logUsage(principal: KafkaPrincipal, topic: ListOffsetsTopic): Unit = {
    if (!enabled) return

    var earliestCnt = 0
    var latestCnt = 0
    var liEarliestLocalCnt = 0
    var maxCnt = 0
    var unknownCnt = 0
    var byTimestampCnt = 0
    topic.partitions().asScala.foreach { partition =>
      partition.timestamp() match {
        // special types like EARLIEST are constants < 0
        case ListOffsetsRequest.EARLIEST_TIMESTAMP => earliestCnt += 1
        case ListOffsetsRequest.LATEST_TIMESTAMP => latestCnt += 1
        case timestamp if ListOffsetsRequest.isEarliestLocalTimestamp(timestamp) => liEarliestLocalCnt += 1
        case ListOffsetsRequest.MAX_TIMESTAMP => maxCnt += 1
        case timestamp if timestamp >= 0 => byTimestampCnt += 1
        case _ => unknownCnt += 1
      }
    }

    def record(count: Int, meter: Option[Meter], histogram: Option[Histogram]): Unit = {
      if (count > 0) {
        meter.foreach(_.mark(count))
        histogram.foreach(_.update(count))
      }
    }

    record(earliestCnt, earliestTimestampMeter, earliestTimestampHist)
    record(latestCnt, latestTimestampMeter, latestTimestampHist)
    record(liEarliestLocalCnt, liEarliestLocalTimestampMeter, liEarliestLocalTimestampHist)
    record(maxCnt, maxTimestampMeter, maxTimestampHist)
    record(unknownCnt, unknownTimestampMeter, unknownTimestampHist)
    record(byTimestampCnt, byTimestampMeter, byTimestampHist)

    if (byTimestampCnt > 0) synchronized {
      // Keep the outer mutable map and its snapshot operation under the same lock. The inner maps
      // remain concurrent because the wrapper consumes a detached snapshot without holding this lock.
      val principalAssociatedTopicCounts = listOffsetsByTimestampApiClientUsers.getOrElseUpdate(
        principal.getName,
        new ConcurrentHashMap[String, AtomicInteger]().asScala)
      val associatedTopicCounter = principalAssociatedTopicCounts.getOrElseUpdate(
        topic.name,
        new AtomicInteger(0))
      associatedTopicCounter.incrementAndGet()
    }
  }
}
