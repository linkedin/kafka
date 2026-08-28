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
 * A short term solution for tracking
 * 1. what are the services/topics using by timestamp.
 * 2. what's the usage size of this
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


  // The object would be periodically dumped to log and cleared on kafka-server wrapper
  var listOffsetsByTimestampApiClientUsers: mutable.Map[String, concurrent.Map[String, AtomicInteger]] = _
  snapshotAndResetListOffsetByTimeStampApiUsers()

  /**
   * A helper method for the external wrapper to obtain the tracked requesters and refresh the tracking map
   */
  def snapshotAndResetListOffsetByTimeStampApiUsers(): mutable.Map[String, concurrent.Map[String, AtomicInteger]] = {
    val old = listOffsetsByTimestampApiClientUsers
    listOffsetsByTimestampApiClientUsers = mutable.Map()
    old
  }

  def logUsage(principal: KafkaPrincipal, topic: ListOffsetsTopic): Unit = {
    if (!enabled) return

    var earliestCnt = 0;
    var latestCnt = 0;
    var liEarliestLocalCnt = 0;
    var maxCnt = 0;
    var unknownCnt = 0;
    var byTimestampCnt = 0;
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

    // Mark partitions requested per second for each type
    earliestTimestampMeter.foreach(_.mark(earliestCnt))
    latestTimestampMeter.foreach(_.mark(latestCnt))
    liEarliestLocalTimestampMeter.foreach(_.mark(liEarliestLocalCnt))
    maxTimestampMeter.foreach(_.mark(maxCnt))
    unknownTimestampMeter.foreach(_.mark(unknownCnt))
    byTimestampMeter.foreach(_.mark(byTimestampCnt))

    // Also mark the distribution of each type
    earliestTimestampHist.foreach(_.update(earliestCnt))
    latestTimestampHist.foreach(_.update(latestCnt))
    liEarliestLocalTimestampHist.foreach(_.update(liEarliestLocalCnt))
    maxTimestampHist.foreach(_.update(maxCnt))
    unknownTimestampHist.foreach(_.update(unknownCnt))
    byTimestampHist.foreach(_.update(byTimestampCnt))

    if (byTimestampCnt > 0) {
      // For by timestamp, we also want to know who are the ones sending
      val principalAssociatedTopicCounts = listOffsetsByTimestampApiClientUsers.get(principal.getName) match {
        case Some(v) => v
        case None =>
          val newMap: concurrent.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]().asScala
          listOffsetsByTimestampApiClientUsers(principal.getName) = newMap
          newMap
      }

      val associatedTopicCounter = principalAssociatedTopicCounts.get(topic.name) match {
        case Some(v) => v
        case None =>
          val newCounter = new AtomicInteger(0)
          principalAssociatedTopicCounts(topic.name) = newCounter
          newCounter
      }

      associatedTopicCounter.incrementAndGet()
    }
  }
}
