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

package kafka.log

import java.io.File
import java.nio.file.Files
import java.util.Properties

import kafka.api.ApiVersion
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Log-level truncation callbacks.
 */
final class LogTruncationCallbacksTest {

  /**
   * Create a Log.
   */
  private def newTestLog(tp: TopicPartition, baseDir: File, time: Time): Log = {
    val props = new Properties()
    val logConfig = LogConfig(props) // defaults
    val scheduler = new MockScheduler(time)
    val brokerTopicStats = new BrokerTopicStats
    val logDirFailureChannel = new LogDirFailureChannel(10)

    val partDir = new File(baseDir, Log.logDirName(tp))
    Files.createDirectories(partDir.toPath)

    Log.apply(
      dir = partDir,
      config = logConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 24 * 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = 60 * 1000,
      logDirFailureChannel = logDirFailureChannel,
      topicId = None,
      keepPartitionMetadataFile = false
    )
  }

  /**
   * Append single records as leader to increment LEO.
   */
  private def appendN(log: Log, n: Int, time: Time): Unit = {
    (0 until n).foreach { i =>
      val rec = new SimpleRecord(time.milliseconds(), s"key-$i".getBytes(), s"value-$i".getBytes())
      val batch = MemoryRecords.withRecords(CompressionType.NONE, rec)
      log.appendAsLeader(batch, leaderEpoch = 0, origin = AppendOrigin.Client, interBrokerProtocolVersion = ApiVersion.latestVersion)
    }
  }

  case class CallbackEvent(tp: TopicPartition, op: String, target: Long, fromLeo: Long, toLeo: Long, msgs: Long, bytes: Long)

  @Test
  def truncateTo_emits_callback_with_bytes_and_counts(): Unit = {
    val time = new MockTime()
    val baseDir = TestUtils.tempDir()
    val tp = new TopicPartition("cb-topic", 0)
    val log = newTestLog(tp, baseDir, time)

    appendN(log, n = 5, time) // LEO should be 5

    var event: Option[CallbackEvent] = None

    TruncationCallbacks.withObserver(new TruncationObserver {
      override def onTruncated(topicPartition: TopicPartition,
                               op: String,
                               target: Long,
                               fromLeo: Long,
                               toLeo: Long,
                               messagesTruncated: Long,
                               bytesTruncated: Long): Unit = {
        event = Some(CallbackEvent(topicPartition, op, target, fromLeo, toLeo, messagesTruncated, bytesTruncated))
      }
    }) {
      // Truncate to offset 3: remove tail and align LEO <= 3
      log.truncateTo(3L)
    }

    val e = event.getOrElse(fail("Expected truncation callback to fire"))

    assertEquals(tp, e.tp)
    assertEquals("truncateTo", e.op)
    assertEquals(3L, e.target)
    assertEquals(5L, e.fromLeo) // original LEO
    assertTrue(e.toLeo <= 3L, "toLEO should be <= target after truncation")
    assertTrue(e.msgs > 0, "messagesTruncated should be positive")
    assertTrue(e.bytes > 0, "bytesTruncated should be positive")
    assertEquals(e.fromLeo - e.toLeo, e.msgs, "msgs must equal fromLEO - toLEO")
  }

  @Test
  def truncateFullyAndStartAt_emits_callback_with_bytes_and_counts(): Unit = {
    val time = new MockTime()
    val baseDir = TestUtils.tempDir()
    val tp = new TopicPartition("cb-topic-full", 0)
    val log = newTestLog(tp, baseDir, time)

    appendN(log, n = 4, time) // LEO should be 4

    var event: Option[CallbackEvent] = None

    TruncationCallbacks.withObserver(new TruncationObserver {
      override def onTruncated(topicPartition: TopicPartition,
                               op: String,
                               target: Long,
                               fromLeo: Long,
                               toLeo: Long,
                               messagesTruncated: Long,
                               bytesTruncated: Long): Unit = {
        event = Some(CallbackEvent(topicPartition, op, target, fromLeo, toLeo, messagesTruncated, bytesTruncated))
      }
    }) {
      // Start the log at offset 2: remove tail and rebuild metadata
      log.truncateFullyAndStartAt(2L)
    }

    val e = event.getOrElse(fail("Expected truncation callback to fire"))
    assertEquals(tp, e.tp)
    assertEquals("truncateFullyAndStartAt", e.op)
    assertEquals(2L, e.target)
    assertEquals(4L, e.fromLeo) // original LEO
    assertEquals(2L, e.toLeo) // post truncation LEO
    assertEquals(2L, e.msgs) // delta 4 -> 2
    assertTrue(e.bytes > 0, "bytesTruncated should be positive for full truncation")
  }
}