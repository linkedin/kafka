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

package kafka.cluster

import java.io.File
import java.nio.file.Files
import java.util.Properties
import kafka.api.ApiVersion
import kafka.log.{Log, LogConfig, LogManager}
import kafka.server._
import kafka.utils.{LogCaptureAppender, MockScheduler, MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.apache.log4j.spi.LoggingEvent
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.{ArgumentMatchers, Mockito}

import scala.collection.Map

/**
 * Partition leader-only truncation logging tests.
 */
final class PartitionLeaderTruncationLoggingTest {

  private val time = new MockTime()
  private val localBrokerId = 1

  /**
   * LogManager mock that forwards truncate calls to our real Log.
   */
  private def newLogManagerFor(logs: Map[TopicPartition, Log]): LogManager = {
    val lm = Mockito.mock(classOf[LogManager])

    // Delegate truncateTo(Map[TopicPartition, Long], isFuture)
    Mockito
      .doAnswer { invocation =>
        val partOffsets = invocation.getArgument(0).asInstanceOf[Map[TopicPartition, Long]]
        partOffsets.foreach { case (tp, off) => logs(tp).truncateTo(off) }
        null
      }
      .when(lm)
      .truncateTo(ArgumentMatchers.any(), ArgumentMatchers.anyBoolean())

    // Delegate truncateFullyAndStartAt(tp, newOffset, isFuture)
    Mockito
      .doAnswer { invocation =>
        val tp = invocation.getArgument(0, classOf[TopicPartition])
        val newOffset = invocation.getArgument(1, classOf[java.lang.Long]).longValue()
        logs(tp).truncateFullyAndStartAt(newOffset)
        null
      }
      .when(lm)
      .truncateFullyAndStartAt(
        ArgumentMatchers.any(classOf[TopicPartition]),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyBoolean()
      )

    lm
  }

  /**
   * Create a real Log for testing.
   */
  private def newTestLog(tp: TopicPartition, baseDir: File, time: Time): Log = {
    val props = new Properties()
    val logConfig = LogConfig(props) // default config
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
      keepPartitionMetadataFile = false)
  }

  /**
   * Append single records as leader to advance LEO.
   */
  private def appendN(log: Log, n: Int): Unit = {
    (0 until n).foreach { i =>
      val rec = new SimpleRecord(time.milliseconds(), s"k-$i".getBytes(), s"v-$i".getBytes())
      val batch = MemoryRecords.withRecords(CompressionType.NONE, rec)
      log.appendAsLeader(batch, leaderEpoch = 0, origin = kafka.log.AppendOrigin.Client, interBrokerProtocolVersion = ApiVersion.latestVersion)
    }
  }

  /**
   * Create a partition.
   */
  private def newPartition(tp: TopicPartition, log: Log): Partition = {
    val producePurg: DelayedOperationPurgatory[DelayedProduce] = null
    val fetchPurg: DelayedOperationPurgatory[DelayedFetch] = null
    val deletePurg: DelayedOperationPurgatory[DelayedDeleteRecords] = null
    val delayedOps = new DelayedOperations(tp, producePurg, fetchPurg, deletePurg)

    val metadataCache = Mockito.mock(classOf[MetadataCache])
    val alterIsrManager = Mockito.mock(classOf[AlterIsrManager])
    val transferLeaderManager = Mockito.mock(classOf[TransferLeaderManager])
    val logMgr = newLogManagerFor(Map(tp -> log))

    // ISR change listener used by Partition (no-op for this test)
    val isrChangeListener = new IsrChangeListener {
      override def markExpand(): Unit = ()

      override def markShrink(): Unit = ()

      override def markFailed(): Unit = ()
    }

    val partition = new Partition(
      topicPartition = tp,
      replicaLagTimeMaxMs = 30000L,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = localBrokerId,
      time = time,
      isrChangeListener = isrChangeListener,
      delayedOperations = delayedOps,
      metadataCache = metadataCache,
      logManager = logMgr,
      alterIsrManager = alterIsrManager,
      transferLeaderManager = transferLeaderManager
    )

    // Attach our log and mark this broker as leader
    partition.setLog(log, isFutureLog = false)
    partition.leaderReplicaIdOpt = Some(localBrokerId)
    partition
  }

  // Capture appender lifecycle around each test
  private var appender: LogCaptureAppender = _

  @BeforeEach
  def setupAppender(): Unit = {
    appender = LogCaptureAppender.createAndRegister()
  }

  @AfterEach
  def teardownAppender(): Unit = {
    LogCaptureAppender.unregister(appender)
  }

  /**
   * Find the first [LeaderTruncation] line.
   */
  private def findLeaderTruncationLine(): Option[String] = {
    val it = appender.getMessages.iterator
    while (it.hasNext) {
      val ev = it.next().asInstanceOf[LoggingEvent]
      val msg = ev.getRenderedMessage
      if (msg != null && msg.contains("[LeaderTruncation]")) return Some(msg)
    }
    None
  }

  @Test
  def leader_truncateTo_logs_single_structured_line_with_bytes(): Unit = {
    val tp = new TopicPartition("lt-topic", 0)
    val baseDir = TestUtils.tempDir()
    val log = newTestLog(tp, baseDir, time)
    appendN(log, 5)

    val partition = newPartition(tp, log)
    assertTrue(partition.isLeader, "Partition should be leader in this scenario")

    partition.truncateTo(3L, isFuture = false)

    val line = findLeaderTruncationLine().getOrElse({
      val it = appender.getMessages.iterator
      val sb = new StringBuilder("Captured logs:\n")
      while (it.hasNext) {
        val ev = it.next().asInstanceOf[LoggingEvent]
        sb.append(String.valueOf(ev.getRenderedMessage)).append('\n')
      }
      fail("Expected [LeaderTruncation] line\n" + sb.toString)
    })

    // Verify content and consistency
    assertTrue(line.contains("op=truncateTo"))
    assertTrue(line.contains("tp=lt-topic-0"))
    assertTrue(line.contains(s"brokerId=$localBrokerId"))
    assertTrue(line.contains("bytes="))

    val fromLeo = """fromLEO=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val toLeo = """\sto=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val msgs = """\smsgs=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get

    assertEquals(fromLeo - toLeo, msgs, "msgs must equal fromLEO - toLEO")
  }

  @Test
  def future_log_truncation_is_not_logged(): Unit = {
    val tp = new TopicPartition("lt-topic-future", 0)
    val baseDir = TestUtils.tempDir()
    val log = newTestLog(tp, baseDir, time)
    appendN(log, 3)

    val partition = newPartition(tp, log)
    assertTrue(partition.isLeader)

    partition.truncateTo(2L, isFuture = true)

    assertTrue(findLeaderTruncationLine().isEmpty, "No [LeaderTruncation] should be logged for future log truncation")
  }

  @Test
  def follower_truncation_is_not_logged(): Unit = {
    val tp = new TopicPartition("lt-topic-follower", 0)
    val baseDir = TestUtils.tempDir()
    val log = newTestLog(tp, baseDir, time)
    appendN(log, 4)

    val partition = newPartition(tp, log)
    // Make this broker a follower
    partition.leaderReplicaIdOpt = Some(localBrokerId + 1)
    assertFalse(partition.isLeader)

    partition.truncateTo(2L, isFuture = false)

    assertTrue(findLeaderTruncationLine().isEmpty, "No [LeaderTruncation] should be logged when broker is a follower")
  }

  @Test
  def leader_truncateFullyAndStartAt_logs_single_structured_line_with_bytes(): Unit = {
    val tp = new TopicPartition("lt-topic-full", 0)
    val baseDir = TestUtils.tempDir()
    val log = newTestLog(tp, baseDir, time)
    appendN(log, 4)

    val partition = newPartition(tp, log)
    assertTrue(partition.isLeader)

    partition.truncateFullyAndStartAt(2L, isFuture = false)

    val line = findLeaderTruncationLine().getOrElse({
      val it = appender.getMessages.iterator
      val sb = new StringBuilder("Captured logs:\n")
      while (it.hasNext) {
        val ev = it.next().asInstanceOf[LoggingEvent]
        sb.append(String.valueOf(ev.getRenderedMessage)).append('\n')
      }
      fail("Expected [LeaderTruncation] line\n" + sb.toString)
    })

    assertTrue(line.contains("op=truncateFullyAndStartAt"))
    assertTrue(line.contains("bytes="))

    val fromLeo = """fromLEO=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val toLeo = """\sto=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get
    val msgs = """\smsgs=([0-9]+)""".r.findFirstMatchIn(line).map(_.group(1).toLong).get

    assertEquals(fromLeo - toLeo, msgs, "msgs must equal fromLEO - toLEO")
  }
}