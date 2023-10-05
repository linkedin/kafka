package kafka.server

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.security.auth.KafkaPrincipal

import java.util.concurrent.TimeUnit
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

/**
 * A short term solution for tracking
 * 1. what are the services/topics using by timestamp.
 * 2. what's the usage size of this
 */
class ListOffsetsRequestInstrumentation extends KafkaMetricsGroup {
  private val metricName = "ListOffsetsPartitionsRequested"
  private val eventType = "partitions"
  private val listBy = "listBy"

  private val unknownTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "UNKNOWN"))
  private val earliestTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "EARLIEST"))
  private val latestTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "LATEST"))
  private val liEarliestLocalTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "LI_EARLIEST_LOCAL"))
  private val maxTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "MAX"))
  private val byTimestamp: Meter = newMeter(metricName, eventType, TimeUnit.SECONDS, Map(listBy -> "BY_TIMESTAMP"))

  // The object would be periodically dumped to log and cleared on kafka-server wrapper
  var listOffsetsByTimestampApiClientUsers: mutable.Map[String, mutable.Set[String]] = _
  snapshotAndResetListOffsetByTimeStampApiUsers()

  /**
   * A helper method for the external wrapper to obtain the tracked requesters and refresh the tracking map
   */
  def snapshotAndResetListOffsetByTimeStampApiUsers(): mutable.Map[String, mutable.Set[String]] = {
    val old = listOffsetsByTimestampApiClientUsers
    listOffsetsByTimestampApiClientUsers = mutable.Map[String, mutable.Set[String]]()
    old
  }

  def logUsage(principal: KafkaPrincipal, topic: ListOffsetsTopic): Unit = {
    topic.partitions().asScala.foreach { partition =>
      partition.timestamp() match {
        // special types like EARLIEST are constants < 0
        case ListOffsetsRequest.EARLIEST_TIMESTAMP => earliestTimestamp.mark()
        case ListOffsetsRequest.LATEST_TIMESTAMP => latestTimestamp.mark()
        case ListOffsetsRequest.LI_EARLIEST_LOCAL_TIMESTAMP => liEarliestLocalTimestamp.mark()
        case ListOffsetsRequest.MAX_TIMESTAMP => maxTimestamp.mark()
        // Negative, not by actual timestamp, but also not yet defined constant type
        case t if t < 0 => unknownTimestamp.mark()
        // When > 0, it's specifying search by an actual timestamp
        case t if t >= 0 =>
          byTimestamp.mark()
          // For by timestamp, we also want to know who are the ones sending
          (listOffsetsByTimestampApiClientUsers.get(principal.getName) match {
            case Some(v) => v
            case None =>
              val newSet: mutable.Set[String] = mutable.Set()
              listOffsetsByTimestampApiClientUsers(principal.getName) = newSet
              newSet
          }).add(topic.name)
      }
    }
  }
}
