package kafka.server

import com.typesafe.scalalogging.Logger
import kafka.utils.Logging
import org.apache.kafka.common.memory.MemoryPoolStatsStore

import scala.collection.JavaConverters._

object MemoryPoolStatsLogger {
  private val logger = Logger("memory.pool.stats.logger")
}

class MemoryPoolStatsLogger extends Logging {
  override lazy val logger = MemoryPoolStatsLogger.logger

  def logStats(memoryPoolStatsStore: MemoryPoolStatsStore): Unit = {
    val frequencyList = memoryPoolStatsStore.getFrequencies.asScala.toSeq.sortBy(_._1.startInclusive)
    frequencyList.foreach {
      case (range, frequency) =>
        info(s"[${range.startInclusive}-${range.endInclusive}] = $frequency")
    }
  }
}
