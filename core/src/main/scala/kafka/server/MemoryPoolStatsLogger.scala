/**
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
