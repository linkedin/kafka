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


package kafka.tools

import joptsimple.{OptionException, OptionParser, OptionSet}
import kafka.utils.Implicits.PropertiesOps
import kafka.utils.{CommandDefaultOptions, CommandLineUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils

import java.util.{Collections, Properties}
import scala.collection.JavaConverters.mapAsScalaMapConverter

object FetchOffsetTest {
  def main(args: Array[String]): Unit = {
    val conf = new FetchOffsetTestOptions(args)
    val consumer = new KafkaConsumer(consumerProps(conf), new ByteArrayDeserializer, new ByteArrayDeserializer)
    val partitionEndOffsets = consumer.endOffsets(Collections.singleton(new TopicPartition(conf.topicArg, conf.partitionArg)))
    for (entry <- partitionEndOffsets.asScala) {
      println("Got entry " + entry._1 + " -> " + entry._2)
    }
  }

  private[tools] def consumerProps(config: FetchOffsetTestOptions): Properties = {
    val props = new Properties
    props ++= config.consumerProps
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props
  }

  class FetchOffsetTestOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val topicOpt = parser.accepts("topic", "The topic to fetch offset from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val partitionIdOpt = parser.accepts("partition", "The partition to fetch offset from.")
      .withRequiredArg
      .describedAs("partition")
      .ofType(classOf[java.lang.Integer])

    val consumerConfigOpt = parser.accepts("consumer.config", s"Consumer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])

    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])

    options = tryParse(parser, args)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to fetch the latest offset on a given partition.")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, partitionIdOpt, bootstrapServerOpt)
    val topicArg = options.valueOf(topicOpt)

    val partitionArg = options.valueOf(partitionIdOpt).intValue

    val consumerProps = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties()
    val bootstrapServer = options.valueOf(bootstrapServerOpt)

    def tryParse(parser: OptionParser, args: Array[String]): OptionSet = {
      try
        parser.parse(args: _*)
      catch {
        case e: OptionException =>
          CommandLineUtils.printUsageAndDie(parser, e.getMessage)
      }
    }

  }
}
