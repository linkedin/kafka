package kafka.api

import kafka.server.BrokerTopicStats
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

class MultiBrokerMetricsTest extends MetricsTest {
  override val brokerCount = 4

  @Test
  def testProduceRequestsWithInvalidAcks(): Unit = {
    val topic = "Topic1"
    val props = new Properties
    props.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    createTopic(topic, numPartitions = 1, replicationFactor = 4, props)
    val tp = new TopicPartition(topic, 0)

    val numRecords = 10
    val recordSize = 100000
    val producerConfigs = new Properties
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1")
    val producer = createProducer(configOverrides = producerConfigs)
    sendRecords(producer, numRecords, recordSize, tp)

    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec},topic=$topic")
    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=${BrokerTopicStats.ProduceRequestsWithInvalidAcksPerSec}")
  }
}
