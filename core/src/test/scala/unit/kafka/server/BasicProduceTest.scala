package unit.kafka.server

import kafka.api.IntegrationTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Test

import java.util.Properties

class BasicProduceTest extends IntegrationTestHarness{
  override protected def brokerCount: Int = 2

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]],
                          numRecords: Int,
                          topicPartition: TopicPartition): Unit = {
    val futures = (0 until numRecords).map( i => {
      val record = new ProducerRecord(topicPartition.topic, topicPartition.partition, s"$i".getBytes, s"$i".getBytes)
      debug(s"Sending this record: $record")
      producer.send(record)
    })

    futures.foreach(_.get)
  }

  @Test
  def basicProduceTest(): Unit = {
    val topic = "topic0"
    createTopic(topic, 1, 2)

    val props = new Properties()
    props.put(ProducerConfig.ACKS_CONFIG, "-1")
    val producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), props)
    sendRecords(producer, 1, new TopicPartition(topic, 0))
  }
}
