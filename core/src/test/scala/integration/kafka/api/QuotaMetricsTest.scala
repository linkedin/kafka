package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.server.{ConfigEntityName, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util
import java.util.Collections.singleton
import java.util.Properties
import java.util.concurrent.{Future, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class QuotaMetricsTest extends IntegrationTestHarness {
  val producerClientId = "producer-100"
  val consumerClientId = "consumer-100"
  val inactiveSensorExpirationTimeSeconds = 2
  override protected def brokerCount: Int = 1
  this.serverConfig.setProperty(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp, "1000000")
  this.serverConfig.setProperty(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, "1000000")
  this.serverConfig.setProperty(KafkaConfig.InactiveSensorExpirationTimeSecondsProp, inactiveSensorExpirationTimeSeconds.toString)


  @BeforeEach
  override def setUp(): Unit = {
    Metrics.METRICS_SCHEDULER_INITIAL_DELAY = 0
    Metrics.METRICS_SCHEDULER_PERIOD = 1
    super.setUp()

    // apply the dynamic request_percentage quota override
    val adminClient = createAdminClient()
    val alterEntityMap = new util.HashMap[String, String]()
    alterEntityMap.put(ClientQuotaEntity.CLIENT_ID, ConfigEntityName.Default)
    val entity = new ClientQuotaEntity(alterEntityMap)
    val entries: util.List[ClientQuotaAlteration] = new util.ArrayList[ClientQuotaAlteration](1)
    entries.add(new ClientQuotaAlteration(entity, singleton(new ClientQuotaAlteration.Op(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, 100.0))))
    adminClient.alterClientQuotas(entries).all().get(60, TimeUnit.SECONDS)
  }

  // Verify that the throttle time metric shows up with a value of 0 when there is no quota violations
  @Test
  def testThrottleTime(): Unit = {
    val topic = "test"
    val props = new Properties
    createTopic(topic, numPartitions = 1, replicationFactor = 1, props)
    val tp = new TopicPartition(topic, 0)

    // Produce and consume some records
    val numRecords = 10
    val recordSize = 100000

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
    val producer = createProducer(configOverrides = producerProps)
    sendRecords(producer, numRecords, recordSize, tp)

    verifyQuotaMetrics("throttle-time", "Produce", producerClientId, true, value => value == 0)
    verifyQuotaMetrics("byte-rate", "Produce", producerClientId, true, value => value > 0)
    verifyQuotaMetrics("throttle-time", "Request", producerClientId, true, value => value == 0)
    verifyQuotaMetrics("request-time", "Request", producerClientId, true, value => value > 0)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
    val consumer = createConsumer(configOverrides = consumerProps)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    TestUtils.consumeRecords(consumer, numRecords)
    verifyQuotaMetrics("throttle-time", "Fetch", consumerClientId, true, value => value == 0)
    verifyQuotaMetrics("byte-rate", "Fetch", consumerClientId, true, value => value > 0)
    verifyQuotaMetrics("throttle-time", "Request", consumerClientId, true, value => value == 0)
    verifyQuotaMetrics("request-time", "Request", consumerClientId, true, value => value > 0)

    // Wait until the Produce and Fetch metrics are removed
    TestUtils.waitUntilTrue(() => {
      val produceMetrics = filterMetric("throttle-time", "Produce", producerClientId)
      val consumeMetrics = filterMetric("throttle-time", "Fetch", consumerClientId)
      produceMetrics.isEmpty && consumeMetrics.isEmpty
    }, "The Produce and Fetch throttle-time metrics should expire")
    // Verify that the Produce metrics are gone
    verifyQuotaMetrics("throttle-time", "Produce", producerClientId, false)
    verifyQuotaMetrics("byte-rate", "Produce", producerClientId, false)
    verifyQuotaMetrics("throttle-time", "Request", producerClientId, false)
    verifyQuotaMetrics("request-time", "Request", producerClientId, false)

    // Verify that the Fetch metrics are gone
    verifyQuotaMetrics("throttle-time", "Fetch", consumerClientId, false)
    verifyQuotaMetrics("byte-rate", "Fetch", consumerClientId, false)
    verifyQuotaMetrics("throttle-time", "Request", consumerClientId, false)
    verifyQuotaMetrics("request-time", "Request", consumerClientId, false)
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
    recordSize: Int, tp: TopicPartition) = {
    val bytes = new Array[Byte](recordSize)
    val sendFutures = mutable.Buffer[Future[RecordMetadata]]()
    (0 until numRecords).map { i =>
      sendFutures += producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, bytes))
    }
    sendFutures.foreach{_.get()}
  }

  private def filterMetric(metricNameFilter: String, metricGroupFilter: String,
    clientIdFilter: String) = {
    val allMetrics = servers(0).metrics.metrics().asScala

    allMetrics.filterKeys{name =>
      name.name().equals(metricNameFilter) && name.group().equals(metricGroupFilter) &&
        name.tags().containsKey("client-id") &&
        name.tags.get("client-id").equals(clientIdFilter)}
  }

  private def verifyQuotaMetrics(metricNameFilter: String, metricGroupFilter: String,
    clientIdFilter: String,
    shouldExist: Boolean,
    valuePredicate: Double => Boolean = v => true): Unit = {

    val metricsToCheck = filterMetric(metricNameFilter, metricGroupFilter, clientIdFilter)
    if (shouldExist) {
      assertEquals(1, metricsToCheck.size)
      assertTrue(metricsToCheck.forall{metric => valuePredicate(metric._2.metricValue.asInstanceOf[Double]) })
    } else {
      assertEquals(0, metricsToCheck.size)
    }
  }
}
