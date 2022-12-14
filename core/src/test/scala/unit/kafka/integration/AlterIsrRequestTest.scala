package unit.kafka.integration

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, MoveControllerOptions}
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Properties}

class AlterIsrRequestTest extends ZooKeeperTestHarness {
  @Test
  def testUnauthorizedAlterISRRequest(): Unit = {
    // create brokers
    val totalBrokers = 4
    val serverConfigs = TestUtils.createBrokerConfigs(totalBrokers, zkConnect, false)
      .map(props => {
        if (props.get(KafkaConfig.BrokerIdProp).equals((totalBrokers - 1).toString)) {
          props.setProperty(KafkaConfig.LiDenyAlterIsrProp, "true")
        }
        if (props.get(KafkaConfig.BrokerIdProp).equals("0")) {
          // let the leader drop Fetch requests from the followers, which will cause the partition to shrink its ISR
          props.setProperty(KafkaConfig.LiDropFetchFollowerEnableProp, "true")
        }
        props.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "10000")
        props
      })
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure the last broker becomes the controller
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val firstControllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertTrue(firstControllerId == totalBrokers - 1)
    info(s"First elected controller is $firstControllerId")

    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    // ensure the ISR has a size of 3
    val adminClient = TestUtils.createAdminClient(servers)
    val initialISR = getISR(adminClient, tp)
    assertEquals(3, initialISR.size())
    info(s"The initial ISR size is $initialISR")

    /**
     * Produce 1 message to trigger a mismatch of log end offset between the leader and followers.
     * This should further trigger an AlterISRRequest from the leader to the controller
     */
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers), acks=1)
    produceRecord(producer, topic, partition)

    adminClient.moveController(new MoveControllerOptions())
    TestUtils.waitUntilTrue(() => {
      val secondController = TestUtils.waitUntilControllerElected(zkClient)
      info(s"Elected new controller $secondController")
      secondController != firstControllerId
    }, "unable to elect a different controller")


    // Ensure that the AlterISR request can go through with the new controller
    TestUtils.waitUntilTrue(() => {
      val currentISR = getISR(adminClient, tp)
      info(s"current isr $currentISR")
      currentISR.size() == 1
    }, "Unable to update the ISR despite a new controller", pause = 2000)


    info("Test has finished, shutting down the clients and servers")
    producer.close()
    adminClient.close()
    servers.foreach(_.shutdown())
  }

  private def getISR(adminClient: Admin, tp: TopicPartition): util.List[Node] = {
    val topicDescMap = adminClient.describeTopics(Collections.singleton(tp.topic())).all().get()
    topicDescMap.get(tp.topic()).partitions().get(tp.partition()).isr()
  }

  private def produceRecord(producer: Producer[Array[Byte], Array[Byte]], topic: String, partition: Int) = {
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
      "value".getBytes(StandardCharsets.UTF_8))
    producer.send(record).get()
  }

}
