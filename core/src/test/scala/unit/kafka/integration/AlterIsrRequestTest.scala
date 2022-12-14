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
          // let the leader drop Fetch requests from the followers, which will cause the partition to be UnderMinISR
          props.setProperty(KafkaConfig.LiDenyAlterIsrProp, "true")
        }
        props
      })
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure the last broker becomes the controller
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val firstControllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertTrue(firstControllerId == totalBrokers - 1)

    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    // ensure the ISR has a size of 3
    val adminClient = TestUtils.createAdminClient(servers)
    assertEquals(3, getISR(adminClient, tp).size())

    // shutdown 1 follower
    val follower = servers.find(_.config.brokerId == 2).get
    follower.shutdown()

    /**
     * Produce 1 message to trigger a mismatch of log end offset between the leader and followers.
     * This should further trigger an AlterISRRequest from the leader to the controller
     */
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers))
    produceRecord(producer, topic, partition)

    Thread.sleep(60000)
    /*
    TestUtils.waitUntilTrue(() => {
      adminClient.moveController(new MoveControllerOptions())
      val secondController = TestUtils.waitUntilControllerElected(zkClient)
      secondController != firstControllerId
    }, "unable to elect a different controller")

    info(s"Elected new controller ${zkClient.getControllerId.get}")


    // Ensure that the AlterISR request can go through with the new controller
    TestUtils.waitUntilTrue(() => {
      getISR(adminClient, tp).size() == 2
    }, "Unable to update the ISR despite a new controller")
     */
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
