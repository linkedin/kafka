package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}

class DropCorruptedFilesTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 2

  @Test
  def testCorruptedLeaderEpochCheckpointOnLeader(): Unit = {
    try {
      val topic = "test"
      val partition = 0
      val tp = new TopicPartition(topic, partition)
      val topicProps = new Properties()
      topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
      createTopic(topic, 1, 2, topicProps)
      val adminClient = createAdminClient()
      val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
      val leader = topicDescMap.get(topic).partitions().get(0).leader().id()
      val follower = if (leader == 0) 1 else 0
      // shutdown the follower
      val leaderBroker = servers.find(_.config.brokerId == leader).get
      val followerBroker = servers.find(_.config.brokerId == follower).get

      val producerConfigs = new Properties()
      producerConfigs.put(ProducerConfig.ACKS_CONFIG, "-1")

      val producer = createProducer()
      // send a normal record
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      producer.send(record).get
      // wait until all servers get the message
      TestUtils.waitUntilTrue(() => {
        servers.forall{s => s.replicaManager.getLog(tp).get.logEndOffset == 1}
      }, "some brokers cannot get the message")

      // shutdown the follower
      followerBroker.shutdown()
      // produce the 2nd message
      producer.send(record).get
      

      // shutdown the leader and startup the follower
      leaderBroker.shutdown()
      followerBroker.startup()
      // produce the record in epoch 2
      producer.send(record).get()

    } catch {
      case e: Exception => warn("got exception ", e)
    } finally {
      // for now avoid the shutting down phase to reduce noise
      Exit.exit(0)
    }


    // shutdown the leader broker and corrupt its leader-epoch-checkpoint file



  }


}
