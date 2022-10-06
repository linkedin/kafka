package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.utils.{Exit, TestUtils}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.junit.jupiter.api.Assertions.assertTrue
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

      warn(s"LLWW0 leader: $leader")
      warn(s"LLWW0 follower: $follower")
      val orderedBrokers = Seq(leader, follower).map {brokerId =>
        servers.find(_.config.brokerId == brokerId).get
      }

      val orderedLogs = orderedBrokers.map{broker => broker.logManager.getLog(tp).get}
      val leaderLog = orderedLogs(0)
      val followerLog = orderedLogs(1)

      // shutdown the servers before appending messages
      servers.foreach(_.shutdown())
      warn("LLWW0 all servers have been shutdown")

      // the offset of the records gets changed, so we cannot reuse the same records object on mulitple appends
      def getRecords(leaderEpoch: Int) = {
        MemoryRecords.withRecords(CompressionType.NONE, leaderEpoch,
          new SimpleRecord("hello".getBytes))
      }

      leaderLog.appendAsLeader(getRecords(0), leaderEpoch = 0)
      leaderLog.appendAsLeader(getRecords(0), leaderEpoch = 0)

      followerLog.appendAsFollower(getRecords(0))

      followerLog.appendAsLeader(getRecords(1), leaderEpoch = 1)

      assertTrue(orderedBrokers.forall{broker =>
        broker.replicaManager.getLog(tp).get.logEndOffset == 2
      })
      warn("LLWW0 all brokers have 2 messages now")

      // start the leader host first so that it's still the leader
      val leaderBroker = orderedBrokers(0)
      val followerBroker = orderedBrokers(1)
      leaderBroker.startup()
      warn("LLWW0 leader broker has been restarted")
      followerBroker.startup()
      warn("LLWW0 follower broker has been restarted")

      // wait until truncation of the message in epoch 1 on the follower
      TestUtils.waitUntilTrue(() => {
        followerBroker.replicaManager.getLog(tp).get.logEndOffset == 1
      }, "some brokers cannot get the message")
      warn("LLWW0 message in epoch 1 on the followir has been truncated")
      adminClient.close()
      /*
      val producerConfigs = new Properties()
      producerConfigs.put(ProducerConfig.ACKS_CONFIG, "-1")
      val producer = createProducer()
      // send a normal record to epoch 0
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      val recordMetadata0 = producer.send(record).get
      warn("LLWW0 0th message produced at offset "+ recordMetadata0.offset())
      // wait until all servers get the message


      // shutdown the follower
      followerBroker.shutdown()
      // produce the 2nd message and make sure the leader gets it
      val recordMetadata1 = producer.send(record).get
      warn("LLWW0 1th message produced at offset "+ recordMetadata1.offset())

      // shutdown the leader and startup the follower
      leaderBroker.shutdown()
      warn("LLWW0 leader shutdown complete")
      followerBroker.startup()
      warn("LLWW0 follower startup complete")
      TestUtils.waitUntilTrue(() => {
        val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
        val currentLeader = topicDescMap.get(topic).partitions().get(0).leader()
        currentLeader == follower
      }, "LLWW0 the leadership cannot be transferred to the follower")
      // produce the record in epoch 1
      val recordMetadata2 = producer.send(record).get()
      warn("LLWW0 2nd message produced at offset "+ recordMetadata2.offset())


      producer.close()
       */
    } catch {
      case e: Exception => warn("LLWW0 got exception ", e)
    } finally {
      // for now avoid the shutting down phase to reduce noise
      Exit.exit(0)
    }


    // shutdown the leader broker and corrupt its leader-epoch-checkpoint file



  }


}
