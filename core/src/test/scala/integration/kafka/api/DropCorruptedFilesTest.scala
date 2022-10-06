package integration.kafka.api

import kafka.api.IntegrationTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.Implicits.PropertiesOps
import kafka.utils.{Exit, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}
import scala.collection.{Map, Seq}

class DropCorruptedFilesTest extends ZooKeeperTestHarness {
  @Test
  def testCorruptedLeaderEpochCheckpointOnLeader(): Unit = {
    try {

      // create brokers
      val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
        .map(KafkaConfig.fromProps)
      // start servers in reverse order to ensure broker 2 becomes the controller
      val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
      val controllerId = TestUtils.waitUntilControllerElected(zkClient)
      assertTrue(controllerId == 2)

      // create the topic with min ISR of 2, which should allow one broker to shut down but should block subsequent
      // shutdowns.
      val topic = "test"
      val partition = 0
      val tp = new TopicPartition(topic, partition)
      val topicConfig = new Properties()
      topicConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
      val expectedReplicaAssignment = Map(0  -> List(0, 1))
      TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)



      val adminClient = createAdminClient(servers)
      val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
      val leader = topicDescMap.get(topic).partitions().get(0).leader().id()
      assertTrue(leader == 0)
      val follower = 1

      val orderedBrokers = Seq(leader, follower).map {brokerId =>
        servers.find(_.config.brokerId == brokerId).get
      }
      val leaderBroker = orderedBrokers(0)
      val followerBroker = orderedBrokers(1)

      val producerConfigs = new Properties()
      producerConfigs.put(ProducerConfig.ACKS_CONFIG, "-1")
      val producer = createProducer(servers)
      // send a normal record to epoch 0
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, "key".getBytes(StandardCharsets.UTF_8),
        "value".getBytes(StandardCharsets.UTF_8))
      val recordMetadata0 = producer.send(record).get
      warn("LLWW0 0th message produced at offset "+ recordMetadata0.offset())
      // wait until all servers get the message
      // wait until truncation of the message in epoch 1 on the follower
      TestUtils.waitUntilTrue(() => {
        followerBroker.replicaManager.getLog(tp).get.logEndOffset == 1
      }, "some brokers cannot get the message")

      // shutdown the follower
      followerBroker.shutdown()
      // check that the partition has become offline
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

      adminClient.close()
      producer.close()
    } catch {
      case e: Exception => warn("LLWW0 got exception ", e)
    } finally {
      // for now avoid the shutting down phase to reduce noise
      Exit.exit(0)
    }


    // shutdown the leader broker and corrupt its leader-epoch-checkpoint file
  }

  private def createAdminClient(servers: Seq[KafkaServer]): Admin = {
    val config = new Properties
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    AdminClient.create(config)
  }
  def createProducer[K, V](servers: Seq[KafkaServer],
    keySerializer: Serializer[K] = new ByteArraySerializer,
    valueSerializer: Serializer[V] = new ByteArraySerializer,
    configOverrides: Properties = new Properties): KafkaProducer[K, V] = {
    val props = new Properties
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props ++= configOverrides
    val producer = new KafkaProducer[K, V](props, keySerializer, valueSerializer)
    producer
  }

  def anotherFunc() : Unit = {
    /*
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
     */
  }


}
