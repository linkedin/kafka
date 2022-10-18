package integration.kafka.api

import kafka.controller.OfflinePartition
import kafka.server.{KafkaConfig, KafkaServer, ReplicaManager}
import kafka.utils.Implicits.PropertiesOps
import kafka.utils.{Exit, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}
import scala.collection.{Map, Seq}

class DropCorruptedFilesTest extends ZooKeeperTestHarness {
  @Test
  def testCorruptedLeaderEpochCheckpointOnLeader(): Unit = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
      .map { props => {
        props.setProperty(KafkaConfig.LiDropCorruptedFilesEnableProp, "true")
        props
      }
      }
      .map(KafkaConfig.fromProps)
    // start servers in reverse order to ensure broker 2 becomes the controller
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    assertTrue(controllerId == 2)

    // create the topic with min ISR of 2, which should allow one broker to shut down but should block subsequent
    // shutdowns.
    val topic = "test"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    val expectedReplicaAssignment = Map(0 -> List(0, 1))
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)


    val adminClient = createAdminClient(servers)
    val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
    val leader = topicDescMap.get(topic).partitions().get(0).leader().id()
    assertTrue(leader == 0)
    val follower = 1

    val orderedBrokers = Seq(leader, follower).map { brokerId =>
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

    // wait until all servers get the message
    TestUtils.waitUntilTrue(() => {
      orderedBrokers.forall { broker => broker.replicaManager.getLog(tp).get.logEndOffset == 1 }
    }, "some brokers cannot get the message")
    warn("LLWW0 0th message produced at offset " + recordMetadata0.offset())


    // shutdown the follower
    followerBroker.shutdown()
    // check that the partition has become offline
    // produce the 2nd message and make sure the leader gets it
    val recordMetadata1 = producer.send(record).get
    warn("LLWW0 1th message produced at offset " + recordMetadata1.offset())

    // shutdown the leader and startup the follower
    leaderBroker.shutdown()
    warn("LLWW0 leader shutdown complete")
    TestUtils.waitUntilTrue(() => {
      controller.controllerContext.partitionState(tp) == OfflinePartition
    }, s"the partition $tp does not become offline after all replicas are shutdown")
    warn("LLWW0 the partiton has become offline")


    followerBroker.startup()
    warn("LLWW0 follower startup complete")


    def ensureLeader(desiredLeader: Int): Unit = {
      TestUtils.waitUntilTrue(() => {
        val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
        val currentLeader = topicDescMap.get(topic).partitions().get(0).leader()
        warn(s"LLWW0 current leader ${currentLeader.id()}, required leader: $desiredLeader")
        desiredLeader.equals(currentLeader.id())
      }, "LLWW0 the leadership cannot be transferred to the follower")
    }

    ensureLeader(follower)
    // produce the record in epoch 1

    warn("LLWW0 producing the 2nd message")
    val recordMetadata2 = producer.send(record).get()
    warn("LLWW0 2nd message produced at offset " + recordMetadata2.offset())

    followerBroker.shutdown()
    // before the leader startup, corrupt its leader epoch cache file
    corruptLeaderEpochCheckpoint(leaderBroker.config.get(KafkaConfig.LogDirProp) + "/" + tp + "/leader-epoch-checkpoint")


    leaderBroker.startup()
    ensureLeader(leader)
    warn(s"LLWW0 the leadership has returned to original leader $leader")

    ReplicaManager.followerStartedCatchingup = true

    followerBroker.startup()

    // wait until the follower joins the ISR again
    TestUtils.waitUntilTrue(() => {
      val topicDescMap = adminClient.describeTopics(Collections.singleton(topic)).all().get()
      val currentISR = topicDescMap.get(topic).partitions().get(0).isr()
      currentISR.size() == 2
    }, "the follower cannot rejoin the ISR")
    warn("LLWW0 the ISR has converged to 2 again")

    adminClient.close()
    producer.close()



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

  private def corruptLeaderEpochCheckpoint(checkpointFile: String): Unit = {
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(checkpointFile)))
    // create a file with a corrupted version number
    bw.write("100")
    bw.newLine()
    bw.close()
  }


}
