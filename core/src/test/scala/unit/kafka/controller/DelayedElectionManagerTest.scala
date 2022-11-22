package kafka.controller

import io.netty.util.concurrent.ScheduledFuture
import kafka.server.KafkaConfig
import kafka.utils.{KafkaScheduler, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractResponse, ListOffsetsRequest}
import org.easymock.{EasyMock, IArgumentMatcher}
import org.junit.jupiter.api.{BeforeEach, Test}
import kafka.controller.DelayedElectionManagerTest.{DelayedElectionWaitMs, equalListOffsetsRequest}

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

object DelayedElectionManagerTest {
  private val DelayedElectionWaitMs = 1000

  private def equalListOffsetsRequest(listOffsetsRequestBuilder: ListOffsetsRequest.Builder): ListOffsetsRequest.Builder = {
    EasyMock.reportMatcher(new IArgumentMatcher {
      override def matches(other: Any): Boolean = {
        other match {
          case builder: ListOffsetsRequest.Builder =>
            val otherData = builder.build().data()
            val thisData = listOffsetsRequestBuilder.build().data()
            thisData.equals(otherData)
          case _ => false
        }
      }

      override def appendTo(stringBuffer: StringBuffer): Unit = {
        stringBuffer.append(s"listOffsetsRequest($listOffsetsRequestBuilder)")
      }
    })
    null
  }
}

class DelayedElectionManagerTest {
  private var controllerContext: ControllerContext = null
  private var delayedElectionManager: DelayedElectionManager = null
  private var eventManager: ControllerEventManager = null
  private var channelManager: ControllerChannelManager = null
  private var kafkaScheduler: KafkaScheduler = null

  private val controllerBrokerId = 5
  val extraProps = new Properties()
  extraProps.put(KafkaConfig.LiLeaderElectionOnCorruptionWaitMsProp, DelayedElectionWaitMs: java.lang.Long)

  private val config = KafkaConfig.fromProps(
    TestUtils.createBrokerConfig(controllerBrokerId, "zkConnect"),
    extraProps
  )
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)

  @BeforeEach
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(0, 1, 2, 3, 4)))

    eventManager = EasyMock.createMock(classOf[ControllerEventManager])
    channelManager = EasyMock.createMock(classOf[ControllerChannelManager])
    kafkaScheduler = EasyMock.createMock(classOf[KafkaScheduler])
    delayedElectionManager = new DelayedElectionManager(
      config, controllerContext, eventManager, channelManager, kafkaScheduler)
  }

  @Test
  def testNewCorruptedPartitionsStartElections(): Unit = {
    controllerContext.setCorruptedBrokers(Map(0 -> true, 1 -> true, 2 -> true))
    controllerContext.setLiveBrokers(Map(
      TestUtils.createBrokerAndEpoch(0, "host", 0),
      TestUtils.createBrokerAndEpoch(1, "host", 0)))

    def expectListOffsetsToBroker(brokerId: Int): Unit = {

      val expectedRequest = ListOffsetsRequest.Builder
        .forReplica(ApiKeys.LIST_OFFSETS.latestVersion(), controllerBrokerId)
        .setTargetTimes(Collections.singletonList(
          new ListOffsetsTopic()
            .setName(partition.topic())
            .setPartitions(
              Collections.singletonList(new ListOffsetsPartition()
                .setPartitionIndex(partition.partition())
                .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
            ))
        ))
      channelManager.sendRequest(
        EasyMock.eq(brokerId), equalListOffsetsRequest(expectedRequest),
        EasyMock.anyObject(classOf[AbstractResponse => Unit]))
      EasyMock.expectLastCall()
    }

    Seq(0, 1).foreach(expectListOffsetsToBroker)
    val scheduledFuture = EasyMock.createMock(classOf[ScheduledFuture[Any]])
    EasyMock.expect(kafkaScheduler.schedule(
      EasyMock.anyString(), EasyMock.anyObject(classOf[() => Unit]), EasyMock.anyLong()))
      .andReturn(scheduledFuture)

    EasyMock.replay(eventManager, channelManager, kafkaScheduler)
    delayedElectionManager.startDelayedElectionsForPartitions(partitions)
    EasyMock.verify(eventManager, channelManager, kafkaScheduler)
  }
}
