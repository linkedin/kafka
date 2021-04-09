package unit.kafka.controller

import java.util

import kafka.controller.ControllerRequestMerger
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, LiCombinedControlRequestUtils, UpdateMetadataRequest}
import org.junit.{Assert, Before, Test}

import scala.collection.JavaConverters._

class ControllerRequestMergerTest {
  private val controllerRequestMerger = new ControllerRequestMerger()

  val leaderAndIsrRequestVersion : Short = 5
  val maxBrokerEpoch = 10
  val controllerId = 0
  val controllerEpoch = 0
  val brokerEpoch = -1
  val topic = "tp0"
  val replicas = new util.ArrayList[Integer]()
  val isr = replicas
  val leaders = Set(0,1,2).map{id => new Node(id, "app-"+id+".linkedin.com", 9092)}

  val updateMetadataRequestVersion: Short = 7
  val updateMetadataLiveBrokers = new util.ArrayList[UpdateMetadataBroker]()

  @Before
  def setUp(): Unit = {
    replicas.add(0)
    replicas.add(1)
    replicas.add(2)
  }

  @Test
  def testMergingDifferentLeaderAndIsrPartitions(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
    brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 1)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = (partitionStates1 ++ partitionStates2).map{partittionState =>
      LiCombinedControlRequestUtils.transformLeaderAndIsrPartition(partittionState, maxBrokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }
  }

  @Test
  def testMultipleRequestsOnSameLeaderAndIsrPartition(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = partitionStates1.map{partittionState =>
      LiCombinedControlRequestUtils.transformLeaderAndIsrPartition(partittionState, maxBrokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    // test that we can poll two separate tests containing the same partition state
    for (_ <- 0 until 2) {
      controllerRequestMerger.pollLatestRequest() match {
        case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
          Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
          Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
          Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())
        }
        case _ => Assert.fail("Unable to get LiCombinedControlRequest")
      }
    }
  }

  @Test
  def testSupercedingLeaderAndIsrPartitionStates(): Unit = {
    val partitionStates1 = getLeaderAndIsrPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getLeaderAndIsrPartitionStates(topic, 0, 1)
    val leaderAndIsrRequest2 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates2.asJava, leaders.asJava)

    val transformedPartitionStates = partitionStates2.map{partitionState =>
      LiCombinedControlRequestUtils.transformLeaderAndIsrPartition(partitionState, maxBrokerEpoch)
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest1)
    controllerRequestMerger.addRequest(leaderAndIsrRequest2)

    // test that we can poll a request with the larger leader epoch
    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.leaderAndIsrPartitionStates())
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }

    // test that trying to poll the request again will result in empty LeaderAndIsr partition states
    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertTrue("Mismatched partition states", liCombinedControlRequest.leaderAndIsrPartitionStates().isEmpty)
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }
  }

  def getLeaderAndIsrPartitionStates(topic: String, partitionIndex: Int, leaderEpoch: Int = 0): List[LeaderAndIsrPartitionState] = {
    //val partitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    List(new LeaderAndIsrPartitionState()
      .setTopicName(topic)
      .setPartitionIndex(partitionIndex)
      .setControllerEpoch(controllerEpoch)
      .setLeader(0)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(0)
      .setReplicas(replicas)
      .setIsNew(false))
  }

  @Test
  def testMergingDifferentUpdateMatadataPartitions(): Unit = {
    val partitionStates1 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest1 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch,
      partitionStates1.asJava, updateMetadataLiveBrokers)

    val partitionStates2 = getUpdateMetadataPartitionStates(topic, 1)
    val updateMetadataRequest2 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch,
      partitionStates2.asJava, updateMetadataLiveBrokers)

    val transformedPartitionStates = (partitionStates1 ++ partitionStates2).map{partitionState =>
      LiCombinedControlRequestUtils.transformUpdateMetadataPartition(partitionState)
    }

    controllerRequestMerger.addRequest(updateMetadataRequest1)
    controllerRequestMerger.addRequest(updateMetadataRequest2)


    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.updateMetadataPartitionStates())
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }
  }

  def testSupercedingUpdateMetadataPartitionStates(): Unit = {
    val partitionStates1 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest1 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch,
      partitionStates1.asJava, updateMetadataLiveBrokers)

    val partitionStates2 = getUpdateMetadataPartitionStates(topic, 0)
    val updateMetadataRequest2 = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, controllerId, controllerEpoch, brokerEpoch, maxBrokerEpoch,
      partitionStates2.asJava, updateMetadataLiveBrokers)

    val transformedPartitionStates = partitionStates2.map{partitionState =>
      LiCombinedControlRequestUtils.transformUpdateMetadataPartition(partitionState)
    }

    controllerRequestMerger.addRequest(updateMetadataRequest1)
    controllerRequestMerger.addRequest(updateMetadataRequest2)

    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertEquals("Mismatched partition states", transformedPartitionStates.asJava, liCombinedControlRequest.updateMetadataPartitionStates())
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }

    // test that trying to poll the request again will result in empty UpdateMetadata partition states
    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertTrue("Mismatched partition states", liCombinedControlRequest.updateMetadataPartitionStates().isEmpty)
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }
  }

  def getUpdateMetadataPartitionStates(topic: String, partitionIndex: Int): List[UpdateMetadataPartitionState] = {
    List(new UpdateMetadataPartitionState()
    .setTopicName(topic)
    .setPartitionIndex(partitionIndex)
    .setControllerEpoch(controllerEpoch)
    .setLeader(0)
    .setLeaderEpoch(0)
    .setIsr(isr)
    .setZkVersion(0)
    .setReplicas(replicas))
  }


}
