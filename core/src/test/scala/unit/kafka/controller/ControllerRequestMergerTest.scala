package unit.kafka.controller

import java.util

import scala.collection.JavaConverters._
import kafka.controller.ControllerRequestMerger
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LiCombinedControlRequestData
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, LiCombinedControlRequestUtils}
import org.junit.{Assert, Before, Test}

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

  @Before
  def setUp(): Unit = {
    replicas.add(0)
    replicas.add(1)
    replicas.add(2)
  }

  @Test
  def testMergingDifferentLeaderAndIsrPartitions() = {
    val partitionStates1 = getPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
    brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getPartitionStates(topic, 1)
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
  def testMultipleRequestsOnSameLeaderAndIsrPartition() = {
    val partitionStates1 = getPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getPartitionStates(topic, 0)
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
  def testSupercedingLeaderAndIsrPartitionStates() = {
    val partitionStates1 = getPartitionStates(topic, 0)
    val leaderAndIsrRequest1 = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
      brokerEpoch, maxBrokerEpoch, partitionStates1.asJava, leaders.asJava)

    val partitionStates2 = getPartitionStates(topic, 0, 1)
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

  def getPartitionStates(topic: String, partitionIndex: Int, leaderEpoch: Int = 0): List[LeaderAndIsrPartitionState] = {
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
}
