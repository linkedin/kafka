package unit.kafka.controller

import java.util

import scala.collection.JavaConverters._
import kafka.controller.ControllerRequestMerger
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LiCombinedControlRequestData
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, LiCombinedControlRequestUtils}
import org.junit.{Assert, Test}

class ControllerRequestMergerTest {
  private val controllerRequestMerger = new ControllerRequestMerger()

  @Test
  def testMergingLeaderAndIsrRequests() = {
    val leaderAndIsrRequestVersion : Short = 5
    val maxBrokerEpoch = 10
    val controllerId = 0
    val controllerEpoch = 0
    val brokerEpoch = -1
    val topic = "tp0"
    val partitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    val replicas = new util.ArrayList[Integer]() // TODO: add initial values 0,1,2 to the replicas
    val isr = replicas
    partitionStates.add(new LeaderAndIsrPartitionState()
    .setTopicName(topic)
    .setPartitionIndex(0)
    .setControllerEpoch(controllerEpoch)
    .setLeader(0)
    .setLeaderEpoch(0)
    .setIsr(isr)
    .setZkVersion(0)
    .setReplicas(replicas)
    .setIsNew(false)) // TODO: add the initial values
    val leaders = Set(0,1,2).map{id => new Node(id, "app-"+id+".linkedin.com", 9092)}
    val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId, controllerEpoch,
    brokerEpoch, maxBrokerEpoch, partitionStates, leaders.asJava)

    val transformedPartitionStates = new util.ArrayList[LiCombinedControlRequestData.LeaderAndIsrPartitionState]()
    partitionStates.forEach{partittionState =>
      transformedPartitionStates.add(LiCombinedControlRequestUtils.transformLeaderAndIsrPartition(partittionState, maxBrokerEpoch))
    }

    controllerRequestMerger.addRequest(leaderAndIsrRequest)

    controllerRequestMerger.pollLatestRequest() match {
      case Some(liCombinedControlRequest : LiCombinedControlRequest.Builder) => {
        Assert.assertEquals("Mismatched controller id", controllerId, liCombinedControlRequest.controllerId())
        Assert.assertEquals("Mismatched controller epoch", controllerEpoch, liCombinedControlRequest.controllerEpoch())
        Assert.assertEquals("Mismatched partition states", transformedPartitionStates, liCombinedControlRequest.leaderAndIsrPartitionStates())
      }
      case _ => Assert.fail("Unable to get LiCombinedControlRequest")
    }

  }
}
