package kafka.utils

import java.util

import kafka.api._
import kafka.server.KafkaConfig
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.LiCombinedControlRequestUtils

object LiDecomposedControlRequestUtils {
  def decomposeRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): LiDecomposedControlRequest = {
    val leaderAndIsrRequest = extractLeaderAndIsrRequest(request, brokerEpoch, config)
    val updateMetadataRequest = extractUpdateMetadataRequest(request, config)
    new LiDecomposedControlRequest(leaderAndIsrRequest, updateMetadataRequest)
  }

  private def extractLeaderAndIsrRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): Option[LeaderAndIsrRequest] = {
    val partitionsInRequest = request.leaderAndIsrPartitionStates()
    val leadersInRequest = request.liveLeaders()

    val effectivePartitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    partitionsInRequest.forEach{partition =>
      if (partition.maxBrokerEpoch() >= brokerEpoch)
        effectivePartitionStates.add(LiCombinedControlRequestUtils.restoreLeaderAndIsrPartition(partition))
    }

    if (effectivePartitionStates.isEmpty) {
      None
    } else {
      val leaderNodes = new util.ArrayList[Node]()
      leadersInRequest.forEach{leader =>
        leaderNodes.add(new Node(leader.brokerId(), leader.hostName(), leader.port()))
      }

      val leaderAndIsrRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 5
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      Some(new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
        request.maxBrokerEpoch(), effectivePartitionStates, leaderNodes).build())
    }
  }

  private def extractUpdateMetadataRequest(request: LiCombinedControlRequest, config: KafkaConfig): Option[UpdateMetadataRequest] = {
    val partitionsInRequest = request.updateMetadataPartitionStates()
    val brokersInRequest = request.liveBrokers()

    val effectivePartitionStates = new util.ArrayList[UpdateMetadataPartitionState]()
    partitionsInRequest.forEach{partition =>
        effectivePartitionStates.add(LiCombinedControlRequestUtils.restoreUpdateMetadataPartition(partition))
    }

    val liveBrokers = new util.ArrayList[UpdateMetadataBroker]()
    brokersInRequest.forEach(broker => liveBrokers.add(LiCombinedControlRequestUtils.restoreUpdateMetadataBroker(broker)))

    if (effectivePartitionStates.isEmpty && liveBrokers.isEmpty) {
      None
    } else {
      val updateMetadataRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 7
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      Some(new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
        request.maxBrokerEpoch(), effectivePartitionStates, liveBrokers).build())
    }
  }
}
