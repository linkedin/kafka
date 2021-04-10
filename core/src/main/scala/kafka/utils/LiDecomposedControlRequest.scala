package kafka.utils

import org.apache.kafka.common.requests.{LeaderAndIsrRequest, UpdateMetadataRequest}

case class LiDecomposedControlRequest(val leaderAndIsrRequest: LeaderAndIsrRequest, val updateMetadataRequest: UpdateMetadataRequest)
