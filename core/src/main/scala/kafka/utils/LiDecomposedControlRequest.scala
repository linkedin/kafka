package kafka.utils

import org.apache.kafka.common.requests.{LeaderAndIsrRequest, UpdateMetadataRequest}

case class LiDecomposedControlRequest(leaderAndIsrRequest: Option[LeaderAndIsrRequest], updateMetadataRequest: Option[UpdateMetadataRequest])
