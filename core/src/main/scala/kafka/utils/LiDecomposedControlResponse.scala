package kafka.utils

import org.apache.kafka.common.requests.{LeaderAndIsrResponse, UpdateMetadataResponse}

case class LiDecomposedControlResponse(val leaderAndIsrResponse: LeaderAndIsrResponse, val updateMetadataResponse: UpdateMetadataResponse)
