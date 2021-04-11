package kafka.utils

import org.apache.kafka.common.message.{LeaderAndIsrResponseData, UpdateMetadataResponseData}
import org.apache.kafka.common.requests.{LeaderAndIsrResponse, LiCombinedControlResponse, UpdateMetadataResponse}
import org.apache.kafka.common.utils.LiCombinedControlRequestUtils

object LiDecomposedControlResponseUtils {
  def decomposeResponse(response: LiCombinedControlResponse): LiDecomposedControlResponse = {
    val leaderAndIsrResponse = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
    .setErrorCode(response.leaderAndIsrErrorCode())
    .setPartitionErrors(LiCombinedControlRequestUtils.restoreLeaderAndIsrPartitionErrors(response.leaderAndIsrPartitionErrors())))

    val updateMetadataResponse = new UpdateMetadataResponse(new UpdateMetadataResponseData()
      .setErrorCode(response.updateMetadataErrorCode()))

    new LiDecomposedControlResponse(leaderAndIsrResponse, updateMetadataResponse)
  }
}
