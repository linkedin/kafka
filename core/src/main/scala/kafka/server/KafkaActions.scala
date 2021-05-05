package kafka.server

import org.apache.kafka.common.requests.ControlledShutdownResponse

trait KafkaActions {
  /** Notify the controlled shutdown status.
   * @param safeToShutdown Whether this broker has determined that it's safe to shutdown.
   * @param prevControlledShutdownResponse The ControlledShutdownResponse that prevented a successful shutdown in this iteration
   * @param remainingRetries The remaining retries (possibly zero)
   * */
  def notifyControlledShutdownStatus(safeToShutdown: Boolean, prevControlledShutdownResponse: ControlledShutdownResponse, remainingRetries: Long): Unit
}
