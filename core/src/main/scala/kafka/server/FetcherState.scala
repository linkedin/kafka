package kafka.server

import kafka.controller.ControllerState

sealed abstract class FetcherState {
  def value: Byte

  def rateAndTimeMetricName: Option[String] =
    if (hasRateAndTimeMetric) Some(s"${toString}RateAndTimeMs") else None

  protected def hasRateAndTimeMetric: Boolean = true
}

object FetcherState {
  // Note: `rateAndTimeMetricName` is based on the case object name by default. Changing a name is a breaking change
  // unless `rateAndTimeMetricName` is overridden.

  case object Idle extends FetcherState {
    def value = 0
    override protected def hasRateAndTimeMetric: Boolean = false
  }

  case object AddPartitions extends FetcherState {
    def value = 1
  }

  case object RemovePartitions extends FetcherState {
    def value = 2
  }

  case object GetPartitionCount extends FetcherState {
    def value = 3
  }

  case object TruncateAndFetch extends FetcherState {
    def value = 4
  }

  val values: Seq[FetcherState] = Seq(Idle, AddPartitions, RemovePartitions, GetPartitionCount, TruncateAndFetch)
}
