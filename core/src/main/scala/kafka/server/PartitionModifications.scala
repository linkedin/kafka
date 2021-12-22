package kafka.server

import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

case class FollowerPartitionStateInFetcher(brokerIdAndFetcherId: BrokerIdAndFetcherId, offsetAndEpoch: OffsetAndEpoch) {

}

class PartitionModifications {
  val partitionsToRemove = mutable.Set[TopicPartition]()
  val partitionsToMakeFollowerWithOffsetAndEpoch = mutable.Map[TopicPartition, FollowerPartitionStateInFetcher]()
}

