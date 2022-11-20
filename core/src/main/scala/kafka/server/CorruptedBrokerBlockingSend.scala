package kafka.server

import io.netty.util.internal.shaded.org.jctools.queues.atomic.LinkedQueueAtomicNode

import java.util
import kafka.log.LogManager
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LiRegisterCorruptedBrokerRequestData
import org.apache.kafka.common.message.LiRegisterCorruptedBrokerRequestData.{CorruptedBrokerPartition, CorruptedBrokerTopic}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{LiRegisterCorruptedBrokerRequest, LiRegisterCorruptedBrokerResponse}

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

object CorruptedBrokerBlockingSend {
  def apply(
    controllerChannelManager: BrokerToControllerChannelManager,
    logManager: LogManager,
    brokerId: Int,
    brokerEpochSupplier: () => Long,
  ): CorruptedBrokerBlockingSend = new CorruptedBrokerBlockingSend(
    controllerChannelManager,
    logManager,
    brokerId,
    brokerEpochSupplier
  )
}

class CorruptedBrokerBlockingSend(
  val controllerChannelManager: BrokerToControllerChannelManager,
  val logManager: LogManager,
  val brokerId: Int,
  val brokerEpochSupplier: () => Long
) extends Logging {
  private val countDownLatch = new CountDownLatch(1)
  private val initialized = new AtomicBoolean(false)
  private var exception = new AtomicReference[Option[Exception]](None)

  private class CorruptedBrokerResponseHandler extends ControllerRequestCompletionHandler {
    override def onTimeout(): Unit = {
      exception.set(Some(new IllegalStateException(
        "Unexpected timeout when trying to send offsets for corrupted broker to controller")))
      countDownLatch.countDown()
    }

    override def onComplete(response: ClientResponse): Unit = {
      val message = response.responseBody().asInstanceOf[LiRegisterCorruptedBrokerResponse]
      handleResponse(message)
      countDownLatch.countDown()
    }
  }

  def handleResponse(response: LiRegisterCorruptedBrokerResponse): Unit = {
    val data = response.data
    exception.set(Errors.forCode(data.errorCode()) match {
      case Errors.NONE => None
      case e => Some(e.exception())
    })
  }

  def sendRequest(): Boolean = {
    if (!initialized.compareAndSet(false, true)) {
      return false
    }

    val partitionToLogEndOffsetMap: Map[TopicPartition, OffsetAndEpoch] = logManager.allLogs
      .flatMap(log => {
        log.latestEpoch.map(latestEpoch => log.topicPartition -> OffsetAndEpoch(log.logEndOffset, latestEpoch))
      }).toMap

    val request = buildRequest(partitionToLogEndOffsetMap)

    controllerChannelManager.sendRequest(new LiRegisterCorruptedBrokerRequest.Builder(request), this)
    countDownLatch.await()

    exception.get() match {
      case Some(exception) => throw exception
    }
  }

  private def buildRequest(partitionToLogEndOffsetMap: Map[TopicPartition, OffsetAndEpoch])
  : LiRegisterCorruptedBrokerRequestData = {
    val message = new LiRegisterCorruptedBrokerRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier())
      .setTopics(new util.ArrayList())

    val topicMap = partitionToLogEndOffsetMap.groupBy(_._1.topic())
    topicMap.foreach(entry => {
      val corruptedBrokerTopic = new CorruptedBrokerTopic()
        .setName(entry._1)
        .setPartitions(new util.ArrayList())
      message.topics().add(corruptedBrokerTopic)

      entry._2.foreach {
        case (partition, offsetAndEpoch) => corruptedBrokerTopic.partitions().add(
          new CorruptedBrokerPartition()
            .setPartitionIndex(partition.partition())
            .setLatestOffset(offsetAndEpoch.offset)
            .setLatestLeaderEpoch(offsetAndEpoch.leaderEpoch)
        )
      }
    })

    message
  }
}
