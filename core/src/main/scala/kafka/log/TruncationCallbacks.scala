package kafka.log

import org.apache.kafka.common.TopicPartition

trait TruncationObserver {
  def onTruncated(topicPartition: TopicPartition,
                  op: String,              // observer
                  target: Long,            // requested target offset
                  fromLeo: Long,           // LEO before truncation
                  toLeo: Long,             // LEO after truncation
                  messagesTruncated: Long, // number of messages removed
                  bytesTruncated: Long     // number of bytes removed
                 ): Unit
}

// Thread-local hook to attach an observer during a truncation
object TruncationCallbacks {
  private val observerTL = new ThreadLocal[TruncationObserver]()

  def withObserver[T](observer: TruncationObserver)(f: => T): T = {
    observerTL.set(observer)
    try f
    finally observerTL.remove()
  }

  private[log] def notify(tp: TopicPartition,
                          op: String,
                          target: Long,
                          fromLeo: Long,
                          toLeo: Long,
                          msgs: Long,
                          bytes: Long): Unit = {
    val obs = observerTL.get()
    if (obs != null) {
      obs.onTruncated(tp, op, target, fromLeo, toLeo, msgs, bytes)
    }
  }
}
