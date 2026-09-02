/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.nio.ByteBuffer
import java.util.concurrent._
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.{Counter, Histogram, Meter}
import kafka.network
import kafka.server.{BrokerMetadataStats, KafkaConfig, NoOpObserver, Observer, RequestLocal}
import kafka.utils.{Logging, Pool}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.EnvelopeResponseData
import org.apache.kafka.common.network.{ClientInformation, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.network.Session
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  private val RequestQueueSizeMetric = "RequestQueueSize"
  private val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"

  /**
    * Deprecated protocol apis are logged at info level while the rest are logged at debug level.
    * That makes it possible to enable the former without enabling latter.
    */
  private def isRequestLoggingEnabled(header: RequestHeader): Boolean = requestLogger.underlying.isDebugEnabled ||
    (requestLogger.underlying.isInfoEnabled && header.isApiVersionDeprecated())

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest
  case object WakeupRequest extends BaseRequest

  class Metrics(enabledApis: Iterable[ApiKeys], config: Option[KafkaConfig] = None) {
    def this(scope: ListenerType) = {
      this(ApiKeys.apisForListener(scope).asScala, None)
    }

    private val metricsMap = mutable.Map[String, RequestMetrics]()
    private val bucketConfig = config.filter(_.liProtocolBridgeRequestMetricBucketsActive)
    val legacyRequestMetricsEnabled: Boolean = config.exists(_.liProtocolBridgeLegacyRequestMetricsActive)

    val produceRequestAcksSizeMetricNameMap: Map[Int, util.TreeMap[Int, String]] =
      bucketConfig.map(_ => createProduceSizeMetricNames()).getOrElse(Map.empty)
    val consumerFetchRequestSizeMetricNameMap: util.TreeMap[Int, String] =
      bucketConfig.map(_ => createSizeMetricNames(RequestMetrics.consumerFetchMetricName))
        .getOrElse(new util.TreeMap[Int, String])

    private val additionalMetricNames =
      produceRequestAcksSizeMetricNameMap.values.flatMap(_.values.asScala) ++
        consumerFetchRequestSizeMetricNameMap.values.asScala
    (enabledApis.map(_.name) ++
      Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName,
        RequestMetrics.verifyPartitionsInTxnMetricName) ++ additionalMetricNames).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name, bucketConfig, legacyRequestMetricsEnabled))
    }

    private def createSizeMetricNames(requestType: String): util.TreeMap[Int, String] =
      RequestMetrics.getBucketBoundaryObjectMap(bucketConfig.get.requestMetricsSizeBuckets,
        requestType, "Mb", identity, addBinNumber = false)

    private def createProduceSizeMetricNames(): Map[Int, util.TreeMap[Int, String]] = {
      val baseNames = createSizeMetricNames(ApiKeys.PRODUCE.name)
      Seq(0 -> "0", 1 -> "1", -1 -> "All").map { case (acks, suffix) =>
        val names = new util.TreeMap[Int, String]
        baseNames.asScala.foreach { case (boundary, name) => names.put(boundary, s"${name}Acks$suffix") }
        acks -> names
      }.toMap
    }

    def requestSizeBucketMetricName(names: util.TreeMap[Int, String], sizeBytes: Long): Option[String] =
      if (names.isEmpty) None
      else Some(RequestMetrics.getBucketObject(names, (sizeBytes / 1024 / 1024).toInt))

    def apply(metricName: String): RequestMetrics = metricsMap.synchronized {
      metricsMap.getOrElseUpdate(metricName,
        new RequestMetrics(metricName, bucketConfig, legacyRequestMetricsEnabled))
    }

    def close(): Unit = metricsMap.synchronized {
      metricsMap.values.foreach(_.removeMetrics())
      metricsMap.clear()
    }
  }

  case class CallbackRequest(fun: RequestLocal => Unit,
                             originalRequest: Request) extends BaseRequest

  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                val memoryPool: MemoryPool,
                @volatile var buffer: ByteBuffer,
                metrics: RequestChannel.Metrics,
                val envelope: Option[RequestChannel.Request] = None) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    @volatile var requestDequeueTimeNanos: Long = -1L
    @volatile var apiLocalCompleteTimeNanos: Long = -1L
    @volatile var responseCompleteTimeNanos: Long = -1L
    @volatile var responseDequeueTimeNanos: Long = -1L
    @volatile var messageConversionsTimeNanos: Long = 0L
    @volatile var apiThrottleTimeMs: Long = 0L
    @volatile var temporaryMemoryBytes: Long = 0L
    @volatile var responseBytes: Long = 0L
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None
    @volatile var callbackRequestDequeueTimeNanos: Option[Long] = None
    @volatile var callbackRequestCompleteTimeNanos: Option[Long] = None

    val session: Session = new Session(context.principal, context.clientAddress)

    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    // This is constructed on creation of a Request so that the JSON representation is computed before the request is
    // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
    val requestLog: Option[JsonNode] =
      if (RequestChannel.isRequestLoggingEnabled(context.header)) Some(RequestConvertToJson.request(loggableRequest))
      else None

    def header: RequestHeader = context.header

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def sizeInBytes: Int = header.size + sizeOfBodyInBytes

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def isForwarded: Boolean = envelope.isDefined

    private def shouldReturnNotController(response: AbstractResponse): Boolean = {
      response match {
        case _: DescribeQuorumResponse => response.errorCounts.containsKey(Errors.NOT_LEADER_OR_FOLLOWER)
        case _ => response.errorCounts.containsKey(Errors.NOT_CONTROLLER)
      }
    }

    def buildResponseSend(abstractResponse: AbstractResponse): Send = {
      envelope match {
        case Some(request) =>
          val envelopeResponse = if (shouldReturnNotController(abstractResponse)) {
            // Since it's a NOT_CONTROLLER error response, we need to make envelope response with NOT_CONTROLLER error
            // to notify the requester (i.e. NodeToControllerRequestThread) to update active controller
            new EnvelopeResponse(new EnvelopeResponseData()
              .setErrorCode(Errors.NOT_CONTROLLER.code()))
          } else {
            val responseBytes = context.buildResponseEnvelopePayload(abstractResponse)
            new EnvelopeResponse(responseBytes, Errors.NONE)
          }
          request.context.buildResponseSend(envelopeResponse)
        case None =>
          context.buildResponseSend(abstractResponse)
      }
    }

    def responseNode(response: AbstractResponse): Option[JsonNode] = {
      if (RequestChannel.isRequestLoggingEnabled(context.header))
        Some(RequestConvertToJson.response(response, context.apiVersion))
      else
        None
    }

    def headerForLoggingOrThrottling(): RequestHeader = {
      envelope match {
        case Some(request) =>
          request.context.header
        case None =>
          context.header
      }
    }

    def requestDesc(details: Boolean): String = {
      val forwardDescription = envelope.map { request =>
        s"Forwarded request: ${request.context} "
      }.getOrElse("")
      s"$forwardDescription$header -- ${loggableRequest.toString(details)}"
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def loggableRequest: AbstractRequest = {

      bodyAndSize.request match {
        case alterConfigs: AlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new AlterConfigsRequest(newData, alterConfigs.version())

        case alterConfigs: IncrementalAlterConfigsRequest =>
          val newData = alterConfigs.data().duplicate()
          newData.resources().forEach(resource => {
            val resourceType = ConfigResource.Type.forId(resource.resourceType())
            resource.configs().forEach(config => {
              config.setValue(KafkaConfig.loggableValue(resourceType, config.name(), config.value()))
            })
          })
          new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version())

        case _ =>
          bodyAndSize.request
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos: Long = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }

    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response): Unit = {
      val endTimeNanos = Time.SYSTEM.nanoseconds

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val callbackRequestTimeNanos = callbackRequestCompleteTimeNanos.getOrElse(0L) - callbackRequestDequeueTimeNanos.getOrElse(0L)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos + callbackRequestTimeNanos)
      val apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos - callbackRequestTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val overrideMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val fetchRequest = body[FetchRequest]
          val specifiedMetricName =
            if (fetchRequest.isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          val sizeMetricName =
            if (fetchRequest.isFromFollower || fetchRequest.maxWait > ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS) None
            else metrics.requestSizeBucketMetricName(metrics.consumerFetchRequestSizeMetricNameMap, responseBytes)
          Seq(specifiedMetricName, header.apiKey.name) ++ sizeMetricName
        } else if (header.apiKey == ApiKeys.PRODUCE) {
          val acks = body[ProduceRequest].acks.toInt
          val names = metrics.produceRequestAcksSizeMetricNameMap.get(acks)
            .orElse(metrics.produceRequestAcksSizeMetricNameMap.get(-1))
          Seq(header.apiKey.name) ++ names.flatMap(metrics.requestSizeBucketMetricName(_, sizeOfBodyInBytes))
        } else if (header.apiKey == ApiKeys.ADD_PARTITIONS_TO_TXN && body[AddPartitionsToTxnRequest].allVerifyOnlyRequest) {
          Seq(RequestMetrics.verifyPartitionsInTxnMetricName)
        } else {
          Seq(header.apiKey.name)
        }
      overrideMetricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestRateAcrossVersions.foreach(_.mark())
        m.deprecatedRequestRate(header.apiKey, header.apiVersion, context.clientInformation).foreach(_.mark())
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(apiThrottleTimeMs)
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.totalTimeBucketHist.foreach(_.update(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.responseBytesHist.foreach(_.update(responseBytes))
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled(header)) {
        val desc = RequestConvertToJson.requestDescMetrics(header, requestLog, response.responseLog,
          context, session, isForwarded,
          totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
          apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
          responseSendTimeMs, temporaryMemoryBytes,
          messageConversionsTimeMs)
        val logPrefix = "Completed request:{}"
        // log deprecated apis at `info` level to allow them to be selectively enabled
        if (header.isApiVersionDeprecated())
          requestLogger.info(logPrefix, desc)
        else
          requestLogger.debug(logPrefix, desc)
      }
    }

    def releaseBuffer(): Unit = {
      envelope match {
        case Some(request) =>
          request.releaseBuffer()
        case None =>
          if (buffer != null) {
            memoryPool.release(buffer)
            buffer = null
          }
      }
    }

    override def toString: String = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer, " +
      s"envelope=$envelope)"

  }

  sealed abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def responseLog: Option[JsonNode] = None

    def onComplete: Option[Send => Unit] = None
  }

  /** responseLogValue should only be defined if request logging is enabled */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val responseLogValue: Option[JsonNode],
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseLog: Option[JsonNode] = responseLogValue

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseLogValue)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

class RequestChannel(val queueSize: Int,
                     val metricNamePrefix: String,
                     time: Time,
                     val metrics: RequestChannel.Metrics,
                     val observer: Observer = new NoOpObserver,
                     enableDequeueWatchdog: Boolean = false) {
  def this(queueSize: Int, metricNamePrefix: String, time: Time, metrics: RequestChannel.Metrics) =
    this(queueSize, metricNamePrefix, time, metrics, new NoOpObserver, enableDequeueWatchdog = false)

  def this(queueSize: Int, metricNamePrefix: String, time: Time, metrics: RequestChannel.Metrics,
           observer: Observer) =
    this(queueSize, metricNamePrefix, time, metrics, observer, enableDequeueWatchdog = false)


  import RequestChannel._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()
  private val requestQueueSizeMetricName = metricNamePrefix.concat(RequestQueueSizeMetric)
  private val responseQueueSizeMetricName = metricNamePrefix.concat(ResponseQueueSizeMetric)
  private val callbackQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  @volatile var lastDequeueTimeMs: Long = Long.MaxValue
  // SocketServer enables this only on the data-plane channel. The unprefixed name is part of the
  // 3.0-li monitoring contract consumed by the kafka-server wrapper.
  private val requestDequeuePollIntervalMs = if (enableDequeueWatchdog)
    Some(metricsGroup.newHistogram("RequestDequeuePollIntervalMs"))
  else None

  metricsGroup.newGauge(requestQueueSizeMetricName, () => requestQueue.size)

  metricsGroup.newGauge(responseQueueSizeMetricName, () => {
    processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId ${processor.id}")

    metricsGroup.newGauge(responseQueueSizeMetricName, () => processor.responseQueueSize,
      Map(ProcessorMetricTag -> processor.id.toString).asJava)
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    metricsGroup.removeMetric(responseQueueSizeMetricName, Map(ProcessorMetricTag -> processorId.toString).asJava)
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  def sendResponse(
    request: RequestChannel.Request,
    response: AbstractResponse,
    onComplete: Option[Send => Unit]
  ): Unit = {
    try observer.observe(request.context, request.body[AbstractRequest], response)
    catch {
      case NonFatal(e) => warn("Request observer failed while observing a response", e)
    }
    updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala)
    val responseSend = request.buildResponseSend(response)
    request.responseBytes = responseSend.size
    if (metrics.legacyRequestMetricsEnabled)
      BrokerMetadataStats.outgoingBytesRate.mark(responseSend.size)
    sendResponse(new RequestChannel.SendResponse(
      request,
      responseSend,
      request.responseNode(response),
      onComplete
    ))
  }

  def sendNoOpResponse(request: RequestChannel.Request): Unit = {
    sendResponse(new network.RequestChannel.NoOpResponse(request))
  }

  def startThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new RequestChannel.StartThrottlingResponse(request))
  }

  def endThrottling(request: RequestChannel.Request): Unit = {
    sendResponse(new EndThrottlingResponse(request))
  }

  /** Send a response back to the socket server to be sent over the network */
  private[network] def sendResponse(response: RequestChannel.Response): Unit = {
    if (isTraceEnabled) {
      val requestHeader = response.request.headerForLoggingOrThrottling()
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    response match {
      // We should only send one of the following per request
      case _: SendResponse | _: NoOpResponse | _: CloseConnectionResponse =>
        val request = response.request
        val timeNanos = time.nanoseconds()
        request.responseCompleteTimeNanos = timeNanos
        if (request.apiLocalCompleteTimeNanos == -1L)
          request.apiLocalCompleteTimeNanos = timeNanos
        // If this callback was executed after KafkaApis returned we will need to adjust the callback completion time here.
        if (request.callbackRequestDequeueTimeNanos.isDefined && request.callbackRequestCompleteTimeNanos.isEmpty)
          request.callbackRequestCompleteTimeNanos = Some(time.nanoseconds())
      // For a given request, these may happen in addition to one in the previous section, skip updating the metrics
      case _: StartThrottlingResponse | _: EndThrottlingResponse => ()
    }

    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed
   *  Check the callback queue and execute first if present since these
   *  requests have already waited in line. */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = {
    recordRequestDequeuePoll()
    val callbackRequest = callbackQueue.poll()
    if (callbackRequest != null)
      callbackRequest
    else {
      val request = requestQueue.poll(timeout, TimeUnit.MILLISECONDS)
      request match {
        case WakeupRequest => callbackQueue.poll()
        case _ => request
      }
    }
  }

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest = {
    recordRequestDequeuePoll()
    requestQueue.take()
  }

  private def recordRequestDequeuePoll(): Unit = {
    val now = time.milliseconds()
    if (lastDequeueTimeMs != Long.MaxValue)
      requestDequeuePollIntervalMs.foreach(_.update(math.max(0L, now - lastDequeueTimeMs)))
    lastDequeueTimeMs = now
  }

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]): Unit = {
    errors.forKeyValue { (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }

  def clear(): Unit = {
    requestQueue.clear()
    callbackQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
    requestDequeuePollIntervalMs.foreach(_ => metricsGroup.removeMetric("RequestDequeuePollIntervalMs"))
    metrics.close()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

  def sendCallbackRequest(request: CallbackRequest): Unit = {
    callbackQueue.put(request)
    if (!requestQueue.offer(RequestChannel.WakeupRequest))
      trace("Wakeup request could not be added to queue. This means queue is full, so we will still process callback.")
  }

}

object RequestMetrics {
  val consumerFetchMetricName: String = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName: String = ApiKeys.FETCH.name + "Follower"

  val verifyPartitionsInTxnMetricName: String = ApiKeys.ADD_PARTITIONS_TO_TXN.name + "Verification"

  val RequestsPerSec: String = "RequestsPerSec"
  val RequestsPerSecAcrossVersions: String = "RequestsPerSecAcrossVersions"
  val DeprecatedRequestsPerSec: String = "DeprecatedRequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val ResponseBytes = "ResponseBytes"
  val MessageConversionsTimeMs: String = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes: String = "TemporaryMemoryBytes"
  val ErrorsPerSec: String = "ErrorsPerSec"

  def getBucketBoundaryObjectMap[T](boundaries: Array[Int], prefix: String, postfix: String,
                                    create: String => T, addBinNumber: Boolean): util.TreeMap[Int, T] = {
    val result = new util.TreeMap[Int, T]
    boundaries.indices.foreach { index =>
      val left = boundaries(index)
      val bin = if (addBinNumber) s"_Bin${index + 1}_" else ""
      val name = if (index == boundaries.length - 1) s"$prefix$bin$left${postfix}Greater"
      else s"$prefix$bin${left}To${boundaries(index + 1)}$postfix"
      result.put(left, create(name))
    }
    result
  }

  def getBucketObject[T](objects: util.TreeMap[Int, T], value: Int): T =
    if (value < objects.firstKey) objects.firstEntry.getValue else objects.floorEntry(value).getValue
}

private case class DeprecatedRequestRateKey(version: Short, clientInformation: ClientInformation)

class RequestMetrics(name: String,
                     bucketConfig: Option[KafkaConfig] = None,
                     legacyRequestMetricsEnabled: Boolean = false) {

  import RequestMetrics._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags: util.Map[String, String] = Map("request" -> name).asJava
  private val requestRateInternal = new Pool[Short, Meter]()
  val requestRateAcrossVersions: Option[Meter] = if (legacyRequestMetricsEnabled)
    Some(metricsGroup.newMeter(RequestsPerSecAcrossVersions, "requests", TimeUnit.SECONDS, tags))
  else None
  private val deprecatedRequestRateInternal = new Pool[DeprecatedRequestRateKey, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist: Histogram = metricsGroup.newHistogram(RequestQueueTimeMs, true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist: Histogram = metricsGroup.newHistogram(LocalTimeMs, true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist: Histogram = metricsGroup.newHistogram(RemoteTimeMs, true, tags)
  // time a request is throttled, not part of the request processing time (throttling is done at the client level
  // for clients that support KIP-219 and by muting the channel for the rest)
  val throttleTimeHist: Histogram = metricsGroup.newHistogram(ThrottleTimeMs, true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist: Histogram = metricsGroup.newHistogram(ResponseQueueTimeMs, true, tags)
  // time to send the response to the requester
  val responseSendTimeHist: Histogram = metricsGroup.newHistogram(ResponseSendTimeMs, true, tags)
  val totalTimeHist: Histogram = metricsGroup.newHistogram(TotalTimeMs, true, tags)
  // request and response sizes in bytes
  val requestBytesHist: Histogram = metricsGroup.newHistogram(RequestBytes, true, tags)
  val responseBytesHist: Option[Histogram] = if (legacyRequestMetricsEnabled)
    Some(metricsGroup.newHistogram(ResponseBytes, true, tags)) else None
  val totalTimeBucketHist: Option[TotalTimeBuckets] = bucketConfig
    .filter(_.totalTimeHistogramEnabledMetrics.exists(_.equalsIgnoreCase(name)))
    .map(config => new TotalTimeBuckets(config.requestMetricsTotalTimeBuckets))
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist: Option[Histogram] =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(MessageConversionsTimeMs, true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist: Option[Histogram] =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(metricsGroup.newHistogram(TemporaryMemoryBytes, true, tags))
    else
      None

  final class TotalTimeBuckets(boundaries: Array[Int]) {
    private val counters: util.TreeMap[Int, (String, Counter)] =
      RequestMetrics.getBucketBoundaryObjectMap(boundaries, "TotalTime", "Ms", metricName =>
        metricName -> KafkaYammerMetrics.defaultRegistry.newCounter(metricsGroup.metricName(metricName, tags)),
        addBinNumber = true)

    def update(value: Double): Unit = {
      val bounded = math.min(Int.MaxValue.toLong, Math.round(value)).toInt
      RequestMetrics.getBucketObject(counters, bounded)._2.inc()
    }

    def remove(): Unit = counters.values.asScala.foreach { case (metricName, _) =>
      metricsGroup.removeMetric(metricName, tags)
    }
  }

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter =
    requestRateInternal.getAndMaybePut(version, metricsGroup.newMeter(RequestsPerSec, "requests", TimeUnit.SECONDS, tagsWithVersion(version)))

  def deprecatedRequestRate(apiKey: ApiKeys, version: Short, clientInformation: ClientInformation): Option[Meter] =
    if (apiKey.isVersionDeprecated(version)) {
      Some(deprecatedRequestRateInternal.getAndMaybePut(DeprecatedRequestRateKey(version, clientInformation),
        metricsGroup.newMeter(DeprecatedRequestsPerSec, "requests", TimeUnit.SECONDS, tagsWithVersionAndClientInfo(version, clientInformation))))
    } else None

  private def tagsWithVersion(version: Short): java.util.Map[String, String] = {
    val nameAndVersionTags = new util.LinkedHashMap[String, String](math.ceil((tags.size() + 1) / 0.75).toInt) // take load factor into account
    nameAndVersionTags.putAll(tags)
    nameAndVersionTags.put("version", version.toString)
    nameAndVersionTags
  }

  private def tagsWithVersionAndClientInfo(version: Short, clientInformation: ClientInformation): java.util.Map[String, String] = {
    val extendedTags = new util.LinkedHashMap[String, String](math.ceil((tags.size() + 3) / 0.75).toInt) // take load factor into account
    extendedTags.putAll(tags)
    extendedTags.put("version", version.toString)
    extendedTags.put("clientSoftwareName", clientInformation.softwareName)
    extendedTags.put("clientSoftwareVersion", clientInformation.softwareVersion)
    extendedTags
  }

  private class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name).asJava

    @volatile private var meter: Meter = _

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = metricsGroup.newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          metricsGroup.removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int): Unit = {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys)
      metricsGroup.removeMetric(RequestsPerSec, tagsWithVersion(version))
    for (key <- deprecatedRequestRateInternal.keys)
      metricsGroup.removeMetric(DeprecatedRequestsPerSec, tagsWithVersionAndClientInfo(key.version, key.clientInformation))
    metricsGroup.removeMetric(RequestQueueTimeMs, tags)
    metricsGroup.removeMetric(LocalTimeMs, tags)
    metricsGroup.removeMetric(RemoteTimeMs, tags)
    metricsGroup.removeMetric(RequestsPerSec, tags)
    if (legacyRequestMetricsEnabled)
      metricsGroup.removeMetric(RequestsPerSecAcrossVersions, tags)
    metricsGroup.removeMetric(ThrottleTimeMs, tags)
    metricsGroup.removeMetric(ResponseQueueTimeMs, tags)
    metricsGroup.removeMetric(TotalTimeMs, tags)
    metricsGroup.removeMetric(ResponseSendTimeMs, tags)
    metricsGroup.removeMetric(RequestBytes, tags)
    if (legacyRequestMetricsEnabled)
      metricsGroup.removeMetric(ResponseBytes, tags)
    totalTimeBucketHist.foreach(_.remove())
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      metricsGroup.removeMetric(MessageConversionsTimeMs, tags)
      metricsGroup.removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
