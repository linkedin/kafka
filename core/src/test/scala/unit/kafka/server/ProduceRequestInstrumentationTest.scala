package kafka.server

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.server.instrumentation.ProduceRequestInstrumentation.Stage
import kafka.server.instrumentation.{ProduceRequestInstrumentation, ProduceRequestInstrumentationLogger}
import kafka.utils.MockTime
import org.apache.kafka.controller.MockRandom
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConverters._
import scala.util.Random


class ProduceRequestInstrumentationTest {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  var kafkaConfig: KafkaConfig = _
  var time: MockTime = _
  var rnd: Random = _
  var replicaManager: ReplicaManager = _

  @BeforeEach
  def beforeEach(): Unit = {
    kafkaConfig = EasyMock.createNiceMock(classOf[KafkaConfig])
    time = new MockTime
    rnd = new MockRandom
    replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
  }

  def jsonStrToMap(json: String): collection.Map[String, Long] = {
    mapper.readValue(json, new TypeReference[java.util.Map[String, Long]](){}).asScala
  }

  @Test
  def testLoggerToTimeTakenInEachStageMessage(): Unit = {
    EasyMock.expect(kafkaConfig.longTailProduceRequestLogThresholdMs).andStubReturn(1000L)
    EasyMock.expect(kafkaConfig.longTailProduceRequestLogRatio).andStubReturn(1.0)
    EasyMock.expect(kafkaConfig.dynamicConfig).andStubReturn(EasyMock.createNiceMock(classOf[DynamicBrokerConfig]))
    EasyMock.replay(kafkaConfig)


    val logger = new ProduceRequestInstrumentationLogger(kafkaConfig, time, rnd, replicaManager)

    // Empty should still work
    val markedFinish = new ProduceRequestInstrumentation(time)
    val empty = new ProduceRequestInstrumentation(time)
    time.sleep(100)
    markedFinish.markStage(Stage.Finish)
    assertEquals(Map(Stage.Init.toString -> 100L), jsonStrToMap(logger.toTimeTakenInEachStageMessage(empty)))
    assertEquals(Map(Stage.Init.toString -> 100L), jsonStrToMap(logger.toTimeTakenInEachStageMessage(markedFinish)))

    // Mark some stages
    val markSomeStages = new ProduceRequestInstrumentation(time)
    time.sleep(100)
    markSomeStages.markStage(Stage.Authorization)
    time.sleep(200)
    markSomeStages.markStage(Stage.AppendToLocalLog)
    time.sleep(300)
    markSomeStages.markStage(Stage.Finish)
    assertEquals(
      Map(
        Stage.Init.toString -> 100L,
        Stage.Authorization.toString -> 200L,
        Stage.AppendToLocalLog.toString -> 300L,
      ),
      jsonStrToMap(logger.toTimeTakenInEachStageMessage(markSomeStages))
    )
  }
}
