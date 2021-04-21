package integration.kafka.api

import java.util.Properties

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import org.junit.Assert
import org.junit.Test
import org.scalatest.Matchers.fail

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class LiCombinedControlRequestTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val overridingProps = new Properties()

  // overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)
  override def generateConfigs = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @Test
  def testChangingLiCombinedControlRequestFlag(): Unit = {
    def createTopicAndVerifyMetric(topicsToCreate: Set[String], shouldHaveLiCombinedControlRequests: Boolean) = {
      for (topic <- topicsToCreate) {
        createTopic(topic, 1, 1)
      }

      // verify that no LiCombinedControlRequest has been sent
      val metrics = Metrics.defaultRegistry.allMetrics.asScala.filter { case (n, metric) =>
        n.getMBeanName.contains("name=LiCombinedControlRequestRateAndTimeMs")
      }
      Assert.assertTrue("Unable to get the LiCombinedControlRequestRateAndTimeMs metric", metrics.size == 1)
      metrics.foreach {
        case (_, metric) => {
          val foundLiCombinedControlRequests = metric.asInstanceOf[Timer].count() != 0
          Assert.assertTrue("The LiCombinedControlRequestRateAndTimeMs metric doesn't match expectation",
            shouldHaveLiCombinedControlRequests == foundLiCombinedControlRequests)
        }
      }
    }

    val controller = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    Assert.assertFalse(controller.controllerContext.liCombinedControlRequestEnabled)

    createTopicAndVerifyMetric(Set(0, 1).map("topic" + _), false)

    // turn on the feature by setting the /li_combined_control_request_flag to true
    zkClient.setLiCombinedControlRequestFlag("true")
    TestUtils.waitUntilTrue(() => {
      controller.controllerContext.liCombinedControlRequestEnabled
    }, "The liCombinedControlRequestEnabled on the controller cannot be enabled")

    createTopicAndVerifyMetric(Set(2, 3).map("topic" + _), true)
  }
}
