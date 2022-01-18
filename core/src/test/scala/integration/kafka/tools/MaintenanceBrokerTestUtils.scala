package integration.kafka.tools

import kafka.server.{DynamicConfig, KafkaServer}
import kafka.utils.CoreUtils.propsWith
import kafka.utils.TestUtils
import kafka.zk.{AdminZkClient, KafkaZkClient}

object MaintenanceBrokerTestUtils {

  def setMaintenanceBrokers(adminZkClient: AdminZkClient,
                            zkClient: KafkaZkClient,
                            brokers: Seq[KafkaServer],
                            maintenanceBrokerIds: Seq[Int]): Unit = {
    val propstring = maintenanceBrokerIds.mkString(",")
    adminZkClient.changeBrokerConfig(None,
      propsWith((DynamicConfig.Broker.MaintenanceBrokerListProp, propstring)))

    val controllerId = TestUtils.waitUntilControllerElected(zkClient)

    TestUtils.waitUntilTrue(() => brokers(controllerId).config.getMaintenanceBrokerList == maintenanceBrokerIds,
      s"wait until broker $propstring is masked as maintenance broker not taking new partitions", 5000)
  }

}
