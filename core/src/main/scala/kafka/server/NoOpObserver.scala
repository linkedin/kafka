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

package kafka.server

import java.util.Map
import java.util.concurrent.TimeUnit
import kafka.network.RequestChannel
import org.apache.kafka.common.requests.AbstractResponse

/**
  * An observer implementation that has no operation and serves as a place holder.
  */
class NoOpObserver extends Observer {

  def configure(configs: Map[String, _]): Unit = {}

  /**
    * Observer the record based on the given information.
    */
  def observe(request: RequestChannel.Request, response: AbstractResponse): Unit = {}

  /**
    * Close the observer with timeout.
    */
  def close(timeout: Long, unit: TimeUnit): Unit = {}

}
