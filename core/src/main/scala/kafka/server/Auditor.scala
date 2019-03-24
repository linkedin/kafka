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

import java.util.concurrent.TimeUnit
import kafka.network.RequestChannel
import org.apache.kafka.common.requests.AbstractResponse
import org.apache.kafka.common.Configurable

/**
  * Top level interface that all pluggable auditor must implement. Kafka will read the 'auditor.class.name' config
  * value at startup time, create an instance of the specificed class using the default constructor, and call its
  * 'configure' method.
  *
  * From that point onwards, every pair of request and response will be routed to the 'record' method.
  *
  * If 'auditor.class.name' has no value specified or the specified class does not exist, the <code>NoOpAuditor</code>
  * will be used as a place holder.
  */
trait Auditor extends Configurable {

  /**
    * Audit the record based on the given information.
    *
    * @param request  the request being recorded for some auditing purpose
    * @param response the response to the request
    */
  def record(request: RequestChannel.Request, response: AbstractResponse): Unit

  /**
    * Close the auditor with timeout.
    *
    * @param timeout the maximum time to wait to close the auditor.
    * @param unit    the time unit.
    */
  def close(timeout: Long, unit: TimeUnit): Unit
}
