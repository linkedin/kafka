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
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.Configurable

/**
  * Top level interface that all pluggable observer must implement. Kafka will read the 'observer.class.name' config
  * value at startup time, create an instance of the specificed class using the default constructor, and call its
  * 'configure' method.
  *
  * From that point onwards, every pair of request and response will be routed to the 'record' method.
  *
  * If 'observer.class.name' has no value specified or the specified class does not exist, the <code>NoOpObserver</code>
  * will be used as a place holder.
  */
trait Observer extends Configurable {

  /**
    * Observe the record based on the given information.
    *
    * @param requestContext the context information about the request
    * @param request  the request being observed for a various purpose(s)
    * @param response the response to the request
    */
  def observe(requestContext: RequestContext, request: AbstractRequest, response: AbstractResponse): Unit

  /**
    * Close the observer with timeout.
    *
    * @param timeout the maximum time to wait to close the observer.
    * @param unit    the time unit.
    */
  def close(timeout: Long, unit: TimeUnit): Unit
}

object Observer {

  /**
    * Generates a description of the given request and response. It could be used mostly for debugging purpose.
    *
    * @param request  the request being described
    * @param response the response to the request
    */
  def describeRequestAndResponse(request: RequestChannel.Request, response: AbstractResponse): String = {
    var requestDesc = "Request"
    var responseDesc = "Response"
    try {
      if (request == null) {
        requestDesc += " null"
      } else {
        requestDesc += (" header: " + request.header)
        requestDesc += (" from service with principal: " +
          request.session.sanitizedUser +
          " IP address: " + request.session.clientAddress)
      }
      requestDesc += " | " // Separate the response description from the request description

      if (response == null) {
        responseDesc += " null"
      } else {
        responseDesc += (if (response.errorCounts == null || response.errorCounts.size == 0) {
          " with no error"
        } else {
          " with errors: " + response.errorCounts
        })
      }
    } catch {
      case e: Exception => return e.toString // If describing fails, return the exception message directly
    }
    requestDesc + responseDesc
  }
}