/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.kafka.common.message.ListXinfraTopicsRequestData;
import org.apache.kafka.common.message.ListXinfraTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class ListXinfraTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListXinfraTopicsRequest> {

        private final ListXinfraTopicsRequestData data;

        public Builder(ListXinfraTopicsRequestData data, short allowedVersion) {
            super(ApiKeys.LIST_XINFRA_TOPICS, allowedVersion);
            this.data = data;
        }

        @Override
        public ListXinfraTopicsRequest build(short version) {
            return new ListXinfraTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListXinfraTopicsRequestData data;

    public ListXinfraTopicsRequest(ListXinfraTopicsRequestData data, short version) {
        super(ApiKeys.LIST_XINFRA_TOPICS, version);
        this.data = data;
    }

    @Override
    public ListXinfraTopicsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ListXinfraTopicsResponseData data = new ListXinfraTopicsResponseData().
            setTopics(Collections.emptyList()).
            setErrorCode(Errors.forException(e).code());
        return new ListXinfraTopicsResponse(data, version());
    }

    public static ListXinfraTopicsRequest parse(ByteBuffer buffer, short version) {
        return new ListXinfraTopicsRequest(new ListXinfraTopicsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public ListXinfraTopicsRequestData data() {
        return data;
    }
}
