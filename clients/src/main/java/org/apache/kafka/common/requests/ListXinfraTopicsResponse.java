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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.message.ListXinfraTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class ListXinfraTopicsResponse extends AbstractResponse {
    private final ListXinfraTopicsResponseData data;
    private final short version;

    public ListXinfraTopicsResponse(ListXinfraTopicsResponseData data, short version) {
        super(ApiKeys.LIST_XINFRA_TOPICS);
        this.data = data;
        this.version = version;
    }

    @Override
    public ListXinfraTopicsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static ListXinfraTopicsResponse parse(ByteBuffer buffer, short version) {
        return new ListXinfraTopicsResponse(
            new ListXinfraTopicsResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    public static ListXinfraTopicsResponse prepareResponse(Errors error, short version) {
        ListXinfraTopicsResponseData data = new ListXinfraTopicsResponseData();
        data.setErrorCode(error.code());
        return new ListXinfraTopicsResponse(data, version);
    }

    public short version() {
        return version;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
