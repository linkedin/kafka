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
import java.util.Map;
import org.apache.kafka.common.message.LiXinfraTopicCreateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiXinfraTopicCreateResponse extends AbstractResponse {
    private final LiXinfraTopicCreateResponseData data;
    private final short version;

    public LiXinfraTopicCreateResponse(LiXinfraTopicCreateResponseData data, short version) {
        super(ApiKeys.LI_XINFRA_TOPIC_CREATE);
        this.data = data;
        this.version = version;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(error(), 1);
    }

    @Override
    public LiXinfraTopicCreateResponseData data() {
        return data;
    }

    public static LiXinfraTopicCreateResponse prepareResponse(Errors error, short version) {
        LiXinfraTopicCreateResponseData data = new LiXinfraTopicCreateResponseData();
        data.setErrorCode(error.code());
        return new LiXinfraTopicCreateResponse(data, version);
    }

    public static LiXinfraTopicCreateResponse parse(ByteBuffer buffer, short version) {
        return new LiXinfraTopicCreateResponse(new LiXinfraTopicCreateResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    public short version() {
        return version;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
