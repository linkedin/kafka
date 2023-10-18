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
import org.apache.kafka.common.message.LiXinfraTopicDeleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiXinfraTopicDeleteResponse extends AbstractResponse {
    private final LiXinfraTopicDeleteResponseData data;
    private final short version;

    public LiXinfraTopicDeleteResponse(LiXinfraTopicDeleteResponseData data, short version) {
        super(ApiKeys.LI_XINFRA_TOPIC_DELETE);
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
    public LiXinfraTopicDeleteResponseData data() {
        return data;
    }

    public static LiXinfraTopicDeleteResponse prepareResponse(Errors error, short version) {
        LiXinfraTopicDeleteResponseData data = new LiXinfraTopicDeleteResponseData();
        data.setErrorCode(error.code());
        return new LiXinfraTopicDeleteResponse(data, version);
    }

    public static LiXinfraTopicDeleteResponse parse(ByteBuffer buffer, short version) {
        return new LiXinfraTopicDeleteResponse(new LiXinfraTopicDeleteResponseData(new ByteBufferAccessor(buffer), version), version);
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
