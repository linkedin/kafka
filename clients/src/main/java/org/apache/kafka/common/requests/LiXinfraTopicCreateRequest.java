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
import org.apache.kafka.common.message.LiXinfraTopicCreateRequestData;
import org.apache.kafka.common.message.LiXinfraTopicCreateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiXinfraTopicCreateRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiXinfraTopicCreateRequest> {
        private final LiXinfraTopicCreateRequestData data;

        public Builder(LiXinfraTopicCreateRequestData data, short allowedVersion) {
            super(ApiKeys.LI_XINFRA_TOPIC_CREATE, allowedVersion);
            this.data = data;
        }

        @Override
        public LiXinfraTopicCreateRequest build(short version) {
            return new LiXinfraTopicCreateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiXinfraTopicCreateRequestData data;

    LiXinfraTopicCreateRequest(LiXinfraTopicCreateRequestData data, short version) {
        super(ApiKeys.LI_XINFRA_TOPIC_CREATE, version);
        this.data = data;
    }

    public static LiXinfraTopicCreateRequest parse(ByteBuffer buffer, short version) {
        return new LiXinfraTopicCreateRequest(new LiXinfraTopicCreateRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiXinfraTopicCreateResponseData data = new LiXinfraTopicCreateResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new LiXinfraTopicCreateResponse(data, version());
    }

    @Override
    public LiXinfraTopicCreateRequestData data() {
        return data;
    }
}
