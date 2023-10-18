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
import org.apache.kafka.common.message.LiXinfraTopicDeleteRequestData;
import org.apache.kafka.common.message.LiXinfraTopicDeleteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiXinfraTopicDeleteRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiXinfraTopicDeleteRequest> {
        private final LiXinfraTopicDeleteRequestData data;

        public Builder(LiXinfraTopicDeleteRequestData data, short allowedVersion) {
            super(ApiKeys.LI_XINFRA_TOPIC_DELETE, allowedVersion);
            this.data = data;
        }

        @Override
        public LiXinfraTopicDeleteRequest build(short version) {
            return new LiXinfraTopicDeleteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiXinfraTopicDeleteRequestData data;

    LiXinfraTopicDeleteRequest(LiXinfraTopicDeleteRequestData data, short version) {
        super(ApiKeys.LI_XINFRA_TOPIC_DELETE, version);
        this.data = data;
    }

    public static LiXinfraTopicDeleteRequest parse(ByteBuffer buffer, short version) {
        return new LiXinfraTopicDeleteRequest(new LiXinfraTopicDeleteRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiXinfraTopicDeleteResponseData data = new LiXinfraTopicDeleteResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new LiXinfraTopicDeleteResponse(data, version());
    }

    @Override
    public LiXinfraTopicDeleteRequestData data() {
        return data;
    }
}
