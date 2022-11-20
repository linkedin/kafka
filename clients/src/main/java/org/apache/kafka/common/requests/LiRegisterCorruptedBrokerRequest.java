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
import org.apache.kafka.common.message.LiRegisterCorruptedBrokerRequestData;
import org.apache.kafka.common.message.LiRegisterCorruptedBrokerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiRegisterCorruptedBrokerRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LiRegisterCorruptedBrokerRequest> {
        private final LiRegisterCorruptedBrokerRequestData data;

        public Builder(LiRegisterCorruptedBrokerRequestData data) {
            super(ApiKeys.LI_REGISTER_CORRUPTED_BROKER);
            this.data = data;
        }

        @Override
        public LiRegisterCorruptedBrokerRequest build(short version) {
            return new LiRegisterCorruptedBrokerRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LiRegisterCorruptedBrokerRequestData data;

    LiRegisterCorruptedBrokerRequest(LiRegisterCorruptedBrokerRequestData data, short version) {
        super(ApiKeys.LI_REGISTER_CORRUPTED_BROKER, version);
        this.data = data;
    }

    public static LiRegisterCorruptedBrokerRequest parse(ByteBuffer buffer, short version) {
        return new LiRegisterCorruptedBrokerRequest(new LiRegisterCorruptedBrokerRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LiRegisterCorruptedBrokerResponseData data = new LiRegisterCorruptedBrokerResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new LiRegisterCorruptedBrokerResponse(data, version());
    }

    public LiRegisterCorruptedBrokerRequestData data() {
        return data;
    }
}
