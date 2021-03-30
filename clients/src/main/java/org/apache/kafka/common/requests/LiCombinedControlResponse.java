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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.LiCombinedControlResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;


class LiCombinedControlResponse extends AbstractResponse {
    private final LiCombinedControlResponseData data;
    public LiCombinedControlResponse(LiCombinedControlResponseData data) {
        this.data = data;
    }

    public LiCombinedControlResponse(Struct struct, short version) {
        this.data = new LiCombinedControlResponseData(struct, version);
    }

    public List<LiCombinedControlResponseData.LeaderAndIsrPartitionError> partitions() {
        return data.leaderAndIsrPartitionErrors();
    }

    private Errors leaderAndIsrError() {
        return Errors.forCode(data.leaderAndIsrErrorCode());
    }
    private Errors updateMetadataError() {
        return Errors.forCode(data.updateMetadataErrorCode());
    }
    public Errors error() {
        // To be backward compatible with the existing API, which can only return one error,
        // we give the following priorities LeaderAndIsr error > Stop Replica error > UpdateMetadata error
        Errors leaderAndIsrError = leaderAndIsrError();
        if (leaderAndIsrError != Errors.NONE) {
            return leaderAndIsrError;
        }
        // TODO: handle stop replica errors

        Errors updateMetadataError = updateMetadataError();
        if (updateMetadataError != Errors.NONE) {
            return updateMetadataError;
        }

        return Errors.NONE;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors leaderAndIsrError = leaderAndIsrError();
        Map<Errors, Integer> leaderAndIsrErrorCount;
        if (leaderAndIsrError != Errors.NONE) {
            // Minor optimization since the top-level error applies to all partitions
            leaderAndIsrErrorCount = Collections.singletonMap(leaderAndIsrError, data.leaderAndIsrPartitionErrors().size());
        } else {
            leaderAndIsrErrorCount = errorCounts(data.leaderAndIsrPartitionErrors().stream().map(l -> Errors.forCode(l.errorCode())).collect(Collectors.toList()));
        }

        Map<Errors, Integer> updateMetadataErrorCount = errorCounts(updateMetadataError());

        // merge the several count maps into one result
        Map<Errors, Integer> combinedErrorCount = leaderAndIsrErrorCount;
        for (Map.Entry<Errors, Integer> entry: updateMetadataErrorCount.entrySet()) {
            Errors key = entry.getKey();
            Integer value = entry.getValue();
            if (combinedErrorCount.containsKey(key)) {
                combinedErrorCount.put(key, combinedErrorCount.get(key) + value);
            } else {
                combinedErrorCount.put(key, value);
            }
        }

        return combinedErrorCount;
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrResponse(ApiKeys.LEADER_AND_ISR.parseResponse(version, buffer), version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
