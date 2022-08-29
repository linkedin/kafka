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

package org.apache.kafka.common.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MemoryPoolStatsStore {
    private static final Logger log = LoggerFactory.getLogger(MemoryPoolStatsStore.class);

    private final AtomicInteger[] histogram;
    private final int maxSizeBytes;
    private final int segmentSizeBytes;

    public static class Range {
        public final int startInclusive;
        public final int endInclusive;

        public Range(int startInclusive, int endInclusive) {
            this.startInclusive = startInclusive;
            this.endInclusive = endInclusive;
        }

        @Override
        public String toString() {
            return "Range{" + "startInclusive=" + startInclusive + ", endInclusive=" + endInclusive + '}';
        }
    }

    public MemoryPoolStatsStore(int segments, int maxSizeBytes) {
        histogram = new AtomicInteger[segments];
        this.maxSizeBytes = maxSizeBytes;
        segmentSizeBytes = (int) Math.ceil((double) maxSizeBytes / segments);
        for (int segmentIndex = 0; segmentIndex < segments; segmentIndex++) {
            histogram[segmentIndex] = new AtomicInteger();
        }
    }

    private int getSegmentIndexForBytes(int bytes) {
        if (bytes == 0) {
            throw new IllegalArgumentException("Requested zero bytes for allocation.");
        }
        if (bytes > maxSizeBytes) {
            log.debug("Requested bytes {} for allocation exceeds maximum recorded value {}", bytes, maxSizeBytes);
            return -1;
        } else {
            return (bytes - 1) / segmentSizeBytes;
        }
    }

    public void recordAllocation(int bytes) {
        try {
            final int segmentIndex = getSegmentIndexForBytes(bytes);
            if (segmentIndex != -1) {
                histogram[segmentIndex].incrementAndGet();
            }
        } catch (IllegalArgumentException e) {
            log.error("Encountered error when trying to record memory allocation for request", e);
        }
    }

    public synchronized Map<Range, Integer> getFrequencies() {
        Map<Range, Integer> frequenciesMap = new HashMap<>();
        for (int segmentIndex = 0; segmentIndex < histogram.length; segmentIndex++) {
            frequenciesMap.put(new Range(
                segmentIndex * segmentSizeBytes + 1,
                segmentIndex * segmentSizeBytes + segmentSizeBytes
            ), histogram[segmentIndex].intValue());
        }
        return frequenciesMap;
    }

    public synchronized void clear() {
        for (AtomicInteger atomicInteger : histogram) {
            atomicInteger.set(0);
        }
    }
}
