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
package org.apache.kafka.common.utils;

public class FixedRateLimiter implements RateLimiter {
    // The timekeeper to use for getting the current time.
    private final Time time;

    // How frequently the rate limiter will allow a permit to be acquired.
    private final double permitsPerSecond;

    // The last time a permit was acquired.
    // Initialized to "negative infinity" so that the first call to tryAcquire() will always return true.
    private long lastPermitTimeNs = Long.MIN_VALUE;

    /**
     * Create a new rate limiter.
     *
     * @param time             The timekeeper to use for getting the current time.
     * @param permitsPerSecond How frequently the rate limiter will allow a permit to be acquired. If this is less than or
     *                         equal to 0, the rate limiter will always allow permits to be acquired.
     */
    public FixedRateLimiter(Time time, double permitsPerSecond) {
        this.time = time;
        this.permitsPerSecond = permitsPerSecond;
    }

    @Override
    public boolean tryAcquire() {
        if (permitsPerSecond <= 0) {
            return true;
        }

        long now = time.nanoseconds();
        long waitTimeNs = delayBetweenPermitsNs();
        long targetTimeNs = lastPermitTimeNs + waitTimeNs;
        if (now >= targetTimeNs) {
            lastPermitTimeNs = now;
            return true;
        }

        return false;
    }

    private long delayBetweenPermitsNs() {
        return (long) (1_000_000_000 / permitsPerSecond);
    }
}
