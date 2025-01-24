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
    private final long intervalMs;

    // The last time a permit was acquired.
    // Initialized to "negative infinity" so that the first call to tryAcquire() will always return true.
    private long lastPermitTimeMs = Long.MIN_VALUE;

    /**
     * Create a new rate limiter.
     *
     * @param time             The timekeeper to use for getting the current time.
     * @param intervalMs       The minimum time between successful calls to {@link #tryAcquire()}.
     */
    public FixedRateLimiter(Time time, long intervalMs) {
        this.time = time;
        this.intervalMs = intervalMs;
    }

    @Override
    public boolean tryAcquire() {
        if (intervalMs <= 0) {
            return true;
        }

        long now = time.milliseconds();
        long targetTimeNs = lastPermitTimeMs + intervalMs;
        if (now >= targetTimeNs) {
            lastPermitTimeMs = now;
            return true;
        }

        return false;
    }
}
