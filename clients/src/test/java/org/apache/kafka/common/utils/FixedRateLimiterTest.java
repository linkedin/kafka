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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FixedRateLimiterTest {
    @Test
    public void testImmediatelyAvailable() {
        Time time = new MockTime();
        FixedRateLimiter limiter = new FixedRateLimiter(time, 1.0);
        assertTrue(limiter.tryAcquire());
    }

    private void assertAlwaysAllows(Time time, RateLimiter limiter) {
        for (int delay = 0; delay < 100; delay++) {
            time.sleep(delay);
            assertTrue(limiter.tryAcquire());
        }
    }

    @Test
    public void testNegativeRate() {
        Time time = new MockTime();
        FixedRateLimiter limiter = new FixedRateLimiter(time, -1.0);
        assertAlwaysAllows(time, limiter);
    }

    @Test
    public void testZeroRate() {
        Time time = new MockTime();
        FixedRateLimiter limiter = new FixedRateLimiter(time, 0.0);
        assertAlwaysAllows(time, limiter);
    }

    @Test
    public void testRateLessThanOne() {
        Time time = new MockTime();
        // Allow 1 permit every 2 seconds.
        FixedRateLimiter limiter = new FixedRateLimiter(time, 0.5);
        assertTrue(limiter.tryAcquire());
        time.sleep(1000);
        assertFalse(limiter.tryAcquire());
        time.sleep(1000);
        assertTrue(limiter.tryAcquire());
        time.sleep(1000);
        assertFalse(limiter.tryAcquire());
        time.sleep(1000);
        assertTrue(limiter.tryAcquire());
    }

    @Test
    public void testRateLessGreaterThanOne() {
        Time time = new MockTime();
        // Allow 1 permit every 0.25 seconds.
        FixedRateLimiter limiter = new FixedRateLimiter(time, 4);

        assertTrue(limiter.tryAcquire());
        time.sleep(100);
        assertFalse(limiter.tryAcquire());
        time.sleep(100);
        assertFalse(limiter.tryAcquire());
        time.sleep(100);
        assertTrue(limiter.tryAcquire());
        time.sleep(100);
        assertFalse(limiter.tryAcquire());
        time.sleep(100);
        assertFalse(limiter.tryAcquire());
        time.sleep(100);
        assertTrue(limiter.tryAcquire());
    }
}
