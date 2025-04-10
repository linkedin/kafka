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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExceptionMapTest {
    @Test
    public void testIdenticalExceptions() {
        // Generate exceptions with the same construction site.
        List<Exception> exceptions = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            try {
                throw new RuntimeException("test");
            } catch (RuntimeException e) {
                exceptions.add(e);
            }
        }

        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(exceptions.get(0), e -> 1);
        int b = exceptionMap.computeIfAbsent(exceptions.get(1), e -> 2);

        // The two exceptions are identical, so the same value should be returned.
        assertEquals(a, b);
    }

    @Test
    public void testDifferentSite() {
        Exception e1 = new RuntimeException("test");
        Exception e2 = new RuntimeException("test");

        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(e1, e -> 1);
        int b = exceptionMap.computeIfAbsent(e2, e -> 2);

        // The 2 exceptions are very similar, but have different construction sites, so they aren't the same.
        assertEquals(a, 1);
        assertEquals(b, 2);
    }

    @Test
    public void testDifferentMessage() {
        // Generate exceptions with the same construction site.
        List<Exception> exceptions = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            try {
                throw new RuntimeException("test" + i);
            } catch (RuntimeException e) {
                exceptions.add(e);
            }
        }

        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(exceptions.get(0), e -> 1);
        int b = exceptionMap.computeIfAbsent(exceptions.get(1), e -> 2);

        // The 2 exceptions have different messages, so they aren't the same.
        assertEquals(a, 1);
        assertEquals(b, 2);
    }

    @Test
    public void testDifferentCause() {
        Exception cause1 = new RuntimeException("cause1");
        Exception cause2 = new RuntimeException("cause2");

        // Generate exceptions with the same construction site.
        List<Exception> exceptions = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            try {
                throw new RuntimeException("test", i == 0 ? cause1 : cause2);
            } catch (RuntimeException e) {
                exceptions.add(e);
            }
        }

        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(exceptions.get(0), e -> 1);
        int b = exceptionMap.computeIfAbsent(exceptions.get(1), e -> 2);

        // The 2 exceptions have different causes, so they aren't the same.
        assertEquals(a, 1);
        assertEquals(b, 2);
    }

    @Test
    public void testExceptionWithNoMessage() {
        Exception e = new RuntimeException();
        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(e, ex -> 1);
        assertEquals(a, 1);
    }

    @Test
    public void testExceptionWithEmptyMessage() {
        Exception e = new RuntimeException("");
        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        int a = exceptionMap.computeIfAbsent(e, ex -> 1);
        assertEquals(a, 1);
    }

    @Test
    public void testExceptionMapWithNullThrowable() {
        ExceptionMap<Integer> exceptionMap = new ExceptionMap<>();
        exceptionMap.computeIfAbsent(null, e -> 1);
    }

    @Test
    public void testExceptionFingerprintWithNullException() {
        ExceptionMap.ExceptionFingerprint exceptionFingerprint = new ExceptionMap.ExceptionFingerprint(null);
        assertEquals(1, exceptionFingerprint.hashCode());
    }
}