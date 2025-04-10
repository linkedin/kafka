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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


/**
 * A map that holds exceptions as keys. Exceptions are compared by a custom fingerprint that depends on the message
 * and cause chain. This map is not thread-safe.
 *
 * @param <V> The type of the value in the map.
 */
public class ExceptionMap<V> {
    public static class ExceptionFingerprint {
        public static final ExceptionFingerprint EMPTY = new ExceptionFingerprint(null);
        private final long hashCode;

        /**
         * Create a fingerprint from an exception
         */
        public ExceptionFingerprint(Throwable throwable) {
            this.hashCode = computeHashCode(throwable);
        }

        /**
         * Compute a hash code for the entire exception chain
         */
        private long computeHashCode(Throwable throwable) {
            long result = 1L;
            Throwable current = throwable;

            while (current != null) {
                // Include class type, message, and first stack element in the hash
                StackTraceElement[] stackTrace = current.getStackTrace();
                StackTraceElement origin = stackTrace.length > 0 ? stackTrace[0] : null;

                long elementHash = 31L;
                elementHash = 31L * elementHash + current.getClass().getName().hashCode();
                elementHash = 31L * elementHash + (current.getMessage() != null ? current.getMessage().hashCode() : 0);

                if (origin != null) {
                    elementHash = 31L * elementHash + origin.getClassName().hashCode();
                    elementHash = 31L * elementHash + origin.getMethodName().hashCode();
                    elementHash = 31L * elementHash + origin.getLineNumber();
                }

                // Combine with running result
                result = 31L * result + elementHash;

                // Move to the next exception in the chain
                current = current.getCause();
            }

            return result;
        }

        @Override
        public int hashCode() {
            return (int) (hashCode ^ (hashCode >>> 32));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            ExceptionFingerprint other = (ExceptionFingerprint) obj;
            return this.hashCode == other.hashCode;
        }
    }

    private final Map<ExceptionFingerprint, V> map = new HashMap<>();

    public V computeIfAbsent(Exception e, Function<? super Exception, ? extends V> mappingFunction) {
        ExceptionFingerprint fingerprint = e == null ? ExceptionFingerprint.EMPTY : new ExceptionFingerprint(e);
        return map.computeIfAbsent(fingerprint, k -> mappingFunction.apply(e));
    }
}
