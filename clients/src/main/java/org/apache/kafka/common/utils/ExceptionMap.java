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
 * A map that holds exceptions as keys. Exceptions are compared by their concatenated stack trace and cause chain. This
 * map is not thread-safe.
 *
 * @param <V> The type of the value in the map.
 */
public class ExceptionMap<V> {
    private final Map<String, V> map = new HashMap<>();

    public V computeIfAbsent(Exception e, Function<? super Exception, ? extends V> mappingFunction) {
        String key = getKey(e);
        return map.computeIfAbsent(key, k -> mappingFunction.apply(e));
    }

    private String getKey(Exception e) {
        if (e == null) {
            return "";
        }

        return Utils.stackTrace(e);
    }
}
