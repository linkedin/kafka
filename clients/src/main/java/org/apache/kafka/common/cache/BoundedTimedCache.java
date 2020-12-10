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
package org.apache.kafka.common.cache;

import java.time.Duration;

/**
 * A bounded LRU cache whose entries expire after the specified TTL duration has elapsed.
 */
public class BoundedTimedCache<K, V> implements Cache<K, V> {
    private final LRUCache<K, CacheEntry> cache;
    private final Duration ttl;

    public BoundedTimedCache(final int maxSize, Duration ttl) {
        this.cache = new LRUCache<>(maxSize);
        this.ttl = ttl;
    }

    @Override
    public V get(K key) {
        CacheEntry entry = this.cache.get(key);
        if (entry == null) {
            return null;
        }

        if (System.currentTimeMillis() >= entry.expiresAt) {
            return null;
        }

        return entry.value;
    }

    @Override
    public void put(K key, V value) {
        this.cache.put(key, new CacheEntry(System.currentTimeMillis() + ttl.toMillis(), value));
    }

    @Override
    public boolean remove(K key) {
        return this.cache.remove(key);
    }

    @Override
    public long size() {
        return this.cache.size();
    }

    private class CacheEntry {
        final long expiresAt;
        final V value;

        CacheEntry(long expiresAt, V value) {
            this.expiresAt = expiresAt;
            this.value = value;
        }
    }
}
