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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.metrics.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of memory pool which recycles buffers of commonly used size.
 * This memory pool is useful if most of the requested buffers' size are within close size range.
 * In this case, instead of deallocate and reallocate the buffer, the memory pool will recycle the buffer for future use.
 */
public class RecyclingMemoryPool implements MemoryPool {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final int cacheableBufferSizeUpperThreshold;
    protected final int cacheableBufferSizeLowerThreshold;
    protected final int bufferCacheCapacity;
    protected final AtomicInteger numAllocatedCacheableBuffer;
    protected final ConcurrentLinkedDeque<ByteBuffer> bufferCache;
    protected volatile Sensor requestSensor;

    public RecyclingMemoryPool(int cacheableBufferSize, int bufferCacheCapacity, Sensor requestSensor) {
        if (bufferCacheCapacity <= 0 || cacheableBufferSize <= 0) {
            throw new IllegalArgumentException(String.format("Must provide a positive cacheable buffer size and buffer cache " +
                    "capacity, provided %d and %d respectively.", cacheableBufferSize, bufferCacheCapacity));
        }
        this.bufferCache = new ConcurrentLinkedDeque<>();
        this.cacheableBufferSizeUpperThreshold = cacheableBufferSize;
        this.cacheableBufferSizeLowerThreshold = cacheableBufferSize / 2;
        this.bufferCacheCapacity = bufferCacheCapacity;
        this.requestSensor = requestSensor;
        this.numAllocatedCacheableBuffer = new AtomicInteger(0);
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 1) {
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        }

        ByteBuffer allocated = null;
        if (sizeBytes > cacheableBufferSizeLowerThreshold  && sizeBytes <= cacheableBufferSizeUpperThreshold) {
            allocated = maybePopBufferFromCache();
        }
        if (allocated == null) {
            allocated = ByteBuffer.allocate(sizeBytes);
        }
        bufferToBeAllocated(allocated);
        return allocated;
    }

    /**
     * If (1) there are recycled buffers in cache or (2) more buffer of size {@link #cacheableBufferSizeUpperThreshold} can be allocated,
     * (allocate the buffer and) return the buffer to the client.
     *
     * @return The available buffer if any.
     */
    protected ByteBuffer maybePopBufferFromCache() {
        ByteBuffer byteBuffer = bufferCache.poll();
        if (byteBuffer == null) {
            int allocated = numAllocatedCacheableBuffer.updateAndGet(v -> v > bufferCacheCapacity ? v : v + 1);
            if (allocated <= bufferCacheCapacity) {
                byteBuffer = ByteBuffer.allocate(cacheableBufferSizeUpperThreshold);
                bufferToBeAllocated(byteBuffer);
            }
        }
        return byteBuffer;
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null) {
            throw new IllegalArgumentException("provided null buffer");
        }
        if (previouslyAllocated.capacity() == cacheableBufferSizeUpperThreshold) {
            maybeRecycleBufferToCache(previouslyAllocated);
        }
        bufferToBeReleased(previouslyAllocated);
    }

    protected void maybeRecycleBufferToCache(ByteBuffer previouslyAllocated) {
        if (bufferCache.size() < bufferCacheCapacity) {
            bufferCache.offer(previouslyAllocated);
        }
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is returned to client code.
    protected void bufferToBeAllocated(ByteBuffer justAllocated) {
        this.requestSensor.record(justAllocated.capacity());
        log.trace("allocated buffer of size {}", justAllocated.capacity());
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is marked as reclaimed.
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        log.trace("released buffer of size {}", justReleased.capacity());
    }

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }
}
