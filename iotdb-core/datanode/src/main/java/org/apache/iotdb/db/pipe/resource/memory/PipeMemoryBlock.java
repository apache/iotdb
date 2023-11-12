/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.db.pipe.resource.PipeResourceManager;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.LongUnaryOperator;

public class PipeMemoryBlock implements AutoCloseable {

  private PipeMemoryManager pipeMemoryManager = PipeResourceManager.memory();

  private AtomicLong memoryUsageInBytes = new AtomicLong(0);

  private ReentrantReadWriteLock objectLock = new ReentrantReadWriteLock();
  private AtomicReference<Object> monitoredObject = new AtomicReference<>();

  private AtomicReference<LongUnaryOperator> estimatedShrinkResult = new AtomicReference<>();
  private AtomicReference<BiFunction<Long, Object, Long>> shrinkMethod = new AtomicReference<>();

  private AtomicReference<LongUnaryOperator> estimatedExtendResult = new AtomicReference<>();
  private AtomicReference<BiFunction<Long, Object, Long>> extendMethod = new AtomicReference<>();
  private AtomicLong minMemoryUsage = new AtomicLong(0);
  private AtomicLong maxMemoryUsage = new AtomicLong(Long.MAX_VALUE);

  private volatile boolean isReleased = false;

  public PipeMemoryBlock(long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  // Be sure not to cache this value for memory restriction process such as size limiting, for
  // it may fluctuate according to the system memory status
  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  boolean isReleased() {
    return isReleased;
  }

  void markAsReleased() {
    isReleased = true;

    // help gc
    pipeMemoryManager = null;
    monitoredObject = null;
    estimatedShrinkResult = null;
    shrinkMethod = null;
    estimatedExtendResult = null;
    extendMethod = null;
    minMemoryUsage = null;
    maxMemoryUsage = null;
    memoryUsageInBytes = null;
    objectLock = null;
  }

  // Never cache this monitoredObject, to avoid concurrent problem.
  public Object getMonitoredObject() {
    objectLock.readLock().lock();
    try {
      return monitoredObject.get();
    } finally {
      objectLock.readLock().unlock();
    }
  }

  public PipeMemoryBlock setEstimatedShrinkResult(LongUnaryOperator operator) {
    this.estimatedShrinkResult.set(operator);
    return this;
  }

  public PipeMemoryBlock setMonitoredObject(Object object) {
    monitoredObject.set(object);
    return this;
  }

  public PipeMemoryBlock setShrinkMethod(BiFunction<Long, Object, Long> shrinkMethod) {
    this.shrinkMethod.set(shrinkMethod);
    return this;
  }

  public PipeMemoryBlock setEstimatedExtendResult(LongUnaryOperator operator) {
    this.estimatedExtendResult.set(operator);
    return this;
  }

  public PipeMemoryBlock setExtendMethod(BiFunction<Long, Object, Long> extendMethod) {
    this.extendMethod.set(extendMethod);
    return this;
  }

  public PipeMemoryBlock setMinMemoryUsage(long minMemoryUsage) {
    this.minMemoryUsage.set(minMemoryUsage);
    return this;
  }

  public PipeMemoryBlock setMaxMemoryUsage(long maxMemoryUsage) {
    this.maxMemoryUsage.set(maxMemoryUsage);
    return this;
  }

  public boolean shrink() {
    objectLock.writeLock().lock();
    try {
      // We assume that sometimes the memory size can not be arbitrarily adjusted,
      // thus the new memory usage may not be "just" the min one, we first estimate the
      // memory size and compare it to the min size.
      if (memoryUsageInBytes.get() < minMemoryUsage.get()
          || estimatedShrinkResult.get() != null
              && estimatedShrinkResult.get().applyAsLong(memoryUsageInBytes.get())
                  < minMemoryUsage.get()) {
        return false;
      }
      if (shrinkMethod != null) {
        long oldMemorySizeInBytes = memoryUsageInBytes.get();
        memoryUsageInBytes.set(
            shrinkMethod.get().apply(memoryUsageInBytes.get(), monitoredObject.get()));
        // Update at last to avoid potential exception
        pipeMemoryManager.addUsedMemorySizeInBytes(memoryUsageInBytes.get() - oldMemorySizeInBytes);
        return true;
      }
    } finally {
      objectLock.writeLock().unlock();
    }
    return false;
  }

  public boolean extend() {
    objectLock.writeLock().lock();
    try {
      // We assume that Sometimes the memory size can not be arbitrarily adjusted,
      // thus the new memory usage may not be "just" the max one, we first estimate the
      // memory size and compare it to the max size.
      long estimatedNewValue = -1;
      if (estimatedExtendResult.get() != null) {
        estimatedNewValue = estimatedExtendResult.get().applyAsLong(memoryUsageInBytes.get());
      }
      if (memoryUsageInBytes.get() > maxMemoryUsage.get()
          || estimatedNewValue > maxMemoryUsage.get()
          || pipeMemoryManager.getTotalMemorySizeInBytes()
                  - memoryUsageInBytes.get()
                  + estimatedNewValue
              > PipeMemoryManager.TOTAL_MEMORY_SIZE_IN_BYTES) {
        return false;
      }
      if (extendMethod != null) {
        long oldMemorySizeInBytes = memoryUsageInBytes.get();
        memoryUsageInBytes.set(
            extendMethod.get().apply(memoryUsageInBytes.get(), monitoredObject.get()));
        pipeMemoryManager.addUsedMemorySizeInBytes(memoryUsageInBytes.get() - oldMemorySizeInBytes);
        return true;
      }
    } finally {
      objectLock.writeLock().unlock();
    }
    return false;
  }

  @Override
  public void close() {
    pipeMemoryManager.release(this);
  }
}
