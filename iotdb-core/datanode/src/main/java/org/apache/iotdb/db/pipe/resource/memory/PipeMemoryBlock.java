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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class PipeMemoryBlock implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryBlock.class);

  private final PipeMemoryManager pipeMemoryManager = PipeResourceManager.memory();

  private final ReentrantLock lock = new ReentrantLock();

  private final AtomicLong memoryUsageInBytes = new AtomicLong(0);

  private final AtomicReference<Function<Long, Long>> shrinkMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> shrinkCallback = new AtomicReference<>();
  private final AtomicReference<Function<Long, Long>> expandMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> expandCallback = new AtomicReference<>();

  private volatile boolean isReleased = false;

  public PipeMemoryBlock(long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  public void setMemoryUsageInBytes(long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  public PipeMemoryBlock setShrinkMethod(Function<Long, Long> shrinkMethod) {
    this.shrinkMethod.set(shrinkMethod);
    return this;
  }

  public PipeMemoryBlock setShrinkCallback(BiConsumer<Long, Long> shrinkCallback) {
    this.shrinkCallback.set(shrinkCallback);
    return this;
  }

  public PipeMemoryBlock setExpandMethod(Function<Long, Long> extendMethod) {
    this.expandMethod.set(extendMethod);
    return this;
  }

  public PipeMemoryBlock setExpandCallback(BiConsumer<Long, Long> expandCallback) {
    this.expandCallback.set(expandCallback);
    return this;
  }

  boolean shrink() {
    if (lock.tryLock()) {
      try {
        return doShrink();
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  private boolean doShrink() {
    if (shrinkMethod.get() == null) {
      return false;
    }

    final long oldMemorySizeInBytes = memoryUsageInBytes.get();
    final long newMemorySizeInBytes = shrinkMethod.get().apply(memoryUsageInBytes.get());

    final long memoryInBytesCanBeReleased = oldMemorySizeInBytes - newMemorySizeInBytes;
    if (memoryInBytesCanBeReleased <= 0
        || !pipeMemoryManager.release(this, memoryInBytesCanBeReleased)) {
      return false;
    }

    if (shrinkCallback.get() != null) {
      try {
        shrinkCallback.get().accept(oldMemorySizeInBytes, newMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn("Failed to execute the shrink callback.", e);
      }
    }
    return true;
  }

  boolean expand() {
    if (lock.tryLock()) {
      try {
        return doExpand();
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  private boolean doExpand() {
    if (expandMethod.get() == null) {
      return false;
    }

    final long oldMemorySizeInBytes = memoryUsageInBytes.get();
    final long newMemorySizeInBytes = expandMethod.get().apply(memoryUsageInBytes.get());

    final long memoryInBytesNeededToBeAllocated = newMemorySizeInBytes - oldMemorySizeInBytes;
    if (memoryInBytesNeededToBeAllocated <= 0
        || !pipeMemoryManager.tryAllocate(this, memoryInBytesNeededToBeAllocated)) {
      return false;
    }

    if (expandCallback.get() != null) {
      try {
        expandCallback.get().accept(oldMemorySizeInBytes, newMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn("Failed to execute the expand callback.", e);
      }
    }
    return true;
  }

  boolean isReleased() {
    return isReleased;
  }

  void markAsReleased() {
    isReleased = true;
  }

  @Override
  public void close() {
    while (true) {
      try {
        if (lock.tryLock(50, TimeUnit.MICROSECONDS)) {
          try {
            pipeMemoryManager.release(this);
            return;
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted while waiting for the lock.", e);
      }
    }
  }
}
