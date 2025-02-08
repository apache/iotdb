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

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;

public class PipeMemoryBlock implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryBlock.class);

  private final PipeMemoryManager pipeMemoryManager = PipeDataNodeResourceManager.memory();

  private final ReentrantLock lock = new ReentrantLock();

  private final AtomicLong memoryUsageInBytes = new AtomicLong(0);

  private final AtomicReference<LongUnaryOperator> shrinkMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> shrinkCallback = new AtomicReference<>();
  private final AtomicReference<LongUnaryOperator> expandMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> expandCallback = new AtomicReference<>();

  private volatile boolean isReleased = false;

  public PipeMemoryBlock(final long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  public void setMemoryUsageInBytes(final long memoryUsageInBytes) {
    this.memoryUsageInBytes.set(memoryUsageInBytes);
  }

  public PipeMemoryBlock setShrinkMethod(final LongUnaryOperator shrinkMethod) {
    this.shrinkMethod.set(shrinkMethod);
    return this;
  }

  public PipeMemoryBlock setShrinkCallback(final BiConsumer<Long, Long> shrinkCallback) {
    this.shrinkCallback.set(shrinkCallback);
    return this;
  }

  public PipeMemoryBlock setExpandMethod(final LongUnaryOperator extendMethod) {
    this.expandMethod.set(extendMethod);
    return this;
  }

  public PipeMemoryBlock setExpandCallback(final BiConsumer<Long, Long> expandCallback) {
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
    final long newMemorySizeInBytes = shrinkMethod.get().applyAsLong(memoryUsageInBytes.get());

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
    final long newMemorySizeInBytes = expandMethod.get().applyAsLong(memoryUsageInBytes.get());

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
  public String toString() {
    return "PipeMemoryBlock{"
        + "memoryUsageInBytes="
        + memoryUsageInBytes.get()
        + ", isReleased="
        + isReleased
        + '}';
  }

  @Override
  public void close() {
    boolean isInterrupted = false;

    while (true) {
      try {
        if (lock.tryLock(50, TimeUnit.MICROSECONDS)) {
          try {
            pipeMemoryManager.release(this);
            if (isInterrupted) {
              LOGGER.warn("{} is released after thread interruption.", this);
            }
            break;
          } finally {
            lock.unlock();
          }
        }
      } catch (final InterruptedException e) {
        // Each time the close task is run, it means that the interrupt status left by the previous
        // tryLock does not need to be retained. Otherwise, it will lead to an infinite loop.
        isInterrupted = true;
        LOGGER.warn("Interrupted while waiting for the lock.", e);
      }
    }

    // Restore the interrupt status of the current thread
    if (isInterrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
