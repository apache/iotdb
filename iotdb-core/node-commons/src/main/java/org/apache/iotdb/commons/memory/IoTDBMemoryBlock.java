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

package org.apache.iotdb.commons.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBMemoryBlock extends IIoTDBMemoryBlock{
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMemoryBlock.class);

  public IoTDBMemoryBlock(final IoTDBMemoryManager memoryManager, final long maxMemorySizeInByte) {
    this.memoryManager = memoryManager;
    this.maxMemorySizeInByte = maxMemorySizeInByte;
    this.memoryBlockType = IoTDBMemoryBlockType.NONE;
  }

  public IoTDBMemoryBlock(
      final IoTDBMemoryManager memoryManager,
      final long maxMemorySizeInByte,
      final IoTDBMemoryBlockType memoryBlockType) {
    this.memoryManager = memoryManager;
    this.maxMemorySizeInByte = maxMemorySizeInByte;
    this.memoryBlockType = memoryBlockType;
  }

  @Override
  public boolean useMemory(final long size) {
    if (size <= 0) {
      memoryUsageInBytes.addAndGet(-size);
      return true;
    } else {

      AtomicBoolean result = new AtomicBoolean(false);
      memoryUsageInBytes.updateAndGet(
          memorySize -> {
            if (size > maxMemorySizeInByte - memorySize) {
              LOGGER.debug(
                  "consensus memory limited. required: {}, used: {}, total: {}",
                  size,
                  memorySize,
                  maxMemorySizeInByte);
              result.set(false);
              return memorySize;
            } else {
              LOGGER.debug(
                  "{} add {} bytes, total memory size: {} bytes.",
                  Thread.currentThread().getName(),
                  size,
                  memorySize + size);
              result.set(true);
              return memorySize + size;
            }
          });
      return result.get();
    }
  }

  @Override
  public String toString() {
    return "IoTDBMemoryBlock{"
        + "memoryBlockType="
        + memoryBlockType
        + "maxMemorySizeInByte="
        + maxMemorySizeInByte
        + "memoryUsageInBytes="
        + memoryUsageInBytes
        + ", isReleased="
        + isReleased
        + '}';
  }

  @Override
  public void close() throws Exception {
    boolean isInterrupted = false;

    while (true) {
      try {
        if (lock.tryLock(50, TimeUnit.MICROSECONDS)) {
          try {
            memoryManager.release(this);
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
