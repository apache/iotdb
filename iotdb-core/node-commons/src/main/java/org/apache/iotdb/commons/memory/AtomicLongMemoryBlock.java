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
import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongMemoryBlock extends IMemoryBlock {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicLongMemoryBlock.class);

  /** The memory usage in byte of this memory block */
  protected final AtomicLong usedMemoryInBytes = new AtomicLong(0);

  public AtomicLongMemoryBlock(
      final String name, final MemoryManager memoryManager, final long maxMemorySizeInByte) {
    this.name = name;
    this.memoryManager = memoryManager;
    this.totalMemorySizeInBytes = maxMemorySizeInByte;
    this.memoryBlockType = MemoryBlockType.NONE;
  }

  public AtomicLongMemoryBlock(
      final String name,
      final MemoryManager memoryManager,
      final long maxMemorySizeInByte,
      final MemoryBlockType memoryBlockType) {
    this.name = name;
    this.memoryManager = memoryManager;
    this.totalMemorySizeInBytes = maxMemorySizeInByte;
    this.memoryBlockType = memoryBlockType;
  }

  @Override
  public long forceAllocateWithoutLimitation(long sizeInBytes) {
    return usedMemoryInBytes.addAndGet(sizeInBytes);
  }

  @Override
  public boolean allocate(long sizeInBytes) {
    AtomicBoolean result = new AtomicBoolean(false);
    usedMemoryInBytes.updateAndGet(
        memCost -> {
          if (memCost + sizeInBytes > totalMemorySizeInBytes) {
            return memCost;
          }
          result.set(true);
          return memCost + sizeInBytes;
        });
    return result.get();
  }

  @Override
  public boolean allocateIfSufficient(final long sizeInBytes, final double maxRatio) {
    AtomicBoolean result = new AtomicBoolean(false);
    usedMemoryInBytes.updateAndGet(
        memCost -> {
          if (memCost + sizeInBytes > totalMemorySizeInBytes * maxRatio) {
            return memCost;
          }
          result.set(true);
          return memCost + sizeInBytes;
        });
    return result.get();
  }

  @Override
  public boolean allocateUntilAvailable(long sizeInBytes, long retryIntervalInMillis)
      throws InterruptedException {
    long originSize = usedMemoryInBytes.get();
    while (true) {
      boolean canUpdate = originSize + sizeInBytes <= totalMemorySizeInBytes;
      if (canUpdate && usedMemoryInBytes.compareAndSet(originSize, originSize + sizeInBytes)) {
        break;
      }
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(retryIntervalInMillis));
      originSize = usedMemoryInBytes.get();
    }
    return true;
  }

  @Override
  public long release(long sizeInBytes) {
    return usedMemoryInBytes.updateAndGet(
        memCost -> {
          if (sizeInBytes > memCost) {
            LOGGER.warn(
                "The memory cost to be released is larger than the memory cost of memory block {}",
                this);
            return 0;
          }
          return memCost - sizeInBytes;
        });
  }

  @Override
  public void setUsedMemoryInBytes(long usedMemoryInBytes) {
    this.usedMemoryInBytes.set(usedMemoryInBytes);
  }

  @Override
  public long getUsedMemoryInBytes() {
    return usedMemoryInBytes.get();
  }

  @Override
  /** Get the free memory in byte of this memory block */
  public long getFreeMemoryInBytes() {
    return totalMemorySizeInBytes - usedMemoryInBytes.get();
  }

  @Override
  public String toString() {
    return "IoTDBMemoryBlock{"
        + "name="
        + name
        + ", isReleased="
        + isReleased
        + ", memoryBlockType="
        + memoryBlockType
        + ", totalMemorySizeInBytes="
        + totalMemorySizeInBytes
        + ", usedMemoryInBytes="
        + usedMemoryInBytes
        + '}';
  }

  @Override
  public void close() throws Exception {
    if (memoryManager != null) {
      memoryManager.release(this);
    }
  }
}
