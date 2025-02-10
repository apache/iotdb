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

import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IMemoryBlock implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IMemoryBlock.class);

  /** The memory manager that manages this memory block */
  protected MemoryManager memoryManager;

  /** The name of this memory block */
  protected String name;

  /** The type of this memory block */
  protected MemoryBlockType memoryBlockType;

  /** The flag that indicates whether this memory block is released */
  protected volatile boolean isReleased = false;

  /** The total memory size in byte of this memory block */
  protected long totalMemorySizeInBytes;

  /**
   * Force allocate memory without the limit of totalMemorySizeInBytes
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   * @return
   */
  public abstract long forceAllocate(final long sizeInByte);

  /**
   * Allocate memory managed by this memory block
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   */
  public abstract boolean allocate(final long sizeInByte);

  /**
   * Allocate memory managed by this memory block
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   * @param maxRatio the maximum ratio of memory can be allocated
   */
  public abstract boolean allocateIfSufficient(final long sizeInByte, final double maxRatio);

  /**
   * Allocate memory managed by this memory block until the required memory is available
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   * @param timeInMillis the time interval to wait for memory to be available
   */
  public abstract boolean allocateUntilAvailable(final long sizeInByte, long timeInMillis)
      throws InterruptedException;

  /**
   * Try to release memory managed by this memory block
   *
   * @param sizeInByte the size of memory to be released, should positive
   * @return
   */
  public abstract long release(final long sizeInByte);

  /**
   * Try to set memory usage in byte of this memory block (for test only)
   *
   * @param usedMemoryInBytes the memory usage in byte to be set
   */
  @TestOnly
  public abstract void setUsedMemoryInBytes(final long usedMemoryInBytes);

  /** Get the memory usage in byte of this memory block */
  public abstract long getUsedMemoryInBytes();

  /** Get the free memory in byte of this memory block */
  public abstract long getFreeMemoryInBytes();

  /** Get the name of memory block */
  public String getName() {
    return name;
  }

  /** Update maximum memory size in byte of this memory block */
  public void setTotalMemorySizeInBytes(final long totalMemorySizeInBytes) {
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
  }

  /** Get the maximum memory size in byte of this memory block */
  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  /** Get whether this memory block is released */
  public boolean isReleased() {
    return isReleased;
  }

  /** Mark this memory block as released */
  public void markAsReleased() {
    isReleased = true;
  }

  /** Get the type of this memory block */
  public MemoryBlockType getMemoryBlockType() {
    return memoryBlockType;
  }

  public void print(int index) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < index; i++) {
      sb.append("  ");
    }
    sb.append(this);
    LOGGER.info(sb.toString());
  }
}
