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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class IMemoryBlock implements AutoCloseable {
  /** The memory manager that manages this memory block */
  protected MemoryManager memoryManager;

  /** The reentrant lock of memory block */
  protected final ReentrantLock lock = new ReentrantLock();

  /** The name of this memory block */
  protected String name;

  /** The type of this memory block */
  protected MemoryBlockType memoryBlockType;

  /** The flag that indicates whether this memory block is released */
  protected volatile boolean isReleased = false;

  /** The maximum memory size in byte of this memory block */
  protected long maxMemorySizeInByte = 0;

  /** The memory usage in byte of this memory block */
  protected final AtomicLong memoryUsageInBytes = new AtomicLong(0);

  /**
   * Allocate memory managed by this memory block
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   * @return
   */
  public abstract boolean allocate(final long sizeInByte);

  /**
   * Allocate memory managed by this memory block until the required memory is available
   *
   * @param sizeInByte the size of memory to be allocated, should positive
   * @param timeInterval the time interval to wait for memory to be available
   */
  public abstract boolean allocateUntilAvailable(final long sizeInByte, long timeInterval)
      throws InterruptedException;

  /**
   * Try to record memory managed by this memory block
   *
   * @param sizeInByte the size of memory to be released, should positive
   */
  public abstract void release(final long sizeInByte);

  /**
   * Try to set memory usage in byte of this memory block (for test only)
   *
   * @param memoryUsageInBytes the memory usage in byte to be set
   */
  @TestOnly
  public abstract void setMemoryUsageInBytes(final long memoryUsageInBytes);

  /** Update maximum memory size in byte of this memory block */
  public void setMaxMemorySizeInByte(final long maxMemorySizeInByte) {
    this.maxMemorySizeInByte = maxMemorySizeInByte;
  }

  /** Get the maximum memory size in byte of this memory block */
  public long getMaxMemorySizeInByte() {
    return maxMemorySizeInByte;
  }

  /** Get the memory usage in byte of this memory block */
  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
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
}
