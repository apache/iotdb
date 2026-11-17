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

package org.apache.iotdb.db.storageengine.load.memory;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A sub-block of another LOAD memory block: it caps what the part of a task it stands for may hold,
 * and accounts every byte on its parent as well, so the parent keeps seeing the whole footprint.
 *
 * <p>Different from a child of {@code MemoryManager}: nothing is reserved here, the parent keeps
 * handing its memory out to whoever asks for it, and the sub-block only answers whether the part it
 * stands for fits both its own cap and what is left of the parent.
 */
class LoadTsFileSubMemoryBlock extends LoadTsFileAbstractMemoryBlock {

  private final LoadTsFileAbstractMemoryBlock parent;
  private final long maxMemorySizeInBytes;
  private final AtomicLong memoryUsageInBytes = new AtomicLong(0);

  LoadTsFileSubMemoryBlock(
      final LoadTsFileAbstractMemoryBlock parent, final long maxMemorySizeInBytes) {
    if (maxMemorySizeInBytes <= 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_LOAD_INVALID_MEMORY_SIZE_D_BYTES_MUST_BE_POSITIVE_D6586ED3,
              maxMemorySizeInBytes));
    }
    this.parent = parent;
    this.maxMemorySizeInBytes = maxMemorySizeInBytes;
  }

  @Override
  public boolean hasEnoughMemory(final long memoryTobeAddedInBytes) {
    return memoryUsageInBytes.get() + memoryTobeAddedInBytes <= maxMemorySizeInBytes
        && parent.hasEnoughMemory(memoryTobeAddedInBytes);
  }

  @Override
  public void addMemoryUsage(final long memoryInBytes) {
    // May temporarily exceed the max size, like the block it accounts on.
    memoryUsageInBytes.addAndGet(memoryInBytes);
    parent.addMemoryUsage(memoryInBytes);
  }

  @Override
  public void reduceMemoryUsage(final long memoryInBytes) {
    memoryUsageInBytes.addAndGet(-memoryInBytes);
    parent.reduceMemoryUsage(memoryInBytes);
  }

  @Override
  public void forceResize(final long newSizeInBytes) {
    throw new UnsupportedOperationException(
        StorageEngineMessages
            .STORAGE_EXCEPTION_SETTOTALMEMORYSIZEINBYTES_IS_NOT_SUPPORTED_FOR_LOADTSFILESUBMEMORYBLOCK_58ABABBB);
  }

  @Override
  long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  @Override
  protected void releaseAllMemory() {
    final long memoryUsage = memoryUsageInBytes.getAndSet(0);
    if (memoryUsage > 0) {
      parent.reduceMemoryUsage(memoryUsage);
    }
  }

  @Override
  public String toString() {
    return "LoadTsFileSubMemoryBlock{"
        + "maxMemorySizeInBytes="
        + maxMemorySizeInBytes
        + ", usedMemoryInBytes="
        + memoryUsageInBytes.get()
        + '}';
  }
}
