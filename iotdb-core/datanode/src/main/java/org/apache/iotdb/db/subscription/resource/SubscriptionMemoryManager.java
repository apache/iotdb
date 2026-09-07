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

package org.apache.iotdb.db.subscription.resource;

import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class SubscriptionMemoryManager {

  private static final String MEMORY_BLOCK_NAME = "Subscription";

  private final IMemoryBlock memoryBlock;

  SubscriptionMemoryManager() {
    memoryBlock =
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getSubscriptionMemoryManager()
            .exactAllocate(MEMORY_BLOCK_NAME, MemoryBlockType.DYNAMIC);
  }

  @TestOnly
  public SubscriptionMemoryManager(final long totalMemorySizeInBytes) {
    memoryBlock =
        new AtomicLongMemoryBlock(
            MEMORY_BLOCK_NAME, null, totalMemorySizeInBytes, MemoryBlockType.DYNAMIC);
  }

  /**
   * Reserves memory for materialized subscription data.
   *
   * <p>A single entry larger than the whole budget is allowed only while the budget is otherwise
   * empty. This avoids permanently blocking progress while keeping the overrun bounded by one
   * consensus entry.
   */
  public synchronized boolean tryAllocate(final long sizeInBytes) {
    if (sizeInBytes <= 0L) {
      return true;
    }
    if (memoryBlock.allocate(sizeInBytes)) {
      return true;
    }
    if (memoryBlock.getTotalMemorySizeInBytes() > 0L
        && memoryBlock.getUsedMemoryInBytes() == 0L
        && sizeInBytes > memoryBlock.getTotalMemorySizeInBytes()) {
      memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
      return true;
    }
    return false;
  }

  public synchronized void release(final long sizeInBytes) {
    if (sizeInBytes > 0L) {
      memoryBlock.release(sizeInBytes);
    }
  }

  public long getTotalMemorySizeInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public long getUsedMemorySizeInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public long getFreeMemorySizeInBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }
}
