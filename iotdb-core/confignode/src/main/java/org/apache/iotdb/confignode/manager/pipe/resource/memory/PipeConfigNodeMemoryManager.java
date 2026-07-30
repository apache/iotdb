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

package org.apache.iotdb.confignode.manager.pipe.resource.memory;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.i18n.CommonMessages;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

public class PipeConfigNodeMemoryManager {

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final IMemoryBlock memoryBlock =
      ConfigNodeDescriptor.getInstance()
          .getMemoryConfig()
          .getPipeMemoryManager()
          .exactAllocate("Stream", MemoryBlockType.DYNAMIC);

  private long pipeLogReducerMemoryUsageInBytes;

  public synchronized long resizeLogReducerMemory(final long targetSizeInBytes) {
    final long nonNegativeTargetSizeInBytes = Math.max(0, targetSizeInBytes);
    if (pipeLogReducerMemoryUsageInBytes < nonNegativeTargetSizeInBytes) {
      final long deltaSizeInBytes = nonNegativeTargetSizeInBytes - pipeLogReducerMemoryUsageInBytes;
      if (memoryBlock.allocate(deltaSizeInBytes)) {
        pipeLogReducerMemoryUsageInBytes = nonNegativeTargetSizeInBytes;
      }
    } else if (pipeLogReducerMemoryUsageInBytes > nonNegativeTargetSizeInBytes) {
      final long deltaSizeInBytes = pipeLogReducerMemoryUsageInBytes - nonNegativeTargetSizeInBytes;
      releaseMemory(deltaSizeInBytes);
      pipeLogReducerMemoryUsageInBytes = nonNegativeTargetSizeInBytes;
    }
    return pipeLogReducerMemoryUsageInBytes;
  }

  public AutoCloseable tryAllocateReceiverMemory(final long requestedMemorySizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    final long nonNegativeRequestedMemorySizeInBytes = Math.max(0, requestedMemorySizeInBytes);
    if (nonNegativeRequestedMemorySizeInBytes == 0) {
      return () -> {};
    }

    forceAllocateWithRetry(nonNegativeRequestedMemorySizeInBytes);
    return () -> releaseMemory(nonNegativeRequestedMemorySizeInBytes);
  }

  private synchronized void forceAllocateWithRetry(final long sizeInBytes)
      throws PipeRuntimeOutOfMemoryCriticalException {
    final int memoryAllocateMaxRetries = PIPE_CONFIG.getPipeMemoryAllocateMaxRetries();
    for (int i = 0; i < memoryAllocateMaxRetries; ++i) {
      if (memoryBlock.allocate(sizeInBytes)) {
        return;
      }

      try {
        final long retryIntervalInMs = PIPE_CONFIG.getPipeMemoryAllocateRetryIntervalInMs();
        if (retryIntervalInMs > 0) {
          wait(retryIntervalInMs);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PipeRuntimeOutOfMemoryCriticalException(PipeMessages.TEMPORARILY_OUT_OF_MEMORY);
      }
    }

    throw new PipeRuntimeOutOfMemoryCriticalException(
        String.format(
            CommonMessages.EXCEPTION_EXACTALLOCATE_FAILED_ALLOCATE_MEMORY_AFTER_ARG_RETRIES_957A647B
                + CommonMessages
                    .EXCEPTION_TOTAL_MEMORY_SIZE_ARG_BYTES_USED_MEMORY_SIZE_ARG_BYTES_9FC9A9C6
                + CommonMessages.EXCEPTION_REQUESTED_MEMORY_SIZE_ARG_BYTES_E6340842,
            memoryAllocateMaxRetries,
            memoryBlock.getTotalMemorySizeInBytes(),
            memoryBlock.getUsedMemoryInBytes(),
            sizeInBytes));
  }

  private synchronized void releaseMemory(final long sizeInBytes) {
    memoryBlock.release(sizeInBytes);
    notifyAll();
  }

  public long getUsedMemorySizeInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public long getFreeMemorySizeInBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }

  public long getTotalMemorySizeInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }
}
