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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  private static final int MEMORY_ALLOCATE_MAX_RETRIES =
      PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
  private static final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS =
      PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs();

  private static final long TOTAL_MEMORY_SIZE_IN_BYTES =
      IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForPipe();
  private long usedMemorySizeInBytes = 0;

  public synchronized PipeMemoryBlock allocate(long sizeInBytes)
      throws PipeRuntimeException, InterruptedException {
    for (int i = 1; i <= MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
        usedMemorySizeInBytes += sizeInBytes;
        return new PipeMemoryBlock(sizeInBytes);
      }

      try {
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("allocate: interrupted while waiting for available memory", e);
      }
    }

    throw new PipeRuntimeCriticalException(
        String.format(
            "failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  public synchronized void release(PipeMemoryBlock block) {
    if (block == null || block.isReleased()) {
      return;
    }

    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES;
  }

  public double getMemoryUsage() {
    return (double) usedMemorySizeInBytes / TOTAL_MEMORY_SIZE_IN_BYTES;
  }
}
