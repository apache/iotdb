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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeMemoryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryManager.class);

  private final long totalMemorySizeInBytes =
      PipeConfig.getInstance().getPipeTotalMemorySizeInBytes();
  private long usedMemorySizeInBytes = 0;

  private final int maxRetries = PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries();
  private final long retryIntervalInMs =
      PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs();

  public PipeMemoryManager() {}

  synchronized PipeMemoryBlock allocate(long size) throws PipeRuntimeException {
    for (int i = 0; i < maxRetries; i++) {
      if (totalMemorySizeInBytes - getUsedMemorySizeInBytes() >= size) {
        PipeMemoryBlock block = new PipeMemoryBlock(size);
        usedMemorySizeInBytes += size;
        return block;
      } else {
        LOGGER.warn(
            "Not enough memory for allocating PipeMemoryBlock, total memory size: {}, used memory size: {}, requested memory size: {}, retrying...",
            totalMemorySizeInBytes,
            getUsedMemorySizeInBytes(),
            size);
        try {
          Thread.sleep(retryIntervalInMs * i);
        } catch (Exception ignored) {
          // do nothing
        }
      }
    }
    throw new PipeRuntimeCriticalException(
        String.format(
            "Not enough memory for Pipe Memory, total memory size: %d, used memory size: %d, requested memory size: %d",
            totalMemorySizeInBytes, getUsedMemorySizeInBytes(), size));
  }

  synchronized void release(PipeMemoryBlock block) {
    if (block == null) {
      LOGGER.warn("No memory block found, skipping...");
      return;
    }
    usedMemorySizeInBytes -= block.getMemoryUsage();
  }

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return totalMemorySizeInBytes;
  }

  public void clean() {
    usedMemorySizeInBytes = 0;
  }
}
