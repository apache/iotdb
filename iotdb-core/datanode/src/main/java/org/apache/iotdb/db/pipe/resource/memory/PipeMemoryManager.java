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

  public PipeMemoryManager() {
    // do nothing
  }

  synchronized PipeMemoryBlock allocate(long size)
      throws PipeRuntimeException, InterruptedException {
    for (int i = 1; i <= maxRetries; i++) {
      if (totalMemorySizeInBytes - usedMemorySizeInBytes >= size) {
        PipeMemoryBlock block = new PipeMemoryBlock(size);
        usedMemorySizeInBytes += size;
        return block;
      }

      this.wait(retryIntervalInMs * i);
    }

    throw new PipeRuntimeCriticalException("Not enough memory for Pipe Memory...");
  }

  synchronized void release(PipeMemoryBlock block) {
    if (block == null || block.isReleased()) {
      return;
    }

    block.markAsReleased();
    usedMemorySizeInBytes -= block.getMemoryUsage();

    this.notify();
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
