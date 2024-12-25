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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LoadTsFileAbstractMemoryBlock implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAbstractMemoryBlock.class);
  protected static final LoadTsFileMemoryManager MEMORY_MANAGER =
      LoadTsFileMemoryManager.getInstance();

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public boolean hasEnoughMemory() {
    return hasEnoughMemory(0L);
  }

  public abstract boolean hasEnoughMemory(long memoryTobeAddedInBytes);

  public abstract void addMemoryUsage(long memoryInBytes);

  public abstract void reduceMemoryUsage(long memoryInBytes);

  abstract long getMemoryUsageInBytes();

  public abstract void forceResize(long newSizeInBytes);

  /**
   * Release all memory of this block.
   *
   * <p>NOTE: This method should be called only by {@link LoadTsFileAbstractMemoryBlock#close()}.
   */
  protected abstract void releaseAllMemory();

  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      try {
        releaseAllMemory();
      } catch (Exception e) {
        LOGGER.error("Release memory block {} failed", this, e);
      }
    }
  }
}
