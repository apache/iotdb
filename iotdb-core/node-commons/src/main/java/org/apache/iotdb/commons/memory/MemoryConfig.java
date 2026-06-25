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

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryControl;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryManager;

public class MemoryConfig {
  private static final String AUTO_RESIZING_BUFFER_MEMORY_MANAGER_NAME = "AutoResizingBuffer";
  private static final String AUTO_RESIZING_BUFFER_MEMORY_BLOCK_NAME = "AutoResizingBufferBlock";

  private final MemoryManager globalMemoryManager =
      new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

  private MemoryManager autoResizingBufferMemoryManagerParent;
  private IMemoryBlock autoResizingBufferMemoryBlock;
  private boolean isAutoResizingBufferMemoryControlEnabled;

  private MemoryConfig() {
    initAutoResizingBufferMemoryControl();
    MetricService.getInstance().addMetricSet(new AutoResizingBufferMemoryMetrics(this));
  }

  public static MemoryManager global() {
    return MemoryConfigHolder.INSTANCE.globalMemoryManager;
  }

  public static MemoryConfig getInstance() {
    return MemoryConfigHolder.INSTANCE;
  }

  private static class MemoryConfigHolder {
    private static final MemoryConfig INSTANCE = new MemoryConfig();

    private MemoryConfigHolder() {}
  }

  private void initAutoResizingBufferMemoryControl() {
    AutoResizingBufferMemoryManager.setMemoryControl(
        new AutoResizingBufferMemoryControl() {
          @Override
          public synchronized boolean allocate(long sizeInBytes) {
            IMemoryBlock memoryBlock = getAutoResizingBufferMemoryBlock();
            if (memoryBlock == null) {
              return true;
            }
            return memoryBlock.allocate(sizeInBytes);
          }

          @Override
          public synchronized void release(long sizeInBytes) {
            IMemoryBlock memoryBlock = getAutoResizingBufferMemoryBlock();
            if (memoryBlock != null) {
              memoryBlock.release(sizeInBytes);
            }
          }
        });
  }

  public synchronized void setAutoResizingBufferMemoryControl(
      MemoryManager parentMemoryManager, long memorySizeInBytes) {
    if (autoResizingBufferMemoryManagerParent != null) {
      autoResizingBufferMemoryManagerParent.releaseChildMemoryManager(
          AUTO_RESIZING_BUFFER_MEMORY_MANAGER_NAME);
    }
    autoResizingBufferMemoryManagerParent = parentMemoryManager;
    autoResizingBufferMemoryBlock = null;

    if (memorySizeInBytes <= 0) {
      isAutoResizingBufferMemoryControlEnabled = false;
      return;
    }

    MemoryManager autoResizingBufferMemoryManager =
        parentMemoryManager.getOrCreateMemoryManager(
            AUTO_RESIZING_BUFFER_MEMORY_MANAGER_NAME, memorySizeInBytes, true);
    if (autoResizingBufferMemoryManager == null) {
      isAutoResizingBufferMemoryControlEnabled = false;
      return;
    }
    autoResizingBufferMemoryBlock =
        autoResizingBufferMemoryManager.exactAllocate(
            AUTO_RESIZING_BUFFER_MEMORY_BLOCK_NAME, memorySizeInBytes, MemoryBlockType.DYNAMIC);
    isAutoResizingBufferMemoryControlEnabled = true;
  }

  private synchronized IMemoryBlock getAutoResizingBufferMemoryBlock() {
    if (!isAutoResizingBufferMemoryControlEnabled
        || autoResizingBufferMemoryBlock == null
        || autoResizingBufferMemoryBlock.isReleased()) {
      return null;
    }
    return autoResizingBufferMemoryBlock;
  }

  public synchronized long getAutoResizingBufferMemoryTotalSizeInBytes() {
    IMemoryBlock memoryBlock = getAutoResizingBufferMemoryBlock();
    return memoryBlock == null ? 0 : memoryBlock.getTotalMemorySizeInBytes();
  }

  public synchronized long getAutoResizingBufferMemoryUsedSizeInBytes() {
    IMemoryBlock memoryBlock = getAutoResizingBufferMemoryBlock();
    return memoryBlock == null ? 0 : memoryBlock.getUsedMemoryInBytes();
  }

  public synchronized long getAutoResizingBufferMemoryAvailableSizeInBytes() {
    IMemoryBlock memoryBlock = getAutoResizingBufferMemoryBlock();
    return memoryBlock == null ? 0 : memoryBlock.getFreeMemoryInBytes();
  }
}
