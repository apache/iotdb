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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryControl;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryManager;

public class MemoryConfig {
  private static final String AUTO_RESIZING_BUFFER_MEMORY_MANAGER_NAME = "AutoResizingBuffer";
  private static final String AUTO_RESIZING_BUFFER_MEMORY_BLOCK_NAME = "AutoResizingBufferBlock";

  private final MemoryManager globalMemoryManager =
      new MemoryManager("GlobalMemoryManager", null, Runtime.getRuntime().totalMemory());

  private MemoryConfig() {
    initAutoResizingBufferMemoryControl();
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
          private IMemoryBlock autoResizingBufferMemoryBlock;

          @Override
          public synchronized boolean allocate(long sizeInBytes) {
            if (isAutoResizingBufferMemoryControlDisabled()) {
              return true;
            }
            return getAutoResizingBufferMemoryBlock().allocate(sizeInBytes);
          }

          @Override
          public synchronized void release(long sizeInBytes) {
            if (isAutoResizingBufferMemoryControlDisabled()) {
              return;
            }
            if (autoResizingBufferMemoryBlock != null
                && !autoResizingBufferMemoryBlock.isReleased()) {
              autoResizingBufferMemoryBlock.release(sizeInBytes);
            }
          }

          private boolean isAutoResizingBufferMemoryControlDisabled() {
            return CommonDescriptor.getInstance()
                    .getConfig()
                    .getAutoResizingBufferMemoryProportion()
                <= 0;
          }

          private IMemoryBlock getAutoResizingBufferMemoryBlock() {
            if (autoResizingBufferMemoryBlock == null
                || autoResizingBufferMemoryBlock.isReleased()) {
              long autoResizingBufferMemorySize =
                  (long)
                      (globalMemoryManager.getTotalMemorySizeInBytes()
                          * CommonDescriptor.getInstance()
                              .getConfig()
                              .getAutoResizingBufferMemoryProportion());
              MemoryManager autoResizingBufferMemoryManager =
                  globalMemoryManager.getOrCreateMemoryManager(
                      AUTO_RESIZING_BUFFER_MEMORY_MANAGER_NAME, autoResizingBufferMemorySize, true);
              autoResizingBufferMemoryBlock =
                  autoResizingBufferMemoryManager.exactAllocate(
                      AUTO_RESIZING_BUFFER_MEMORY_BLOCK_NAME,
                      autoResizingBufferMemorySize,
                      MemoryBlockType.DYNAMIC);
            }
            return autoResizingBufferMemoryBlock;
          }
        });
  }
}
