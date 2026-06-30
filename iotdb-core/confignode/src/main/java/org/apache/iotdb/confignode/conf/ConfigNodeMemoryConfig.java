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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.memory.MemoryConfig;
import org.apache.iotdb.commons.memory.MemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNodeMemoryConfig {
  public static final String PIPE_MEMORY_MANAGER_NAME = "Pipe";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeMemoryConfig.class);

  /** The memory manager of on heap. */
  private MemoryManager onHeapMemoryManager;

  /** Memory manager for the pipe. */
  private MemoryManager pipeMemoryManager;

  public void init(final TrimProperties properties) {
    String memoryAllocateProportion = properties.getProperty("confignode_memory_proportion", null);
    if (memoryAllocateProportion == null) {
      memoryAllocateProportion = properties.getProperty("config_node_memory_proportion", null);
      if (memoryAllocateProportion != null) {
        LOGGER.warn(
            "The parameter config_node_memory_proportion is deprecated, "
                + "please use confignode_memory_proportion instead.");
      }
    }

    final long maxMemoryAvailable = Runtime.getRuntime().maxMemory();
    long pipeMemorySize = maxMemoryAvailable / 10;
    long freeMemorySize = maxMemoryAvailable - pipeMemorySize;

    if (memoryAllocateProportion != null) {
      final String[] proportions = memoryAllocateProportion.split(":");
      if (proportions.length >= 2) {
        int proportionSum = 0;
        for (final String proportion : proportions) {
          proportionSum += Integer.parseInt(proportion.trim());
        }

        if (proportionSum != 0) {
          pipeMemorySize =
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum;
          freeMemorySize = maxMemoryAvailable - pipeMemorySize;
        }
      } else {
        LOGGER.warn(
            "The parameter confignode_memory_proportion should be in the form of Pipe:Free, "
                + "but got {}. Use default value 1:9.",
            memoryAllocateProportion);
      }
    }

    onHeapMemoryManager =
        MemoryConfig.global().getOrCreateMemoryManager("ConfigNodeOnHeap", maxMemoryAvailable);
    pipeMemoryManager =
        onHeapMemoryManager.getOrCreateMemoryManager(PIPE_MEMORY_MANAGER_NAME, pipeMemorySize);
    // Keep the rest of ConfigNode heap unconnected for now. The memory framework currently only
    // serves PipePeriodicalLogReducer on ConfigNode.

    LOGGER.info(
        "initial ConfigNode allocateMemoryForPipe = {}",
        pipeMemoryManager.getTotalMemorySizeInBytes());
    LOGGER.info("initial ConfigNode freeMemory = {}", freeMemorySize);
  }

  public MemoryManager getOnHeapMemoryManager() {
    return onHeapMemoryManager;
  }

  public MemoryManager getPipeMemoryManager() {
    return pipeMemoryManager;
  }
}
