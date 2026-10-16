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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNodeMemoryConfig {
  public static final String PIPE_MEMORY_MANAGER_NAME = "Pipe";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeMemoryConfig.class);

  private long maxMemorySizeInBytes;

  private long pipeMemorySizeInBytes;

  private long freeMemorySizeInBytes;

  public void init(final TrimProperties properties) {
    final String memoryAllocateProportion =
        properties.getProperty("confignode_memory_proportion", null);

    maxMemorySizeInBytes = Runtime.getRuntime().maxMemory();
    pipeMemorySizeInBytes = maxMemorySizeInBytes / 10;
    freeMemorySizeInBytes = maxMemorySizeInBytes - pipeMemorySizeInBytes;

    if (memoryAllocateProportion != null) {
      final String[] proportions = memoryAllocateProportion.split(":");
      if (proportions.length >= 2) {
        int proportionSum = 0;
        for (final String proportion : proportions) {
          proportionSum += Integer.parseInt(proportion.trim());
        }

        if (proportionSum != 0) {
          pipeMemorySizeInBytes =
              maxMemorySizeInBytes * Integer.parseInt(proportions[0].trim()) / proportionSum;
          freeMemorySizeInBytes = maxMemorySizeInBytes - pipeMemorySizeInBytes;
        }
      } else {
        LOGGER.warn(
            "The parameter confignode_memory_proportion should be in the form of Pipe:Free, "
                + "but got {}. Use default value 1:9.",
            memoryAllocateProportion);
      }
    }

    LOGGER.info("initial ConfigNode allocateMemoryForPipe = {}", pipeMemorySizeInBytes);
    LOGGER.info("initial ConfigNode freeMemory = {}", freeMemorySizeInBytes);
  }

  public long getMaxMemorySizeInBytes() {
    return maxMemorySizeInBytes;
  }

  public long getPipeMemorySizeInBytes() {
    return pipeMemorySizeInBytes;
  }

  public long getFreeMemorySizeInBytes() {
    return freeMemorySizeInBytes;
  }
}
