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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryRuntimeAgent implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryRuntimeAgent.class);
  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final boolean ENABLE_MEMORY_TRANSFER = CONFIG.isEnableMemoryTransfer();
  private static final boolean ENABLE_MEMORY_ADAPT = CONFIG.isEnableMemoryAdapt();
  private static final long MEMORY_CHECK_INTERVAL_IN_S = CONFIG.getMemoryCheckIntervalInS();

  private static final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private static final MemoryPeriodicalJobExecutor memoryPeriodicalJobExecutor =
      new MemoryPeriodicalJobExecutor(
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.MEMORY_PERIODICAL_JOB_EXECUTOR.getName()),
          MEMORY_CHECK_INTERVAL_IN_S);

  @Override
  public void start() throws StartupException {
    memoryPeriodicalJobExecutor.start();

    // Try to transfer memory between memory modules periodically
    if (ENABLE_MEMORY_TRANSFER) {
      LOGGER.info(
          "Enable automatic memory transfer with an interval of {} s", MEMORY_CHECK_INTERVAL_IN_S);
      MemoryRuntimeAgent.getInstance()
          .registerPeriodicalJob(
              "GlobalMemoryManager#updateAllocate()",
              this::updateMemoryAllocate,
              MEMORY_CHECK_INTERVAL_IN_S);
    }
    // Try to adapt total memory size according to the JVM total memory size
    if (ENABLE_MEMORY_ADAPT) {
      LOGGER.info(
          "Enable automatic memory adapt with an interval of {} s", MEMORY_CHECK_INTERVAL_IN_S);
      MemoryRuntimeAgent.getInstance()
          .registerPeriodicalJob(
              "MemoryRuntimeAgent#adaptTotalMemory()",
              this::adaptTotalMemory,
              MEMORY_CHECK_INTERVAL_IN_S);
    }

    isShutdown.set(false);
  }

  private void updateMemoryAllocate() {
    MemoryConfig.global().getMemoryManager("OnHeap").updateAllocate();
  }

  private void adaptTotalMemory() {
    long totalMemory = Runtime.getRuntime().totalMemory();
    MemoryManager memoryManager = MemoryConfig.global().getMemoryManager("OnHeap");
    if (memoryManager != null) {
      long originMemorySize = memoryManager.getInitialAllocatedMemorySizeInBytes();
      if (originMemorySize != totalMemory) {
        LOGGER.info("Total memory size changed from {} to {}", originMemorySize, totalMemory);
        memoryManager.reAllocateInitialMemoryAccordingToRatio(
            (double) totalMemory / originMemorySize);
      }
    }
  }

  @Override
  public void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    memoryPeriodicalJobExecutor.stop();
  }

  public void registerPeriodicalJob(String id, Runnable periodicalJob, long intervalInSeconds) {
    memoryPeriodicalJobExecutor.register(id, periodicalJob, intervalInSeconds);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MEMORY_RUNTIME_AGENT;
  }

  private static class MemoryRuntimeAgentHolder {
    private static final MemoryRuntimeAgent HANDLE = new MemoryRuntimeAgent();
  }

  public static MemoryRuntimeAgent getInstance() {
    return MemoryRuntimeAgentHolder.HANDLE;
  }
}
