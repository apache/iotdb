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
  private static final long MEMORY_CHECK_INTERVAL_IN_S = CONFIG.getMemoryCheckIntervalInS();
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private static final MemoryPeriodicalJobExecutor memoryPeriodicalJobExecutor =
      new MemoryPeriodicalJobExecutor(
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.MEMORY_PERIODICAL_JOB_EXECUTOR.getName()),
          20);

  @Override
  public void start() throws StartupException {
    memoryPeriodicalJobExecutor.start();

    LOGGER.info(
        "{} Enable automatic memory transfer with an interval of {} s",
        ENABLE_MEMORY_TRANSFER,
        MEMORY_CHECK_INTERVAL_IN_S);
    if (ENABLE_MEMORY_TRANSFER) {
      MemoryRuntimeAgent.getInstance()
          .registerPeriodicalJob(
              "GlobalMemoryManager#updateAllocate()",
              MemoryManager.global()::updateAllocate,
              MEMORY_CHECK_INTERVAL_IN_S);
    }

    isShutdown.set(false);
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
