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

package org.apache.iotdb.confignode.manager.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PipeMetaSyncer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMetaSyncer.class);

  private static final ScheduledExecutorService SYNC_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_META_SYNC_SERVICE.getName());
  // TODO: make this configurable
  private static final long SYNC_INTERVAL_MINUTES = 3;

  private final ConfigManager configManager;

  private Future<?> metaSyncFuture;

  public PipeMetaSyncer(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public synchronized void start() {
    if (metaSyncFuture == null) {
      metaSyncFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              SYNC_EXECUTOR, this::sync, 0, SYNC_INTERVAL_MINUTES, TimeUnit.MINUTES);
      LOGGER.info("PipeMetaSyncer is started successfully.");
    }
  }

  private void sync() {
    final TSStatus status = configManager.getProcedureManager().pipeMetaSync();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          "PipeMetaSyncer meets error in syncing pipe meta, code: {}, message: {}",
          status.getCode(),
          status.getMessage());
    }
  }

  public synchronized void stop() {
    if (metaSyncFuture != null) {
      metaSyncFuture.cancel(false);
      metaSyncFuture = null;
      LOGGER.info("PipeMetaSyncer is stopped successfully.");
    }
  }
}
