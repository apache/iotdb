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

package org.apache.iotdb.db.queryengine.execution.load.active;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ActiveLoadDirScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveLoadDirScanner.class);

  private static final ScheduledExecutorService DIR_SCAN_JOB_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.ACTIVE_LOAD_DIR_SCANNER.getName());

  private static final long MIN_INTERVAL_SECONDS =
      IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningCheckIntervalSeconds();

  private final ActiveLoadTsFileLoader activeLoadTsFileLoader;

  private Future<?> dirScanJobFuture;

  public ActiveLoadDirScanner(final ActiveLoadTsFileLoader activeLoadTsFileLoader) {
    this.activeLoadTsFileLoader = activeLoadTsFileLoader;
  }

  public synchronized void start() {
    if (dirScanJobFuture == null) {
      dirScanJobFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              DIR_SCAN_JOB_EXECUTOR,
              this::scanSafely,
              MIN_INTERVAL_SECONDS,
              MIN_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Active load dir scanner started.");
    }
  }

  public synchronized void stop() {
    if (dirScanJobFuture != null) {
      dirScanJobFuture.cancel(false);
      dirScanJobFuture = null;
      LOGGER.info("Active load dir scanner stopped.");
    }
  }

  private void scanSafely() {
    try {
      scan();
    } catch (Exception e) {
      LOGGER.warn("Error occurred during active load dir scanning.", e);
    }
  }

  private void scan() {
    // do something
  }
}
