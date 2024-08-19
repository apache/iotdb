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
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ActiveLoadListeningDirsCountExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ActiveLoadListeningDirsCountExecutor.class);

  private static final ScheduledExecutorService DIRS_SCAN_JOB_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.ACTIVE_LOAD_DIRS_COUNT.name());
  private static final long MIN_SCAN_INTERVAL_SECONDS =
      IoTDBDescriptor.getInstance().getConfig().getLoadActiveListeningCheckIntervalSeconds();

  private long rounds;
  private Future<?> dirsScanJobFuture;

  private final List<Pair<WrappedRunnable, Long>> fileCountPeriodicalJobs =
      new CopyOnWriteArrayList<>();

  private ActiveLoadListeningDirsCountExecutor() {}

  public void register(Runnable runnable) {
    fileCountPeriodicalJobs.add(
        new Pair<>(
            new WrappedRunnable() {
              @Override
              public void runMayThrow() {
                try {
                  runnable.run();
                } catch (Exception e) {
                  LOGGER.warn("active load file metric job failed.", e);
                }
              }
            },
            Math.max(MIN_SCAN_INTERVAL_SECONDS, 1)));
  }

  public synchronized void start() {
    if (dirsScanJobFuture == null) {
      rounds = 0;

      dirsScanJobFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              DIRS_SCAN_JOB_EXECUTOR,
              this::execute,
              MIN_SCAN_INTERVAL_SECONDS,
              MIN_SCAN_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info(
          "Active load file metric job started. Scan interval: {}s.", MIN_SCAN_INTERVAL_SECONDS);
    }
  }

  private void execute() {
    ++rounds;

    for (final Pair<WrappedRunnable, Long> periodicalJob : fileCountPeriodicalJobs) {
      if (rounds % periodicalJob.right == 0) {
        periodicalJob.left.run();
      }
    }
  }

  public synchronized void stop() {
    if (dirsScanJobFuture != null) {
      dirsScanJobFuture.cancel(false);
      dirsScanJobFuture = null;
      LOGGER.info("Active load file metric periodical jobs executor is stopped successfully.");
    }
  }

  @TestOnly
  public void clear() {
    fileCountPeriodicalJobs.clear();
    LOGGER.info("All Active load file metric periodical jobs are cleared successfully.");
  }

  private static class ActiveLoadListeningDirsCountExecutorHolder {
    private static final ActiveLoadListeningDirsCountExecutor INSTANCE =
        new ActiveLoadListeningDirsCountExecutor();

    private ActiveLoadListeningDirsCountExecutorHolder() {}
  }

  public static ActiveLoadListeningDirsCountExecutor getInstance() {
    return ActiveLoadListeningDirsCountExecutorHolder.INSTANCE;
  }
}
