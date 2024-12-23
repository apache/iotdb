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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class ActiveLoadScheduledExecutorService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ActiveLoadScheduledExecutorService.class);

  protected static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final long MIN_EXECUTION_INTERVAL_SECONDS =
      IOTDB_CONFIG.getLoadActiveListeningCheckIntervalSeconds();
  private final ScheduledExecutorService scheduledExecutorService;
  private Future<?> future;

  private final List<Pair<WrappedRunnable, Long>> jobs = new CopyOnWriteArrayList<>();

  protected ActiveLoadScheduledExecutorService(final ThreadName threadName) {
    scheduledExecutorService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(threadName.name());
  }

  public void register(Runnable runnable) {
    jobs.add(
        new Pair<>(
            new WrappedRunnable() {
              @Override
              public void runMayThrow() {
                try {
                  runnable.run();
                } catch (Exception e) {
                  LOGGER.warn("Error occurred when executing active load periodical job.", e);
                }
              }
            },
            Math.max(MIN_EXECUTION_INTERVAL_SECONDS, 1)));
  }

  public synchronized void start() {
    if (future == null) {
      future =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              scheduledExecutorService,
              this::execute,
              MIN_EXECUTION_INTERVAL_SECONDS,
              MIN_EXECUTION_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Active load periodical jobs executor is started successfully.");
    }
  }

  private void execute() {
    for (final Pair<WrappedRunnable, Long> periodicalJob : jobs) {
      periodicalJob.left.run();
    }
  }

  public synchronized void stop() {
    if (future != null) {
      future.cancel(false);
      future = null;
      LOGGER.info("Active load periodical jobs executor is stopped successfully.");
    }
  }
}
