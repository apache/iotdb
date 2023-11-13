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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.extractor.realtime.listener.PipeInsertionDataNodeListener;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Single thread to execute pipe periodical jobs on dataNode. This is for limiting the thread num on
 * the DataNode instance.
 */
public class PipePeriodicalJobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePeriodicalJobExecutor.class);

  private static final ScheduledExecutorService PERIODICAL_JOB_EXECUTOR =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_RUNTIME_PERIODICAL_JOB_EXECUTOR.getName());

  private static final long CRON_EVENT_INJECTOR_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds();
  private long cronEventInjectRounds = 0;

  private static final long MEMORY_EXPANDER_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeMemoryExpanderIntervalSeconds();
  private long memoryExpandRounds = 0;

  private long rounds = 0;

  // Currently we use the CRON_EVENT_INJECTOR_INTERVAL_SECONDS as minimum interval
  private static final long EXECUTOR_INTERVAL_SECONDS = CRON_EVENT_INJECTOR_INTERVAL_SECONDS;

  private Future<?> executorFuture;

  public synchronized void start() {
    if (executorFuture == null) {
      executorFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              PERIODICAL_JOB_EXECUTOR,
              this::execute,
              EXECUTOR_INTERVAL_SECONDS,
              EXECUTOR_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Pipe periodical job executor is started successfully.");
    }
  }

  private synchronized void execute() {
    rounds++;
    long elapsedTime = EXECUTOR_INTERVAL_SECONDS * rounds;
    // We still judge the cron heartbeat rounds for the generality of EXECUTOR_INTERVAL_SECONDS
    if (elapsedTime / CRON_EVENT_INJECTOR_INTERVAL_SECONDS > cronEventInjectRounds) {
      cronEventInjectRounds = elapsedTime / EXECUTOR_INTERVAL_SECONDS;
      PipeInsertionDataNodeListener.getInstance().listenToHeartbeat(false);
    }
    if (elapsedTime / MEMORY_EXPANDER_INTERVAL_SECONDS > memoryExpandRounds) {
      memoryExpandRounds = elapsedTime / EXECUTOR_INTERVAL_SECONDS;
      PipeResourceManager.memory().tryExpandAll();
    }
  }

  public synchronized void stop() {
    if (executorFuture != null) {
      executorFuture.cancel(false);
      executorFuture = null;
      LOGGER.info("Pipe periodical job executor is stopped successfully.");
    }
  }
}
