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
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

  private static final long MIN_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds();
  private long rounds;
  private Future<?> executorFuture;

  // <Periodical job, Interval in rounds>
  private static final List<Pair<WrappedRunnable, Long>> periodicalJobs = new ArrayList<>();

  public synchronized void register(String id, Runnable periodicalJob, long intervalInSeconds) {
    periodicalJobs.add(
        new Pair<>(
            new WrappedRunnable() {
              @Override
              public void runMayThrow() {
                try {
                  periodicalJob.run();
                } catch (Exception e) {
                  LOGGER.warn("Periodical job {} failed.", id, e);
                }
              }
            },
            Math.max(intervalInSeconds / MIN_INTERVAL_SECONDS, 1)));
    LOGGER.info(
        "Pipe periodical job {} is registered successfully. Interval: {} seconds.",
        id,
        Math.max(intervalInSeconds / MIN_INTERVAL_SECONDS, 1) * MIN_INTERVAL_SECONDS);
  }

  public synchronized void start() {
    if (executorFuture == null) {
      rounds = 0;

      executorFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              PERIODICAL_JOB_EXECUTOR,
              this::execute,
              MIN_INTERVAL_SECONDS,
              MIN_INTERVAL_SECONDS,
              TimeUnit.SECONDS);
      LOGGER.info("Pipe periodical job executor is started successfully.");
    }
  }

  private synchronized void execute() {
    ++rounds;

    for (final Pair<WrappedRunnable, Long> periodicalJob : periodicalJobs) {
      if (rounds % periodicalJob.right == 0) {
        periodicalJob.left.run();
      }
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
