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

package org.apache.iotdb.commons.pipe.agent.runtime;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single thread to execute pipe periodical jobs on DataNode or ConfigNode. This is for limiting the
 * thread num on the DataNode or ConfigNode instance.
 */
public abstract class AbstractPipePeriodicalJobExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractPipePeriodicalJobExecutor.class);

  private static final long PERIODICAL_JOB_FAILURE_LOG_INTERVAL_MS = TimeUnit.HOURS.toMillis(1);

  private final ScheduledExecutorService executorService;
  private final long minIntervalSeconds;

  private long rounds;
  private Future<?> executorFuture;

  // <Periodical job, Interval in rounds>
  private final List<Pair<WrappedRunnable, Long>> periodicalJobs = new CopyOnWriteArrayList<>();

  public AbstractPipePeriodicalJobExecutor(
      final ScheduledExecutorService executorService, final long minIntervalSeconds) {
    this.executorService = executorService;
    this.minIntervalSeconds = minIntervalSeconds;
  }

  public void register(String id, Runnable periodicalJob, long intervalInSeconds) {
    periodicalJobs.add(
        new Pair<>(
            wrapPeriodicalJobWithFailureLogThrottle(id, periodicalJob),
            Math.max(intervalInSeconds / minIntervalSeconds, 1)));
    LOGGER.info(
        PipeMessages.PERIODICAL_JOB_REGISTERED,
        id,
        Math.max(intervalInSeconds / minIntervalSeconds, 1) * minIntervalSeconds);
  }

  static WrappedRunnable wrapPeriodicalJobWithFailureLogThrottle(
      String id, Runnable periodicalJob) {
    return new WrappedRunnable() {
      private final AtomicLong lastFailureLogTime = new AtomicLong(0L);

      @Override
      public void runMayThrow() {
        try {
          periodicalJob.run();
          lastFailureLogTime.set(0L);
        } catch (Exception e) {
          logPeriodicalJobFailureIfNecessary(id, e, lastFailureLogTime);
        }
      }
    };
  }

  private static void logPeriodicalJobFailureIfNecessary(
      String id, Exception e, AtomicLong lastFailureLogTime) {
    long now = System.currentTimeMillis();
    long previousLogTime = lastFailureLogTime.get();
    if ((previousLogTime == 0L || now - previousLogTime >= PERIODICAL_JOB_FAILURE_LOG_INTERVAL_MS)
        && lastFailureLogTime.compareAndSet(previousLogTime, now)) {
      LOGGER.warn(PipeMessages.PERIODICAL_JOB_FAILED, id, e);
    }
  }

  public synchronized void start() {
    if (executorFuture == null) {
      rounds = 0;

      executorFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              executorService,
              this::execute,
              minIntervalSeconds,
              minIntervalSeconds,
              TimeUnit.SECONDS);
      LOGGER.info(PipeMessages.PERIODICAL_JOB_EXECUTOR_STARTED);
    }
  }

  protected void execute() {
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
      LOGGER.info(PipeMessages.PERIODICAL_JOB_EXECUTOR_STOPPED);
    }
  }

  public void runDirectly(final Runnable command) {
    executorService.execute(command);
  }

  @TestOnly
  public void clear() {
    periodicalJobs.clear();
    LOGGER.info(PipeMessages.PERIODICAL_JOBS_CLEARED);
  }
}
