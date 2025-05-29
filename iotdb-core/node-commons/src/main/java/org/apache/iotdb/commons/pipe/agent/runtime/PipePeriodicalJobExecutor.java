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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The shortest scheduling cycle for these jobs is {@link
 * PipeConfig#getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds()}, suitable for jobs that are
 * NOT time-critical.
 */
public class PipePeriodicalJobExecutor extends AbstractPipePeriodicalJobExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePeriodicalJobExecutor.class);
  // This background service is used to execute jobs that need to be cancelled and released.
  private static final ScheduledExecutorService backgroundService =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_PROGRESS_INDEX_BACKGROUND_SERVICE.getName());

  public static Future<?> submitBackgroundJob(
      Runnable job, long initialDelayInMs, long periodInMs) {
    return ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        backgroundService, job, initialDelayInMs, periodInMs, TimeUnit.MILLISECONDS);
  }

  public static void shutdownBackgroundService() {
    backgroundService.shutdownNow();
    try {
      if (!backgroundService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Pipe progressIndex background service did not terminate within {}s", 30);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Pipe progressIndex background service still doesn't exit after 30s");
      Thread.currentThread().interrupt();
    }
  }

  public PipePeriodicalJobExecutor() {
    super(
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.PIPE_RUNTIME_PERIODICAL_JOB_EXECUTOR.getName()),
        PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds());
  }
}
