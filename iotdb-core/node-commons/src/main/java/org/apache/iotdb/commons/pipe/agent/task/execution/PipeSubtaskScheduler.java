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

package org.apache.iotdb.commons.pipe.agent.task.execution;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

public class PipeSubtaskScheduler {

  private final PipeSubtaskExecutor executor;

  private boolean isFirstSchedule = true;

  private static final int BASIC_CHECKPOINT_INTERVAL_BY_CONSUMED_EVENT_COUNT =
      PipeConfig.getInstance().getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount();
  private int consumedEventCountCheckpointInterval;
  private int consumedEventCount;

  // in ms
  private static final long BASIC_CHECKPOINT_INTERVAL_BY_TIME_DURATION =
      PipeConfig.getInstance().getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration();
  private long timeDurationCheckpointInterval;
  private long lastCheckTime;

  public PipeSubtaskScheduler(PipeSubtaskExecutor executor) {
    this.executor = executor;
  }

  public boolean schedule() {
    if (isFirstSchedule) {
      isFirstSchedule = false;

      adjustCheckpointIntervalBasedOnExecutorStatus();

      ++consumedEventCount;
      return true;
    }

    if (consumedEventCount < consumedEventCountCheckpointInterval
        && System.currentTimeMillis() - lastCheckTime < timeDurationCheckpointInterval) {
      ++consumedEventCount;
      return true;
    }

    return false;
  }

  private void adjustCheckpointIntervalBasedOnExecutorStatus() {
    // 1. reset consumedEventCount and lastCheckTime
    consumedEventCount = 0;
    lastCheckTime = System.currentTimeMillis();

    // 2. adjust checkpoint interval
    final int corePoolSize = Math.max(1, executor.getCorePoolSize());
    final int runningSubtaskNumber = Math.max(1, executor.getRunningSubtaskNumber());
    consumedEventCountCheckpointInterval =
        Math.max(
            1,
            (int)
                (((float) BASIC_CHECKPOINT_INTERVAL_BY_CONSUMED_EVENT_COUNT / runningSubtaskNumber)
                    * corePoolSize));
    timeDurationCheckpointInterval =
        Math.max(
            1,
            (long)
                (((float) BASIC_CHECKPOINT_INTERVAL_BY_TIME_DURATION / runningSubtaskNumber)
                    * corePoolSize));
  }

  public void reset() {
    isFirstSchedule = true;
  }
}
