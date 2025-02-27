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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PipeProcessorSubtaskWorker extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtaskWorker.class);

  private static final long CLOSED_SUBTASK_CLEANUP_ROUND_INTERVAL = 1000;
  private long closedSubtaskCleanupRoundCounter = 0;

  private static final int SLEEP_INTERVAL_ADJUSTMENT_ROUND_INTERVAL = 100;
  private int totalRoundInAdjustmentInterval = 0;
  private int workingRoundInAdjustmentInterval = 0;
  private long sleepingTimeInMilliSecond = 50;

  private final Set<PipeProcessorSubtask> subtasks =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  @SuppressWarnings("squid:S2189")
  public void runMayThrow() {
    while (true) {
      cleanupClosedSubtasksIfNecessary();
      final boolean canSleepBeforeNextRound = runSubtasks();
      sleepIfNecessary(canSleepBeforeNextRound);
      adjustSleepingTimeIfNecessary();
    }
  }

  private void cleanupClosedSubtasksIfNecessary() {
    if (++closedSubtaskCleanupRoundCounter % CLOSED_SUBTASK_CLEANUP_ROUND_INTERVAL == 0) {
      subtasks.stream()
          .filter(PipeProcessorSubtask::isClosed)
          .collect(Collectors.toList())
          .forEach(subtasks::remove);
    }
  }

  private boolean runSubtasks() {
    ++totalRoundInAdjustmentInterval;

    boolean canSleepBeforeNextRound = true;

    for (final PipeProcessorSubtask subtask : subtasks) {
      if (subtask.isClosed() || !subtask.isSubmittingSelf() || subtask.isStoppedByException()) {
        continue;
      }

      try {
        final boolean hasAtLeastOneEventProcessed = subtask.call();
        if (hasAtLeastOneEventProcessed) {
          canSleepBeforeNextRound = false;
        }
        subtask.onSuccess(hasAtLeastOneEventProcessed);
      } catch (final Exception e) {
        if (subtask.isClosed()) {
          LOGGER.warn("subtask {} is closed, ignore exception", subtask, e);
        } else {
          subtask.onFailure(e);
        }
      }
    }

    return canSleepBeforeNextRound;
  }

  private void sleepIfNecessary(final boolean canSleepBeforeNextRound) {
    if (canSleepBeforeNextRound) {
      try {
        Thread.sleep(sleepingTimeInMilliSecond);
      } catch (final InterruptedException e) {
        LOGGER.warn("subtask worker is interrupted", e);
        Thread.currentThread().interrupt();
      }
    } else {
      ++workingRoundInAdjustmentInterval;
    }
  }

  private void adjustSleepingTimeIfNecessary() {
    if (totalRoundInAdjustmentInterval % SLEEP_INTERVAL_ADJUSTMENT_ROUND_INTERVAL == 0) {
      final double workingRatioInAdjustmentInterval =
          (double) workingRoundInAdjustmentInterval / totalRoundInAdjustmentInterval;

      if (0.25 <= workingRatioInAdjustmentInterval) {
        sleepingTimeInMilliSecond = Math.max(1, sleepingTimeInMilliSecond / 2);
      }

      if (workingRatioInAdjustmentInterval <= 0.05) {
        sleepingTimeInMilliSecond = Math.min(1000, sleepingTimeInMilliSecond * 2);
      }

      totalRoundInAdjustmentInterval = 0;
      workingRoundInAdjustmentInterval = 0;
    }
  }

  public void schedule(final PipeProcessorSubtask pipeProcessorSubtask) {
    subtasks.add(pipeProcessorSubtask);
  }
}
