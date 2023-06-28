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

package org.apache.iotdb.db.pipe.task.subtask.processor;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.db.pipe.agent.PipeAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.iotdb.db.pipe.task.subtask.PipeSubtask.MAX_RETRY_TIMES;

public class PipeProcessorSubtaskWorker extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtaskWorker.class);

  private final ConcurrentMap<PipeProcessorSubtask, PipeProcessorSubtask> subtasks =
      new ConcurrentHashMap<>();

  @Override
  public void runMayThrow() {
    while (true) {
      // exit if the agent is shutdown
      if (PipeAgent.runtime().isShutdown()) {
        return;
      }

      // clean up closed subtasks before running
      subtasks.keySet().stream().filter(PipeProcessorSubtask::isClosed).forEach(subtasks::remove);

      // run subtasks
      final boolean canSleepBeforeNextRound = runSubtasks();

      // sleep if no subtask is running
      if (canSleepBeforeNextRound) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOGGER.warn("subtask worker is interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private boolean runSubtasks() {
    boolean canSleepBeforeNextRound = true;

    for (final PipeProcessorSubtask subtask : subtasks.keySet()) {
      if (subtask.isClosed()
          || !subtask.isSubmittingSelf()
          || MAX_RETRY_TIMES <= subtask.getRetryCount()) {
        continue;
      }

      try {
        final boolean hasAtLeastOneEventProcessed = subtask.call();
        if (hasAtLeastOneEventProcessed) {
          canSleepBeforeNextRound = false;
        }
        subtask.onSuccess(hasAtLeastOneEventProcessed);
      } catch (Exception e) {
        if (subtask.isClosed()) {
          LOGGER.warn("subtask {} is closed, ignore exception", subtask, e);
        } else {
          subtask.onFailure(e);
        }
      }
    }

    return canSleepBeforeNextRound;
  }

  public void schedule(PipeProcessorSubtask pipeProcessorSubtask) {
    subtasks.put(pipeProcessorSubtask, pipeProcessorSubtask);
  }
}
