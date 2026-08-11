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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PipeProcessorSubtaskWorker extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtaskWorker.class);

  private static final int SLEEP_INTERVAL_ADJUSTMENT_ROUND_INTERVAL = 100;
  private static final long LONG_RUNNING_EVENT_INITIAL_REPORT_DELAY_IN_NANOS =
      TimeUnit.MINUTES.toNanos(10);
  private static final long LONG_RUNNING_EVENT_REPORT_INTERVAL_IN_NANOS =
      TimeUnit.MINUTES.toNanos(30);
  private static final int MAX_EVENT_REPORT_LENGTH = 1024;
  private static final int MAX_STACK_TRACE_DEPTH = 64;

  private int totalRoundInAdjustmentInterval = 0;
  private int workingRoundInAdjustmentInterval = 0;
  private long sleepingTimeInMilliSecond = 50;

  private final Set<PipeProcessorSubtask> subtasks;

  private volatile Thread workerThread;
  private volatile PipeProcessorSubtask currentSubtask;

  private PipeProcessorSubtask.EventProcessingContext lastReportedEventProcessingContext;
  private long lastEventReportTimeInNanos = Long.MIN_VALUE;

  public PipeProcessorSubtaskWorker() {
    this(Collections.newSetFromMap(new ConcurrentHashMap<>()));
  }

  @VisibleForTesting
  PipeProcessorSubtaskWorker(final Set<PipeProcessorSubtask> subtasks) {
    this.subtasks = subtasks;
  }

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
    subtasks.removeIf(PipeProcessorSubtask::isClosed);
  }

  @VisibleForTesting
  boolean runSubtasks() {
    ++totalRoundInAdjustmentInterval;

    boolean canSleepBeforeNextRound = true;

    for (final PipeProcessorSubtask subtask : subtasks) {
      if (subtask.isClosed() || !subtask.isSubmittingSelf() || subtask.isStoppedByException()) {
        continue;
      }

      workerThread = Thread.currentThread();
      currentSubtask = subtask;
      try {
        final boolean hasAtLeastOneEventProcessed = subtask.call();
        if (hasAtLeastOneEventProcessed) {
          canSleepBeforeNextRound = false;
        }
        subtask.onSuccess(hasAtLeastOneEventProcessed);
      } catch (final PipeProcessorSubtaskYieldException ignored) {
        // The subtask voluntarily yields this worker without succeeding, failing, or retrying.
      } catch (final Exception e) {
        if (subtask.isClosed()) {
          LOGGER.warn(DataNodePipeMessages.SUBTASK_IS_CLOSED_IGNORE_EXCEPTION, subtask, e);
        } else {
          subtask.onFailure(e);
        }
      } finally {
        currentSubtask = null;
      }
    }

    return canSleepBeforeNextRound;
  }

  private void sleepIfNecessary(final boolean canSleepBeforeNextRound) {
    if (canSleepBeforeNextRound) {
      try {
        Thread.sleep(sleepingTimeInMilliSecond);
      } catch (final InterruptedException e) {
        LOGGER.warn(DataNodePipeMessages.SUBTASK_WORKER_IS_INTERRUPTED, e);
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

  void watchLongRunningEvent() {
    final PipeProcessorSubtask subtask = currentSubtask;
    final Thread thread = workerThread;
    if (subtask == null || thread == null) {
      return;
    }

    final PipeProcessorSubtask.EventProcessingContext context = subtask.getEventProcessingContext();
    final long currentTimeInNanos = System.nanoTime();
    if (!isLongRunningEventReportDue(context, currentTimeInNanos)) {
      return;
    }

    final StackTraceElement[] stackTrace = thread.getStackTrace();
    // The event may finish while its stack is being captured. Do not attribute a later event's
    // stack to this event.
    if (currentSubtask != subtask || subtask.getEventProcessingContext() != context) {
      return;
    }

    markLongRunningEventReported(context, currentTimeInNanos);
    LOGGER.warn(
        DataNodePipeMessages
            .LOG_PIPE_PROCESSOR_WORKER_ARG_HAS_BEEN_PROCESSING_THE_SAME_EVENT_FOR_ARG_MS_PIPE_ARG_DATAREGION_ARG_SUBTASK_ARG_EVENT_ARG_THREAD_STATE_ARG_STACK_ARG_63B40775,
        thread.getName(),
        TimeUnit.NANOSECONDS.toMillis(currentTimeInNanos - context.getStartTimeInNanos()),
        subtask.getPipeName(),
        subtask.getRegionId(),
        subtask.getDisplayTaskID(),
        getEventReport(context.getEvent()),
        thread.getState(),
        formatStackTrace(stackTrace));
  }

  @VisibleForTesting
  boolean isLongRunningEventReportDue(
      final PipeProcessorSubtask.EventProcessingContext context, final long currentTimeInNanos) {
    if (context == null
        || currentTimeInNanos - context.getStartTimeInNanos()
            < LONG_RUNNING_EVENT_INITIAL_REPORT_DELAY_IN_NANOS) {
      return false;
    }

    return lastReportedEventProcessingContext != context
        || currentTimeInNanos - lastEventReportTimeInNanos
            >= LONG_RUNNING_EVENT_REPORT_INTERVAL_IN_NANOS;
  }

  @VisibleForTesting
  void markLongRunningEventReported(
      final PipeProcessorSubtask.EventProcessingContext context, final long currentTimeInNanos) {
    lastReportedEventProcessingContext = context;
    lastEventReportTimeInNanos = currentTimeInNanos;
  }

  @VisibleForTesting
  static String getEventReport(final Event event) {
    String report = event.getClass().getName();
    if (event instanceof EnrichedEvent) {
      try {
        report =
            event.getClass().getSimpleName() + ": " + ((EnrichedEvent) event).coreReportMessage();
      } catch (final RuntimeException ignored) {
        // Keep the event class name if its diagnostic method fails.
      }
    }

    report = report.replace('\n', ' ').replace('\r', ' ');
    return report.length() <= MAX_EVENT_REPORT_LENGTH
        ? report
        : report.substring(0, MAX_EVENT_REPORT_LENGTH) + "...";
  }

  @VisibleForTesting
  static String formatStackTrace(final StackTraceElement[] stackTrace) {
    final StringBuilder builder = new StringBuilder();
    final int frameCount = Math.min(stackTrace.length, MAX_STACK_TRACE_DEPTH);
    for (int i = 0; i < frameCount; ++i) {
      builder.append('\n').append('\t').append(stackTrace[i]);
    }
    if (stackTrace.length > frameCount) {
      builder
          .append('\n')
          .append('\t')
          .append("... (")
          .append(stackTrace.length - frameCount)
          .append(')');
    }
    return builder.toString();
  }
}
