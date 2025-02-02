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

package org.apache.iotdb.db.pipe.agent.task.subtask.connector;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.agent.task.subtask.checker.PipeConnectorTimeOutChecker;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskWorkerManager;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionConnectorMetrics;
import org.apache.iotdb.db.pipe.metric.PipeSchemaRegionConnectorMetrics;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class UserDefinedConnectorSubtask extends PipeConnectorSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedConnectorSubtask.class);

  private static final AtomicReference<PipeConnectorTimeOutChecker> timeOutChecker =
      new AtomicReference<>();
  private volatile long startRunningTime = System.currentTimeMillis();

  private volatile boolean scheduled = false;

  private volatile boolean isTimeout = false;

  private volatile ListenableFuture<Boolean> nextFuture;

  public UserDefinedConnectorSubtask(
      String taskID,
      long creationTime,
      String attributeSortedString,
      int connectorIndex,
      UnboundedBlockingPendingQueue<Event> inputPendingQueue,
      PipeConnector outputPipeConnector) {
    super(
        taskID,
        creationTime,
        attributeSortedString,
        connectorIndex,
        inputPendingQueue,
        outputPipeConnector);
  }

  @Override
  public void bindExecutors(
      final ListeningExecutorService ignored,
      final ListeningExecutorService userDefineSubtaskWorkerThreadPoolExecutor,
      final ExecutorService subtaskCallbackListeningExecutor,
      final ScheduledExecutorService subtaskTimeoutListeningExecutor,
      final PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = userDefineSubtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskTimeoutListeningExecutor = subtaskTimeoutListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
    // double check locking for constructing PipeProcessorSubtaskWorkerManager
    if (timeOutChecker.get() == null) {
      synchronized (PipeProcessorSubtaskWorkerManager.class) {
        if (timeOutChecker.get() == null) {
          timeOutChecker.set(new PipeConnectorTimeOutChecker());
          subtaskTimeoutListeningExecutor.submit(timeOutChecker.get());
        }
      }
    }
  }

  @Override
  protected boolean executeOnce() {
    Event event = null;
    try {
      beginSubtaskProcessing();
      event = executeOnceInternal();
      endSubtaskProcessing();
    } catch (final PipeException e) {
      endSubtaskProcessing();
      if (!isClosed.get()) {
        setLastExceptionEvent(event);
        throw e;
      } else {
        LOGGER.info(
            "{} in pipe transfer, ignored because the connector subtask is dropped.",
            e.getClass().getSimpleName(),
            e);
        clearReferenceCountAndReleaseLastEvent(event);
      }
    } catch (final Exception e) {
      endSubtaskProcessing();
      if (!isClosed.get()) {
        setLastExceptionEvent(event);
        throw new PipeException(
            String.format(
                "Exception in pipe transfer, subtask: %s, last event: %s, root cause: %s",
                taskID,
                event instanceof EnrichedEvent
                    ? ((EnrichedEvent) event).coreReportMessage()
                    : event,
                ErrorHandlingUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info(
            "Exception in pipe transfer, ignored because the connector subtask is dropped.", e);
        clearReferenceCountAndReleaseLastEvent(event);
      }
    }

    return true;
  }

  protected Event executeOnceInternal() throws Exception {
    final Event event =
        lastEvent != null
            ? lastEvent
            : UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll());
    // Record this event for retrying on connection failure or other exceptions
    setLastEvent(event);
    if (event instanceof EnrichedEvent && ((EnrichedEvent) event).isReleased()) {
      lastEvent = null;
      return event;
    }

    if (event == null) {
      if (System.currentTimeMillis() - lastHeartbeatEventInjectTime
          > CRON_HEARTBEAT_EVENT_INJECT_INTERVAL_MILLISECONDS) {
        transferHeartbeatEvent(CRON_HEARTBEAT_EVENT);
      }
      return null;
    }

    if (event instanceof TabletInsertionEvent) {
      outputPipeConnector.transfer((TabletInsertionEvent) event);
      PipeDataRegionConnectorMetrics.getInstance().markTabletEvent(taskID);
    } else if (event instanceof TsFileInsertionEvent) {
      outputPipeConnector.transfer((TsFileInsertionEvent) event);
      PipeDataRegionConnectorMetrics.getInstance().markTsFileEvent(taskID);
    } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
      outputPipeConnector.transfer(event);
      if (((PipeSchemaRegionWritePlanEvent) event).getPlanNode().getType()
          != PlanNodeType.DELETE_DATA) {
        // Only plan nodes in schema region will be marked, delete data node is currently not
        // taken into account
        PipeSchemaRegionConnectorMetrics.getInstance().markSchemaEvent(taskID);
      }
    } else if (event instanceof PipeHeartbeatEvent) {
      transferHeartbeatEvent((PipeHeartbeatEvent) event);
    } else {
      outputPipeConnector.transfer(
          event instanceof UserDefinedEnrichedEvent
              ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
              : event);
    }

    decreaseReferenceCountAndReleaseLastEvent(event, true);
    return event;
  }

  @Override
  public synchronized void submitSelf() {
    if (shouldStopSubmittingSelf.get() || isSubmitted) {
      return;
    }

    nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
    registerCallbackHookAfterSubmit(nextFuture);
    isSubmitted = true;
  }

  @Override
  public void allowSubmittingSelf() {
    super.allowSubmittingSelf();
    if (timeOutChecker.get() != null) {
      timeOutChecker.get().registerUserDefinedConnectorSubtask(this);
    }
  }

  @Override
  public boolean disallowSubmittingSelf() {
    if (timeOutChecker.get() != null) {
      timeOutChecker.get().unregisterUserDefinedConnectorSubtask(this);
    }
    return super.disallowSubmittingSelf();
  }

  public long getStartRunningTime() {
    return startRunningTime;
  }

  public boolean isScheduled() {
    return scheduled;
  }

  // The task starts running. Only after the task starts running can the interruption be checked to
  // ensure that the thread interruption will not affect the Pipe framework.
  private void beginSubtaskProcessing() {
    startRunningTime = System.currentTimeMillis();
    synchronized (this) {
      scheduled = true;
    }
  }

  // Mark the end. When the process is finished, cannot check whether it has timed out to avoid
  // affecting the pipe framework.
  private void endSubtaskProcessing() {
    // synchronized will not be interrupted by Thread.interrupt function
    synchronized (this) {
      scheduled = false;
    }
    // clear thread execution state;
    if (Thread.currentThread().isInterrupted()) {
      LOGGER.warn("Connector {} SubTask execution time timeout", taskID);
    }
  }

  public ListenableFuture<Boolean> getNextFuture() {
    return nextFuture;
  }
}
