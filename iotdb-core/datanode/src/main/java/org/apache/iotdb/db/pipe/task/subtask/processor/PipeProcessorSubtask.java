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

import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.db.pipe.task.connection.EventSupplier;
import org.apache.iotdb.db.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PipeProcessorSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
      new AtomicReference<>();

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final EventCollector outputEventCollector;

  private final AtomicBoolean isClosed;

  public PipeProcessorSubtask(
      String taskID,
      EventSupplier inputEventSupplier,
      PipeProcessor pipeProcessor,
      EventCollector outputEventCollector) {
    super(taskID);
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
    isClosed = new AtomicBoolean(false);
  }

  @Override
  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService ignored,
      PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskScheduler = subtaskScheduler;

    // double check locking for constructing PipeProcessorSubtaskWorkerManager
    if (subtaskWorkerManager.get() == null) {
      synchronized (PipeProcessorSubtaskWorkerManager.class) {
        if (subtaskWorkerManager.get() == null) {
          subtaskWorkerManager.set(
              new PipeProcessorSubtaskWorkerManager(subtaskWorkerThreadPoolExecutor));
        }
      }
    }
    subtaskWorkerManager.get().schedule(this);
  }

  @Override
  protected synchronized boolean executeOnce() throws Exception {
    final Event event = lastEvent != null ? lastEvent : inputEventSupplier.supply();
    // Record the last event for retry when exception occurs
    lastEvent = event;
    if (event == null) {
      return false;
    }

    try {
      if (event instanceof TabletInsertionEvent) {
        pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
      } else if (event instanceof TsFileInsertionEvent) {
        pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
      } else if (event instanceof PipeHeartbeatEvent) {
        pipeProcessor.process(event, outputEventCollector);
        ((PipeHeartbeatEvent) event).onProcessed();
      } else {
        pipeProcessor.process(event, outputEventCollector);
      }

      releaseLastEvent();
    } catch (Exception e) {
      throw new PipeException(
          "Error occurred during executing PipeProcessor#process, perhaps need to check "
              + "whether the implementation of PipeProcessor is correct "
              + "according to the pipe-api description.",
          e);
    }

    return true;
  }

  @Override
  public void submitSelf() {
    // this subtask won't be submitted to the executor directly
    // instead, it will be executed by the PipeProcessorSubtaskWorker
    // and the worker will be submitted to the executor
  }

  @Override
  // synchronized for pipeProcessor.close() and releaseLastEvent() in super.close().
  // make sure that the lastEvent will not be updated after pipeProcessor.close() to avoid
  // resource leak because of the lastEvent is not released.
  public synchronized void close() {
    try {
      isClosed.set(true);

      pipeProcessor.close();

      // should be called after pipeProcessor.close()
      super.close();
    } catch (Exception e) {
      LOGGER.info(
          "Error occurred during closing PipeProcessor, perhaps need to check whether the "
              + "implementation of PipeProcessor is correct according to the pipe-api description.",
          e);
    }
  }

  boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public boolean equals(Object that) {
    return that instanceof PipeProcessorSubtask
        && this.taskID.equals(((PipeProcessorSubtask) that).taskID);
  }

  @Override
  public int hashCode() {
    return taskID.hashCode();
  }
}
