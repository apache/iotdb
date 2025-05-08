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

package org.apache.iotdb.db.pipe.event.common.terminate;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.StorageEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@link PipeTerminateEvent} is an {@link EnrichedEvent} that controls the termination of pipe,
 * that is, when the historical {@link PipeTsFileInsertionEvent}s are all processed, this will be
 * reported next and mark the {@link PipeDataNodeTask} as completed. WARNING: This event shall never
 * be discarded.
 */
public class PipeTerminateEvent extends EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTerminateEvent.class);

  private static final AtomicInteger PROGRESS_REPORT_COUNT = new AtomicInteger(0);
  private static final AtomicLong LAST_PROGRESS_REPORT_TIME = new AtomicLong(0);

  public static void flushDataRegionIfNeeded() {
    if (PROGRESS_REPORT_COUNT.get() > PipeConfig.getInstance().getPipeFlushAfterTerminateCount()) {
      PROGRESS_REPORT_COUNT.set(0);
      LAST_PROGRESS_REPORT_TIME.set(0);
      try {
        StorageEngine.getInstance().operateFlush(new TFlushReq());
        LOGGER.warn("Force flush all data regions because of progress report count exceed.");
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to flush all data regions, please check the error message: {}",
            e.getMessage(),
            e);
      }
      return;
    }

    if (LAST_PROGRESS_REPORT_TIME.get() > 0) {
      final long timeSinceLastReport = System.currentTimeMillis() - LAST_PROGRESS_REPORT_TIME.get();
      if (timeSinceLastReport
          > PipeConfig.getInstance().getPipeFlushAfterLastTerminateSeconds() * 1000L) {
        try {
          StorageEngine.getInstance().operateFlush(new TFlushReq());
          LAST_PROGRESS_REPORT_TIME.set(0);
          LOGGER.warn("Force flush all data regions because of last progress report time.");
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to flush all data regions, please check the error message: {}",
              e.getMessage(),
              e);
        }
      }
    }
  }

  private final int dataRegionId;

  public PipeTerminateEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final int dataRegionId) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
    this.dataRegionId = dataRegionId;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userName,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    // Should record PipeTaskMeta, for the terminateEvent shall report progress to
    // notify the pipeTask it's completed.
    return new PipeTerminateEvent(pipeName, creationTime, pipeTaskMeta, dataRegionId);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  @Override
  public void reportProgress() {
    PROGRESS_REPORT_COUNT.incrementAndGet();
    LAST_PROGRESS_REPORT_TIME.set(System.currentTimeMillis());

    // To avoid deadlock
    CompletableFuture.runAsync(
        () -> PipeDataNodeAgent.task().markCompleted(pipeName, dataRegionId));
  }

  @Override
  public String toString() {
    return String.format("PipeTerminateEvent{dataRegionId=%s}", dataRegionId)
        + " - "
        + super.toString();
  }
}
