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

package org.apache.iotdb.db.pipe.event.common.statement;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeStatementInsertionEvent extends PipeInsertionEvent
    implements ReferenceTrackableEvent, AutoCloseable {

  // For better calculation
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeStatementInsertionEvent.class);
  private InsertBaseStatement statement;

  private volatile ProgressIndex progressIndex;
  private boolean needToReport;

  private final PipeTabletMemoryBlock allocatedMemoryBlock;

  public PipeStatementInsertionEvent(
      String pipeName,
      long creationTime,
      PipeTaskMeta pipeTaskMeta,
      TreePattern treePattern,
      TablePattern tablePattern,
      String userId,
      String userName,
      String cliHostname,
      boolean skipIfNoPrivileges,
      Boolean isTableModelEvent,
      String databaseNameFromDataRegion,
      InsertBaseStatement statement) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userId,
        userName,
        cliHostname,
        skipIfNoPrivileges,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        isTableModelEvent,
        databaseNameFromDataRegion,
        null,
        null);
    this.statement = statement;
    // Allocate empty memory block, will be resized later.
    this.allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    PipeDataNodeResourceManager.memory()
        .forceResize(allocatedMemoryBlock, statement.ramBytesUsed() + INSTANCE_SIZE);
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeSinglePipeMetrics.getInstance()
          .increaseRawTabletEventCount(pipeName, creationTime);
    }
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeSinglePipeMetrics.getInstance()
          .decreaseRawTabletEventCount(pipeName, creationTime);
    }
    allocatedMemoryBlock.close();

    statement = null;
    return true;
  }

  @Override
  public void bindProgressIndex(final ProgressIndex progressIndex) {
    // Normally not all events need to report progress, but if the progressIndex
    // is given, indicating that the progress needs to be reported.
    if (Objects.nonNull(progressIndex)) {
      markAsNeedToReport();
    }

    this.progressIndex = progressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex == null ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      long creationTime,
      PipeTaskMeta pipeTaskMeta,
      TreePattern treePattern,
      TablePattern tablePattern,
      String userId,
      String userName,
      String cliHostname,
      boolean skipIfNoPrivileges,
      long startTime,
      long endTime) {
    throw new UnsupportedOperationException(
        "shallowCopySelfAndBindPipeTaskMetaForProgressReport() is not supported!");
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    throw new UnsupportedOperationException(
        "mayEventTimeOverlappedWithTimeRange() is not supported!");
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    throw new UnsupportedOperationException(
        "mayEventPathsOverlappedWithPattern() is not supported!");
  }

  public void markAsNeedToReport() {
    this.needToReport = true;
  }

  public Statement getStatement() {
    return statement;
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeStatementInsertionEvent{statement=%s, needToReport=%s, allocatedMemoryBlock=%s}",
            statement, needToReport, allocatedMemoryBlock)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeStatementInsertionEvent{statement=%s, needToReport=%s, allocatedMemoryBlock=%s}",
            statement, needToReport, allocatedMemoryBlock)
        + " - "
        + super.coreReportMessage();
  }

  /////////////////////////// ReferenceTrackableEvent //////////////////////////////

  @Override
  protected void trackResource() {
    PipeDataNodeResourceManager.ref().trackPipeEventResource(this, eventResourceBuilder());
  }

  @Override
  public PipePhantomReferenceManager.PipeEventResource eventResourceBuilder() {
    return new PipeStatementInsertionEventResource(
        this.isReleased, this.referenceCount, this.allocatedMemoryBlock);
  }

  private static class PipeStatementInsertionEventResource
      extends PipePhantomReferenceManager.PipeEventResource {

    private final PipeTabletMemoryBlock allocatedMemoryBlock;

    private PipeStatementInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final PipeTabletMemoryBlock allocatedMemoryBlock) {
      super(isReleased, referenceCount);
      this.allocatedMemoryBlock = allocatedMemoryBlock;
    }

    @Override
    protected void finalizeResource() {
      allocatedMemoryBlock.close();
    }
  }

  /////////////////////////// AutoCloseable ///////////////////////////

  @Override
  public void close() throws Exception {}
}
