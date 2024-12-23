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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTablePatternParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTreePatternParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent extends PipeInsertionEvent
    implements TabletInsertionEvent, ReferenceTrackableEvent {

  // For better calculation
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeRawTabletInsertionEvent.class);
  private Tablet tablet;
  private String deviceId; // Only used when the tablet is released.
  private final boolean isAligned;

  private final EnrichedEvent sourceEvent;
  private boolean needToReport;

  private PipeTabletMemoryBlock allocatedMemoryBlock;

  private TabletInsertionEventParser eventParser;

  private volatile ProgressIndex overridingProgressIndex;

  private PipeRawTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final Tablet tablet,
      final boolean isAligned,
      final EnrichedEvent sourceEvent,
      final boolean needToReport,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        startTime,
        endTime,
        isTableModelEvent,
        databaseName);
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.sourceEvent = sourceEvent;
    this.needToReport = needToReport;
  }

  public PipeRawTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final Tablet tablet,
      final boolean isAligned,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final boolean needToReport) {
    this(
        isTableModelEvent,
        databaseName,
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(final Tablet tablet, final boolean isAligned) {
    this(
        null,
        null,
        tablet,
        isAligned,
        null,
        false,
        null,
        0,
        null,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final boolean isAligned, final TreePattern treePattern) {
    this(
        null,
        null,
        tablet,
        isAligned,
        null,
        false,
        null,
        0,
        null,
        treePattern,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final long startTime, final long endTime) {
    this(null, null, tablet, false, null, false, null, 0, null, null, null, startTime, endTime);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(
                PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) + INSTANCE_SIZE);
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
          .increaseTabletEventCount(pipeName, creationTime);
    }
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    if (Objects.nonNull(pipeName)) {
      PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
          .decreaseTabletEventCount(pipeName, creationTime);
    }
    allocatedMemoryBlock.close();

    // Record the deviceId before the memory is released,
    // for later possibly updating the leader cache.
    deviceId = tablet.getDeviceId();

    // Actually release the occupied memory.
    tablet = null;
    eventParser = null;
    return true;
  }

  @Override
  protected void reportProgress() {
    if (needToReport) {
      super.reportProgress();
      if (sourceEvent instanceof PipeTsFileInsertionEvent) {
        ((PipeTsFileInsertionEvent) sourceEvent).eliminateProgressIndex();
      }
    }
  }

  @Override
  public void bindProgressIndex(final ProgressIndex overridingProgressIndex) {
    // Normally not all events need to report progress, but if the overridingProgressIndex
    // is given, indicating that the progress needs to be reported.
    if (Objects.nonNull(overridingProgressIndex)) {
      markAsNeedToReport();
    }

    this.overridingProgressIndex = overridingProgressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    // If the overridingProgressIndex is given, ignore the sourceEvent's progressIndex.
    if (Objects.nonNull(overridingProgressIndex)) {
      return overridingProgressIndex;
    }

    return sourceEvent != null ? sourceEvent.getProgressIndex() : MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return new PipeRawTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getTreeModelDatabaseName(),
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    throw new UnsupportedOperationException("isGeneratedByPipe() is not supported!");
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    final long[] timestamps = tablet.timestamps;
    if (Objects.isNull(timestamps) || timestamps.length == 0) {
      return false;
    }
    // We assume that `timestamps` is ordered.
    return startTime <= timestamps[timestamps.length - 1] && timestamps[0] <= endTime;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return sourceEvent == null || sourceEvent.mayEventPathsOverlappedWithPattern();
  }

  public void markAsNeedToReport() {
    this.needToReport = true;
  }

  public String getDeviceId() {
    // NonNull indicates that the internallyDecreaseResourceReferenceCount has not been called.
    return Objects.nonNull(tablet) ? tablet.getDeviceId() : deviceId;
  }

  public EnrichedEvent getSourceEvent() {
    return sourceEvent;
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> processRowByRow(
      final BiConsumer<Row, RowCollector> consumer) {
    return initEventParser().processRowByRow(consumer);
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer) {
    return initEventParser().processTablet(consumer);
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned() {
    return isAligned;
  }

  public Tablet convertToTablet() {
    if (!shouldParseTimeOrPattern()) {
      return tablet;
    }
    return initEventParser().convertToTablet();
  }

  /////////////////////////// event parser ///////////////////////////

  private TabletInsertionEventParser initEventParser() {
    if (eventParser == null) {
      eventParser =
          tablet.getDeviceId().startsWith("root.")
              ? new TabletInsertionEventTreePatternParser(
                  pipeTaskMeta, this, tablet, isAligned, treePattern)
              : new TabletInsertionEventTablePatternParser(
                  pipeTaskMeta, this, tablet, isAligned, tablePattern);
    }
    return eventParser;
  }

  public long count() {
    final Tablet covertedTablet = shouldParseTimeOrPattern() ? convertToTablet() : tablet;
    return (long) covertedTablet.getRowSize() * covertedTablet.getSchemas().size();
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public PipeRawTabletInsertionEvent parseEventWithPatternOrTime() {
    return new PipeRawTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getTreeModelDatabaseName(),
        convertToTablet(),
        isAligned,
        pipeName,
        creationTime,
        pipeTaskMeta,
        this,
        needToReport);
  }

  public boolean hasNoNeedParsingAndIsEmpty() {
    return !shouldParseTimeOrPattern() && isTabletEmpty(tablet);
  }

  public static boolean isTabletEmpty(final Tablet tablet) {
    return Objects.isNull(tablet)
        || tablet.getRowSize() == 0
        || Objects.isNull(tablet.getSchemas())
        || tablet.getSchemas().isEmpty();
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeRawTabletInsertionEvent{tablet=%s, isAligned=%s, sourceEvent=%s, needToReport=%s, allocatedMemoryBlock=%s, eventParser=%s}",
            tablet, isAligned, sourceEvent, needToReport, allocatedMemoryBlock, eventParser)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeRawTabletInsertionEvent{tablet=%s, isAligned=%s, sourceEvent=%s, needToReport=%s, allocatedMemoryBlock=%s}",
            tablet,
            isAligned,
            sourceEvent == null ? "null" : sourceEvent.coreReportMessage(),
            needToReport,
            allocatedMemoryBlock)
        + " - "
        + super.coreReportMessage();
  }

  /////////////////////////// ReferenceTrackableEvent ///////////////////////////

  @Override
  protected void trackResource() {
    PipeDataNodeResourceManager.ref().trackPipeEventResource(this, eventResourceBuilder());
  }

  @Override
  public PipeEventResource eventResourceBuilder() {
    return new PipeRawTabletInsertionEventResource(
        this.isReleased, this.referenceCount, this.allocatedMemoryBlock);
  }

  private static class PipeRawTabletInsertionEventResource extends PipeEventResource {

    private final PipeTabletMemoryBlock allocatedMemoryBlock;

    private PipeRawTabletInsertionEventResource(
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
}
