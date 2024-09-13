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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent extends EnrichedEvent implements TabletInsertionEvent {

  private Tablet tablet;
  private String deviceId; // Only used when the tablet is released.
  private final boolean isAligned;

  private final EnrichedEvent sourceEvent;
  private boolean needToReport;

  private PipeTabletMemoryBlock allocatedMemoryBlock;

  private TabletInsertionDataContainer dataContainer;

  private ProgressIndex overridingProgressIndex;

  private PipeRawTabletInsertionEvent(
      final Tablet tablet,
      final boolean isAligned,
      final EnrichedEvent sourceEvent,
      final boolean needToReport,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    super(pipeName, creationTime, pipeTaskMeta, pattern, startTime, endTime);
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.sourceEvent = sourceEvent;
    this.needToReport = needToReport;
  }

  public PipeRawTabletInsertionEvent(
      final Tablet tablet,
      final boolean isAligned,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final boolean needToReport) {
    this(
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(final Tablet tablet, final boolean isAligned) {
    this(tablet, isAligned, null, false, null, 0, null, null, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final boolean isAligned, final PipePattern pattern) {
    this(tablet, isAligned, null, false, null, 0, null, pattern, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final long startTime, final long endTime) {
    this(tablet, false, null, false, null, 0, null, null, startTime, endTime);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(
                PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet));
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    allocatedMemoryBlock.close();

    // Record the deviceId before the memory is released,
    // for later possibly updating the leader cache.
    deviceId = tablet.getDeviceId();

    // Actually release the occupied memory.
    tablet = null;
    dataContainer = null;
    return true;
  }

  @Override
  protected void reportProgress() {
    if (needToReport) {
      super.reportProgress();
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
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    return new PipeRawTabletInsertionEvent(
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pattern,
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
    final String deviceId = getDeviceId();
    return Objects.isNull(deviceId)
        || pipePattern.mayOverlapWithDevice(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId));
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
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, pipePattern);
    }
    return dataContainer.processRowByRow(consumer);
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer) {
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, pipePattern);
    }
    return dataContainer.processTablet(consumer);
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned() {
    return isAligned;
  }

  public Tablet convertToTablet() {
    if (!shouldParseTimeOrPattern()) {
      return tablet;
    }

    // if notNullPattern is not "root", we need to convert the tablet
    if (dataContainer == null) {
      dataContainer =
          new TabletInsertionDataContainer(pipeTaskMeta, this, tablet, isAligned, pipePattern);
    }
    return dataContainer.convertToTablet();
  }

  public long count() {
    final Tablet covertedTablet = shouldParseTimeOrPattern() ? convertToTablet() : tablet;
    return (long) covertedTablet.rowSize * covertedTablet.getSchemas().size();
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public PipeRawTabletInsertionEvent parseEventWithPatternOrTime() {
    return new PipeRawTabletInsertionEvent(
        convertToTablet(), isAligned, pipeName, creationTime, pipeTaskMeta, this, needToReport);
  }

  public boolean hasNoNeedParsingAndIsEmpty() {
    return !shouldParseTimeOrPattern() && isTabletEmpty(tablet);
  }

  public static boolean isTabletEmpty(final Tablet tablet) {
    return Objects.isNull(tablet)
        || tablet.rowSize == 0
        || Objects.isNull(tablet.getSchemas())
        || tablet.getSchemas().isEmpty();
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeRawTabletInsertionEvent{tablet=%s, isAligned=%s, sourceEvent=%s, needToReport=%s, allocatedMemoryBlock=%s, dataContainer=%s}",
            tablet, isAligned, sourceEvent, needToReport, allocatedMemoryBlock, dataContainer)
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
}
