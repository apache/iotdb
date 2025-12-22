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

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
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
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.collector.TabletCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class PipeRawTabletInsertionEvent extends PipeInsertionEvent
    implements TabletInsertionEvent, ReferenceTrackableEvent, AutoCloseable {

  // For better calculation
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeRawTabletInsertionEvent.class);
  private Tablet tablet;
  private String deviceId; // Only used when the tablet is released.
  private final boolean isAligned;

  private final EnrichedEvent sourceEvent;
  private boolean needToReport;

  private final PipeTabletMemoryBlock allocatedMemoryBlock;

  private TabletInsertionEventParser eventParser;

  private volatile ProgressIndex overridingProgressIndex;

  private PipeRawTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final String tableModelDataBaseName,
      final String treeModelDataBaseName,
      final Tablet tablet,
      final boolean isAligned,
      final EnrichedEvent sourceEvent,
      final boolean needToReport,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
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
        startTime,
        endTime,
        isTableModelEvent,
        databaseName,
        tableModelDataBaseName,
        treeModelDataBaseName);
    this.tablet = Objects.requireNonNull(tablet);
    this.isAligned = isAligned;
    this.sourceEvent = sourceEvent;
    this.needToReport = needToReport;

    // Allocate empty memory block, will be resized later.
    this.allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);

    addOnCommittedHook(
        () -> {
          if (shouldReportOnCommit) {
            eliminateProgressIndex();
          }
        });
  }

  public PipeRawTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final String tableModelDataBaseName,
      final String treeModelDataBaseName,
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
        tableModelDataBaseName,
        treeModelDataBaseName,
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        null,
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  public PipeRawTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final String tableModelDataBaseName,
      final String treeModelDataBaseName,
      final Tablet tablet,
      final boolean isAligned,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final boolean needToReport,
      final String userId,
      final String userName,
      final String cliHostname) {
    this(
        isTableModelEvent,
        databaseName,
        tableModelDataBaseName,
        treeModelDataBaseName,
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        null,
        null,
        userId,
        userName,
        cliHostname,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(final Tablet tablet, final boolean isAligned) {
    this(
        null,
        null,
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
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final boolean isAligned, final TreePattern treePattern) {
    this(
        null,
        null,
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
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  @TestOnly
  public PipeRawTabletInsertionEvent(
      final Tablet tablet, final long startTime, final long endTime) {
    this(
        null, null, null, null, tablet, false, null, false, null, 0, null, null, null, null, null,
        null, true, startTime, endTime);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    PipeDataNodeResourceManager.memory()
        .forceResize(
            allocatedMemoryBlock,
            PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet) + INSTANCE_SIZE);
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

    // Record the deviceId before the memory is released,
    // for later possibly updating the leader cache.
    deviceId = tablet.getDeviceId();

    // Actually release the occupied memory.
    tablet = null;
    eventParser = null;

    // Update metrics of the source event
    if (needToReport && shouldReportOnCommit && Objects.nonNull(pipeName)) {
      if (sourceEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        PipeDataNodeSinglePipeMetrics.getInstance()
            .updateInsertNodeTransferTimer(
                pipeName,
                creationTime,
                System.nanoTime()
                    - ((PipeInsertNodeTabletInsertionEvent) sourceEvent).getExtractTime());
      } else if (sourceEvent instanceof PipeTsFileInsertionEvent) {
        PipeDataNodeSinglePipeMetrics.getInstance()
            .updateTsFileTransferTimer(
                pipeName,
                creationTime,
                System.nanoTime() - ((PipeTsFileInsertionEvent) sourceEvent).getExtractTime());
      }
    }

    return true;
  }

  protected void eliminateProgressIndex() {
    if (needToReport) {
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
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    return new PipeRawTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getSourceDatabaseNameFromDataRegion(),
        getRawTableModelDataBase(),
        getRawTreeModelDataBase(),
        tablet,
        isAligned,
        sourceEvent,
        needToReport,
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userId,
        userName,
        cliHostname,
        skipIfNoPrivileges,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    throw new UnsupportedOperationException("isGeneratedByPipe() is not supported!");
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    final long[] timestamps = tablet.getTimestamps();
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

  // This getter is reserved for user-defined plugins
  public boolean isNeedToReport() {
    return needToReport;
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
    try {
      return initEventParser().processRowByRow(consumer);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer) {
    try {
      return initEventParser().processTablet(consumer);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> processTabletWithCollect(
      BiConsumer<Tablet, TabletCollector> consumer) {
    return initEventParser().processTabletWithCollect(consumer);
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
                  pipeTaskMeta,
                  this,
                  tablet,
                  isAligned,
                  treePattern,
                  shouldParse4Privilege
                      ? new UserEntity(Long.parseLong(userId), userName, cliHostname)
                      : null)
              : new TabletInsertionEventTablePatternParser(
                  pipeTaskMeta, this, tablet, isAligned, tablePattern);
    }
    return eventParser;
  }

  public long count() throws IllegalPathException {
    final Tablet convertedTablet = shouldParseTimeOrPattern() ? convertToTablet() : tablet;
    return (long) convertedTablet.getRowSize() * convertedTablet.getSchemas().size();
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public PipeRawTabletInsertionEvent parseEventWithPatternOrTime() throws IllegalPathException {
    return new PipeRawTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getSourceDatabaseNameFromDataRegion(),
        getRawTableModelDataBase(),
        getRawTreeModelDataBase(),
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

  /////////////////////////// AutoCloseable ///////////////////////////

  @Override
  public void close() {
    // The semantic of close is to release the memory occupied by parsing, this method does nothing
    // to unify the external close semantic:
    //   1. PipeRawTabletInsertionEvent: the tablet occupying memory upon construction, even when
    // parsing is involved.
    //   2. PipeInsertNodeTabletInsertionEvent: the tablet is only constructed when it's actually
    // involved in parsing.
  }
}
