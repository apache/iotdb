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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTablePatternParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTreePatternParser;
import org.apache.iotdb.db.pipe.metric.PipeDataNodeRemainingEventAndTimeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryPosition;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class PipeInsertNodeTabletInsertionEvent extends PipeInsertionEvent
    implements TabletInsertionEvent, ReferenceTrackableEvent, Accountable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeInsertNodeTabletInsertionEvent.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeInsertNodeTabletInsertionEvent.class)
          + RamUsageEstimator.shallowSizeOfInstance(WALEntryHandler.class)
          + RamUsageEstimator.shallowSizeOfInstance(WALEntryPosition.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicInteger.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicBoolean.class);

  private final WALEntryHandler walEntryHandler;
  private final boolean isAligned;
  private final boolean isGeneratedByPipe;

  private List<TabletInsertionEventParser> eventParsers;

  private final PartialPath devicePath;

  private ProgressIndex progressIndex;

  public PipeInsertNodeTabletInsertionEvent(
      final String databaseName,
      final WALEntryHandler walEntryHandler,
      final PartialPath devicePath,
      final ProgressIndex progressIndex,
      final boolean isAligned,
      final boolean isGeneratedByPipe) {
    this(
        null,
        databaseName,
        walEntryHandler,
        devicePath,
        progressIndex,
        isAligned,
        isGeneratedByPipe,
        null,
        0,
        null,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  private PipeInsertNodeTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseName,
      final WALEntryHandler walEntryHandler,
      final PartialPath devicePath,
      final ProgressIndex progressIndex,
      final boolean isAligned,
      final boolean isGeneratedByPipe,
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
    this.walEntryHandler = walEntryHandler;
    // Record device path here so there's no need to get it from InsertNode cache later.
    this.devicePath = devicePath;
    this.progressIndex = progressIndex;
    this.isAligned = isAligned;
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  public InsertNode getInsertNode() throws WALPipeException {
    return walEntryHandler.getInsertNode();
  }

  public ByteBuffer getByteBuffer() throws WALPipeException {
    return walEntryHandler.getByteBuffer();
  }

  // This method is a pre-determination of whether to use binary transfers.
  // If the insert node is null in cache, it means that we need to read the bytebuffer from the wal,
  // and when the pattern is default, we can transfer the bytebuffer directly without serializing or
  // deserializing
  public InsertNode getInsertNodeViaCacheIfPossible() {
    return walEntryHandler.getInsertNodeViaCacheIfPossible();
  }

  public String getDeviceId() {
    return Objects.nonNull(devicePath) ? devicePath.getFullPath() : null;
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    try {
      PipeDataNodeResourceManager.wal().pin(walEntryHandler);
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
            .increaseTabletEventCount(pipeName, creationTime);
        PipeDataNodeAgent.task().addFloatingMemoryUsageInByte(pipeName, ramBytesUsed());
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for memTable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    try {
      PipeDataNodeResourceManager.wal().unpin(walEntryHandler);
      // Release the parsers' memory.
      if (eventParsers != null) {
        eventParsers.clear();
        eventParsers = null;
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for memtable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    } finally {
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeAgent.task().decreaseFloatingMemoryUsageInByte(pipeName, ramBytesUsed());
        PipeDataNodeRemainingEventAndTimeMetrics.getInstance()
            .decreaseTabletEventCount(pipeName, creationTime);
      }
    }
  }

  @Override
  public void bindProgressIndex(final ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex == null ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  @Override
  public PipeInsertNodeTabletInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return new PipeInsertNodeTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getTreeModelDatabaseName(),
        walEntryHandler,
        devicePath,
        progressIndex,
        isAligned,
        isGeneratedByPipe,
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
    return isGeneratedByPipe;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    try {
      final InsertNode insertNode = getInsertNodeViaCacheIfPossible();
      if (Objects.isNull(insertNode)) {
        return true;
      }

      if (insertNode instanceof InsertRowNode) {
        final long timestamp = ((InsertRowNode) insertNode).getTime();
        return startTime <= timestamp && timestamp <= endTime;
      }

      if (insertNode instanceof InsertTabletNode) {
        final long[] timestamps = ((InsertTabletNode) insertNode).getTimes();
        if (Objects.isNull(timestamps) || timestamps.length == 0) {
          return false;
        }
        // We assume that `timestamps` is ordered.
        return startTime <= timestamps[timestamps.length - 1] && timestamps[0] <= endTime;
      }

      if (insertNode instanceof InsertRowsNode) {
        return ((InsertRowsNode) insertNode)
            .getInsertRowNodeList().stream()
                .anyMatch(
                    insertRowNode -> {
                      final long timestamp = insertRowNode.getTime();
                      return startTime <= timestamp && timestamp <= endTime;
                    });
      }

      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          "Exception occurred when determining the event time of PipeInsertNodeTabletInsertionEvent({}) overlaps with the time range: [{}, {}]. Returning true to ensure data integrity.",
          this,
          startTime,
          endTime,
          e);
      return true;
    }
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    try {
      final InsertNode insertNode = getInsertNodeViaCacheIfPossible();
      if (Objects.isNull(insertNode)) {
        return true;
      }

      if (insertNode instanceof RelationalInsertRowNode
          || insertNode instanceof RelationalInsertTabletNode
          || insertNode instanceof RelationalInsertRowsNode) {
        return true;
      }

      if (insertNode instanceof InsertRowNode || insertNode instanceof InsertTabletNode) {
        final PartialPath devicePartialPath = insertNode.getTargetPath();
        return Objects.isNull(devicePartialPath)
            || treePattern.mayOverlapWithDevice(devicePartialPath.getIDeviceIDAsFullDevice());
      }

      if (insertNode instanceof InsertRowsNode) {
        return ((InsertRowsNode) insertNode)
            .getInsertRowNodeList().stream()
                .anyMatch(
                    insertRowNode ->
                        Objects.isNull(insertRowNode.getTargetPath())
                            || treePattern.mayOverlapWithDevice(
                                insertRowNode.getTargetPath().getIDeviceIDAsFullDevice()));
      }

      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          "Exception occurred when determining the event time of PipeInsertNodeTabletInsertionEvent({}) overlaps with the time range: [{}, {}]. Returning true to ensure data integrity.",
          this,
          startTime,
          endTime,
          e);
      return true;
    }
  }

  /////////////////////////// TabletInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> processRowByRow(
      final BiConsumer<Row, RowCollector> consumer) {
    return initEventParsers().stream()
        .map(tabletInsertionEventParser -> tabletInsertionEventParser.processRowByRow(consumer))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer) {
    return initEventParsers().stream()
        .map(tabletInsertionEventParser -> tabletInsertionEventParser.processTablet(consumer))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned(final int i) {
    return initEventParsers().get(i).isAligned();
  }

  // TODO: for table model insertion, we need to get the database name
  public List<Tablet> convertToTablets() {
    return initEventParsers().stream()
        .map(TabletInsertionEventParser::convertToTablet)
        .collect(Collectors.toList());
  }

  /////////////////////////// event parser ///////////////////////////

  private List<TabletInsertionEventParser> initEventParsers() {
    try {
      if (eventParsers != null) {
        return eventParsers;
      }

      eventParsers = new ArrayList<>();
      final InsertNode node = getInsertNode();
      switch (node.getType()) {
        case INSERT_ROW:
        case INSERT_TABLET:
          eventParsers.add(
              new TabletInsertionEventTreePatternParser(pipeTaskMeta, this, node, treePattern));
          break;
        case INSERT_ROWS:
          for (final InsertRowNode insertRowNode : ((InsertRowsNode) node).getInsertRowNodeList()) {
            eventParsers.add(
                new TabletInsertionEventTreePatternParser(
                    pipeTaskMeta, this, insertRowNode, treePattern));
          }
          break;
        case RELATIONAL_INSERT_ROW:
        case RELATIONAL_INSERT_TABLET:
          eventParsers.add(
              new TabletInsertionEventTablePatternParser(pipeTaskMeta, this, node, tablePattern));
          break;
        case RELATIONAL_INSERT_ROWS:
          for (final InsertRowNode insertRowNode :
              ((RelationalInsertRowsNode) node).getInsertRowNodeList()) {
            eventParsers.add(
                new TabletInsertionEventTablePatternParser(
                    pipeTaskMeta, this, insertRowNode, tablePattern));
          }
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported node type " + node.getType());
      }

      final int size = eventParsers.size();
      if (size > 0) {
        eventParsers.get(size - 1).markAsNeedToReport();
      }

      return eventParsers;
    } catch (final Exception e) {
      throw new PipeException("Initialize data container error.", e);
    }
  }

  public long count() {
    long count = 0;
    for (final Tablet covertedTablet : convertToTablets()) {
      count += (long) covertedTablet.getRowSize() * covertedTablet.getSchemas().size();
    }
    return count;
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public List<PipeRawTabletInsertionEvent> toRawTabletInsertionEvents() {
    final List<PipeRawTabletInsertionEvent> events =
        initEventParsers().stream()
            .map(
                container ->
                    new PipeRawTabletInsertionEvent(
                        getRawIsTableModelEvent(),
                        getTreeModelDatabaseName(),
                        container.convertToTablet(),
                        container.isAligned(),
                        pipeName,
                        creationTime,
                        pipeTaskMeta,
                        this,
                        false))
            .filter(event -> !event.hasNoNeedParsingAndIsEmpty())
            .collect(Collectors.toList());

    final int size = events.size();
    if (size > 0) {
      events.get(size - 1).markAsNeedToReport();
    }

    return events;
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{walEntryHandler=%s, progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s, eventParsers=%s}",
            walEntryHandler, progressIndex, isAligned, isGeneratedByPipe, eventParsers)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{walEntryHandler=%s, progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s}",
            walEntryHandler, progressIndex, isAligned, isGeneratedByPipe)
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
    return new PipeInsertNodeTabletInsertionEventResource(
        this.isReleased, this.referenceCount, this.walEntryHandler);
  }

  // Notes:
  // 1. We only consider insertion event's memory for degrade and restart, because degrade/restart
  // may not be of use for releasing other events' memory.
  // 2. We do not consider eventParsers because they may not exist and if it is invoked, the event
  // will soon be released.
  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + (Objects.nonNull(devicePath) ? PartialPath.estimateSize(devicePath) : 0)
        + (Objects.nonNull(progressIndex) ? progressIndex.ramBytesUsed() : 0);
  }

  private static class PipeInsertNodeTabletInsertionEventResource extends PipeEventResource {

    private final WALEntryHandler walEntryHandler;

    private PipeInsertNodeTabletInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final WALEntryHandler walEntryHandler) {
      super(isReleased, referenceCount);
      this.walEntryHandler = walEntryHandler;
    }

    @Override
    protected void finalizeResource() {
      try {
        PipeDataNodeResourceManager.wal().unpin(walEntryHandler);
        // no need to release the containers' memory because it has already been GCed
      } catch (final Exception e) {
        LOGGER.warn(
            String.format(
                "Decrease reference count for memTable %d error.", walEntryHandler.getMemTableId()),
            e);
      }
    }
  }
}
