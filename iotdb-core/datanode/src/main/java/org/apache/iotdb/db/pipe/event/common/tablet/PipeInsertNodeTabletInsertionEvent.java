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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTablePatternParser;
import org.apache.iotdb.db.pipe.event.common.tablet.parser.TabletInsertionEventTreePatternParser;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.collector.TabletCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class PipeInsertNodeTabletInsertionEvent extends PipeInsertionEvent
    implements TabletInsertionEvent, ReferenceTrackableEvent, Accountable, AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeInsertNodeTabletInsertionEvent.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeInsertNodeTabletInsertionEvent.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicInteger.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicBoolean.class)
          + RamUsageEstimator.shallowSizeOf(Boolean.class);

  private final AtomicReference<PipeTabletMemoryBlock> allocatedMemoryBlock;
  private volatile List<Tablet> tablets;

  private List<TabletInsertionEventParser> eventParsers;

  private InsertNode insertNode;

  private ProgressIndex progressIndex;
  private long bytes = Long.MIN_VALUE;

  private long extractTime = 0;

  public PipeInsertNodeTabletInsertionEvent(
      final Boolean isTableModel,
      final String databaseNameFromDataRegion,
      final InsertNode insertNode) {
    this(
        isTableModel,
        databaseNameFromDataRegion,
        insertNode,
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

  public PipeInsertNodeTabletInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseNameFromDataRegion,
      final InsertNode insertNode,
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
        databaseNameFromDataRegion);
    this.insertNode = insertNode;
    this.progressIndex = insertNode.getProgressIndex();

    this.allocatedMemoryBlock = new AtomicReference<>();
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  public ByteBuffer getByteBuffer() throws WALPipeException {
    final InsertNode node = insertNode;
    if (Objects.isNull(node)) {
      throw new PipeException("InsertNode has been released");
    }
    return node.serializeToByteBuffer();
  }

  public String getDeviceId() {
    final InsertNode node = insertNode;
    if (Objects.isNull(node)) {
      return null;
    }
    final PartialPath targetPath = node.getTargetPath();
    return Objects.nonNull(targetPath) ? targetPath.getFullPath() : null;
  }

  public long getExtractTime() {
    return extractTime;
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    extractTime = System.nanoTime();
    try {
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeSinglePipeMetrics.getInstance()
            .increaseInsertNodeEventCount(pipeName, creationTime);
        PipeDataNodeAgent.task()
            .addFloatingMemoryUsageInByte(pipeName, creationTime, ramBytesUsed());
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format("Increase reference count error. Holder Message: %s", holderMessage), e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    try {
      // release the parsers' memory and close memory block
      if (eventParsers != null) {
        eventParsers.clear();
        eventParsers = null;
      }
      close();
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format("Decrease reference count error. Holder Message: %s", holderMessage), e);
      return false;
    } finally {
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeAgent.task()
            .decreaseFloatingMemoryUsageInByte(pipeName, creationTime, ramBytesUsed());
        PipeDataNodeSinglePipeMetrics.getInstance()
            .decreaseInsertNodeEventCount(
                pipeName,
                creationTime,
                shouldReportOnCommit ? System.nanoTime() - extractTime : -1);
      }
      insertNode = null;
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
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    final InsertNode node = insertNode;
    if (Objects.isNull(node)) {
      throw new PipeException("InsertNode has been released");
    }
    return new PipeInsertNodeTabletInsertionEvent(
        getRawIsTableModelEvent(),
        getSourceDatabaseNameFromDataRegion(),
        node,
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
    final InsertNode node = insertNode;
    if (Objects.isNull(node)) {
      throw new PipeException("InsertNode has been released");
    }
    return node.isGeneratedByPipe();
  }

  @Override
  public void throwIfNoPrivilege() throws Exception {
    if (skipIfNoPrivileges) {
      return;
    }
    final InsertNode node = insertNode;
    if (Objects.isNull(node)) {
      // Event is released, skip privilege check
      return;
    }
    if (Objects.nonNull(node.getTargetPath())) {
      if (isTableModelEvent()) {
        checkTableName(
            DeviceIDFactory.getInstance().getDeviceID(node.getTargetPath()).getTableName());
      } else {
        checkTreePattern(node.getDeviceID(), node.getMeasurements());
      }
    } else if (insertNode instanceof InsertRowsNode) {
      for (final InsertNode subNode : ((InsertRowsNode) node).getInsertRowNodeList()) {
        if (isTableModelEvent()) {
          checkTableName(
              DeviceIDFactory.getInstance().getDeviceID(subNode.getTargetPath()).getTableName());
        } else {
          checkTreePattern(subNode.getDeviceID(), subNode.getMeasurements());
        }
      }
    }
  }

  private void checkTableName(final String tableName) {
    if (!AuthorityChecker.getAccessControl()
        .checkCanSelectFromTable4Pipe(
            userName,
            new QualifiedObjectName(getTableModelDatabaseName(), tableName),
            new UserEntity(Long.parseLong(userId), userName, cliHostname))) {
      throw new AccessDeniedException(
          String.format(
              "No privilege for SELECT for user %s at table %s.%s",
              userName, tableModelDatabaseName, tableName));
    }
  }

  private void checkTreePattern(final IDeviceID deviceID, final String[] measurements)
      throws IllegalPathException {
    final List<MeasurementPath> measurementList = new ArrayList<>();
    for (final String measurement : measurements) {
      if (treePattern.matchesMeasurement(deviceID, measurement)) {
        measurementList.add(new MeasurementPath(deviceID, measurement));
      }
    }
    final TSStatus status =
        AuthorityChecker.getAccessControl()
            .checkSeriesPrivilege4Pipe(
                new UserEntity(Long.parseLong(userId), userName, cliHostname),
                measurementList,
                PrivilegeType.READ_DATA);
    if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
      if (skipIfNoPrivileges) {
        shouldParse4Privilege = true;
      } else {
        throw new AccessDeniedException(status.getMessage());
      }
    }
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    try {
      final InsertNode insertNode = getInsertNode();
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
      final InsertNode insertNode = getInsertNode();
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

  @Override
  public Iterable<TabletInsertionEvent> processTabletWithCollect(
      BiConsumer<Tablet, TabletCollector> consumer) {
    return initEventParsers().stream()
        .map(
            tabletInsertionEventParser ->
                tabletInsertionEventParser.processTabletWithCollect(consumer))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned(final int i) {
    return initEventParsers().get(i).isAligned();
  }

  // TODO: for table model insertion, we need to get the database name
  public synchronized List<Tablet> convertToTablets() {
    if (Objects.isNull(tablets)) {
      tablets =
          initEventParsers().stream()
              .map(TabletInsertionEventParser::convertToTablet)
              .collect(Collectors.toList());
      allocatedMemoryBlock.compareAndSet(
          null,
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(
                  tablets.stream()
                      .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
                      .reduce(Long::sum)
                      .orElse(0L)));
    }
    return tablets;
  }

  /////////////////////////// event parser ///////////////////////////

  private List<TabletInsertionEventParser> initEventParsers() {
    try {
      if (eventParsers != null) {
        return eventParsers;
      }

      eventParsers = new ArrayList<>();
      final InsertNode node = getInsertNode();
      if (Objects.isNull(node)) {
        throw new PipeException("InsertNode has been released");
      }
      switch (node.getType()) {
        case INSERT_ROW:
        case INSERT_TABLET:
          eventParsers.add(
              new TabletInsertionEventTreePatternParser(
                  pipeTaskMeta,
                  this,
                  node,
                  treePattern,
                  shouldParse4Privilege
                      ? new UserEntity(Long.parseLong(userId), userName, cliHostname)
                      : null));
          break;
        case INSERT_ROWS:
          for (final InsertRowNode insertRowNode : ((InsertRowsNode) node).getInsertRowNodeList()) {
            eventParsers.add(
                new TabletInsertionEventTreePatternParser(
                    pipeTaskMeta,
                    this,
                    insertRowNode,
                    treePattern,
                    shouldParse4Privilege
                        ? new UserEntity(Long.parseLong(userId), userName, cliHostname)
                        : null));
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
                        getSourceDatabaseNameFromDataRegion(),
                        getRawTableModelDataBase(),
                        getRawTreeModelDataBase(),
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
    final InsertNode node = insertNode;
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s, eventParsers=%s}",
            progressIndex,
            Objects.nonNull(node) ? node.isAligned() : null,
            Objects.nonNull(node) ? node.isGeneratedByPipe() : null,
            eventParsers)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    final InsertNode node = insertNode;
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s}",
            progressIndex,
            Objects.nonNull(node) ? node.isAligned() : null,
            Objects.nonNull(node) ? node.isGeneratedByPipe() : null)
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
        this.isReleased, this.referenceCount, this.allocatedMemoryBlock);
  }

  // Notes:
  // 1. We only consider insertion event's memory for degrading, because degrading may not be of use
  // for releasing other events' memory.
  // 2. We do not consider eventParsers and database names because they may not exist and if it is
  // invoked, the event will soon be released.
  @Override
  public long ramBytesUsed() {
    if (bytes > 0) {
      return bytes;
    }
    final InsertNode node = insertNode;
    bytes =
        INSTANCE_SIZE
            + (Objects.nonNull(node) ? InsertNodeMemoryEstimator.sizeOf(node) : 0)
            + (Objects.nonNull(progressIndex) ? progressIndex.ramBytesUsed() : 0);
    return bytes;
  }

  private static class PipeInsertNodeTabletInsertionEventResource extends PipeEventResource {

    private final AtomicReference<PipeTabletMemoryBlock> allocatedMemoryBlock;

    private PipeInsertNodeTabletInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final AtomicReference<PipeTabletMemoryBlock> allocatedMemoryBlock) {
      super(isReleased, referenceCount);
      this.allocatedMemoryBlock = allocatedMemoryBlock;
    }

    @Override
    protected void finalizeResource() {
      try {
        allocatedMemoryBlock.getAndUpdate(
            memoryBlock -> {
              if (Objects.nonNull(memoryBlock)) {
                memoryBlock.close();
              }
              return null;
            });
      } catch (final Exception e) {
        LOGGER.warn("Decrease reference count error.", e);
      }
    }
  }

  /////////////////////////// AutoCloseable ///////////////////////////

  @Override
  public synchronized void close() {
    allocatedMemoryBlock.getAndUpdate(
        memoryBlock -> {
          if (Objects.nonNull(memoryBlock)) {
            memoryBlock.close();
          }
          return null;
        });
    tablets = null;
  }
}
