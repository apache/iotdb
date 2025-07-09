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
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class PipeInsertNodeTabletInsertionEvent extends EnrichedEvent
    implements TabletInsertionEvent, ReferenceTrackableEvent, Accountable, AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeInsertNodeTabletInsertionEvent.class);
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PipeInsertNodeTabletInsertionEvent.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicInteger.class)
          + RamUsageEstimator.shallowSizeOfInstance(AtomicBoolean.class);

  private final AtomicReference<PipeTabletMemoryBlock> allocatedMemoryBlock;
  private volatile List<Tablet> tablets;

  private List<TabletInsertionDataContainer> dataContainers;

  private InsertNode insertNode;

  private ProgressIndex progressIndex;

  private long extractTime = 0;

  public PipeInsertNodeTabletInsertionEvent(final InsertNode insertNode) {
    this(insertNode, null, 0, null, null, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private PipeInsertNodeTabletInsertionEvent(
      final InsertNode insertNode,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    super(pipeName, creationTime, pipeTaskMeta, pattern, startTime, endTime);
    // Record device path here so there's no need to get it from InsertNode cache later.
    this.progressIndex = insertNode.getProgressIndex();
    this.insertNode = insertNode;
    this.allocatedMemoryBlock = new AtomicReference<>();
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  public ByteBuffer getByteBuffer() throws WALPipeException {
    return insertNode.serializeToByteBuffer();
  }

  public String getDeviceId() {
    return Objects.nonNull(insertNode.getDevicePath())
        ? insertNode.getDevicePath().getFullPath()
        : null;
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
      // release the containers' memory and close memory block
      if (dataContainers != null) {
        dataContainers.clear();
        dataContainers = null;
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
            .decreaseInsertNodeEventCount(pipeName, creationTime, System.nanoTime() - extractTime);
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
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    return new PipeInsertNodeTabletInsertionEvent(
        insertNode, pipeName, creationTime, pipeTaskMeta, pattern, startTime, endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return insertNode.isGeneratedByPipe();
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

      if (insertNode instanceof InsertRowNode || insertNode instanceof InsertTabletNode) {
        final PartialPath devicePartialPath = insertNode.getDevicePath();
        return Objects.isNull(devicePartialPath)
            || pipePattern.mayOverlapWithDevice(devicePartialPath.getFullPath());
      }

      if (insertNode instanceof InsertRowsNode) {
        return ((InsertRowsNode) insertNode)
            .getInsertRowNodeList().stream()
                .anyMatch(
                    insertRowNode ->
                        Objects.isNull(insertRowNode.getDevicePath())
                            || pipePattern.mayOverlapWithDevice(
                                insertRowNode.getDevicePath().getFullPath()));
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
    return initDataContainers().stream()
        .map(tabletInsertionDataContainer -> tabletInsertionDataContainer.processRowByRow(consumer))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(
      final BiConsumer<Tablet, RowCollector> consumer) {
    return initDataContainers().stream()
        .map(tabletInsertionDataContainer -> tabletInsertionDataContainer.processTablet(consumer))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned(final int i) {
    return initDataContainers().get(i).isAligned();
  }

  public synchronized List<Tablet> convertToTablets() {
    if (Objects.isNull(tablets)) {
      tablets =
          initDataContainers().stream()
              .map(TabletInsertionDataContainer::convertToTablet)
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

  /////////////////////////// dataContainer ///////////////////////////

  private List<TabletInsertionDataContainer> initDataContainers() {
    try {
      if (dataContainers != null) {
        return dataContainers;
      }

      dataContainers = new ArrayList<>();
      final InsertNode node = getInsertNode();
      switch (node.getType()) {
        case INSERT_ROW:
        case INSERT_TABLET:
          dataContainers.add(
              new TabletInsertionDataContainer(pipeTaskMeta, this, node, pipePattern));
          break;
        case INSERT_ROWS:
          for (final InsertRowNode insertRowNode : ((InsertRowsNode) node).getInsertRowNodeList()) {
            dataContainers.add(
                new TabletInsertionDataContainer(pipeTaskMeta, this, insertRowNode, pipePattern));
          }
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported node type " + node.getType());
      }

      final int size = dataContainers.size();
      if (size > 0) {
        dataContainers.get(size - 1).markAsNeedToReport();
      }

      return dataContainers;
    } catch (final Exception e) {
      throw new PipeException("Initialize data container error.", e);
    }
  }

  public long count() {
    long count = 0;
    for (final Tablet covertedTablet : convertToTablets()) {
      count += (long) covertedTablet.rowSize * covertedTablet.getSchemas().size();
    }
    return count;
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public List<PipeRawTabletInsertionEvent> toRawTabletInsertionEvents() {
    final List<PipeRawTabletInsertionEvent> events =
        initDataContainers().stream()
            .map(
                container ->
                    new PipeRawTabletInsertionEvent(
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
            "PipeInsertNodeTabletInsertionEvent{progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s, dataContainers=%s}",
            progressIndex,
            Objects.nonNull(insertNode) ? insertNode.isAligned() : null,
            Objects.nonNull(insertNode) ? insertNode.isGeneratedByPipe() : null,
            dataContainers)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s}",
            progressIndex,
            Objects.nonNull(insertNode) ? insertNode.isAligned() : null,
            Objects.nonNull(insertNode) ? insertNode.isGeneratedByPipe() : null)
        + " - "
        + super.coreReportMessage();
  }

  // Notes:
  // 1. We only consider insertion event's memory for degrade and restart, because degrade/restart
  // may not be of use for releasing other events' memory.
  // 2. We do not consider eventParsers because they may not exist and if it is invoked, the event
  // will soon be released.
  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + (Objects.nonNull(insertNode) ? InsertNodeMemoryEstimator.sizeOf(insertNode) : 0)
        + (Objects.nonNull(progressIndex) ? progressIndex.ramBytesUsed() : 0);
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
