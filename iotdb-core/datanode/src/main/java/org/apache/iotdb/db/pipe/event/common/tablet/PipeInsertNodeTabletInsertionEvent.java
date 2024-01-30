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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;

public class PipeInsertNodeTabletInsertionEvent extends EnrichedEvent
    implements TabletInsertionEvent {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeInsertNodeTabletInsertionEvent.class);

  private final WALEntryHandler walEntryHandler;
  private final ProgressIndex progressIndex;
  private final boolean isAligned;
  private final boolean isGeneratedByPipe;

  private TabletInsertionDataContainer dataContainer;

  public PipeInsertNodeTabletInsertionEvent(
      WALEntryHandler walEntryHandler,
      ProgressIndex progressIndex,
      boolean isAligned,
      boolean isGeneratedByPipe) {
    this(
        walEntryHandler,
        progressIndex,
        isAligned,
        isGeneratedByPipe,
        null,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  private PipeInsertNodeTabletInsertionEvent(
      WALEntryHandler walEntryHandler,
      ProgressIndex progressIndex,
      boolean isAligned,
      boolean isGeneratedByPipe,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      String pattern,
      long startTime,
      long endTime) {
    super(pipeName, pipeTaskMeta, pattern, startTime, endTime);
    this.walEntryHandler = walEntryHandler;
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

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.wal().pin(walEntryHandler);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for memtable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.wal().unpin(walEntryHandler);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for memtable %d error. Holder Message: %s",
              walEntryHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex == null ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  @Override
  public PipeInsertNodeTabletInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeInsertNodeTabletInsertionEvent(
        walEntryHandler,
        progressIndex,
        isAligned,
        isGeneratedByPipe,
        pipeName,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean isEventTimeOverlappedWithTimeRange() {
    try {
      InsertNode insertNode = getInsertNode();
      if (insertNode instanceof InsertRowNode) {
        long timestamp = ((InsertRowNode) insertNode).getTime();
        return startTime <= timestamp && timestamp <= endTime;
      } else if (insertNode instanceof InsertTabletNode) {
        long[] timestamps = ((InsertTabletNode) insertNode).getTimes();
        if (Objects.isNull(timestamps) || timestamps.length == 0) {
          return false;
        }
        // We assume that `timestamps` is ordered.
        return startTime <= timestamps[timestamps.length - 1] && timestamps[0] <= endTime;
      } else {
        throw new UnSupportedDataTypeException(
            String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
      }
    } catch (Exception e) {
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
  public Iterable<TabletInsertionEvent> processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    try {
      if (dataContainer == null) {
        dataContainer =
            new TabletInsertionDataContainer(pipeTaskMeta, this, getInsertNode(), getPattern());
      }
      return dataContainer.processRowByRow(consumer);
    } catch (Exception e) {
      throw new PipeException("Process row by row error.", e);
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    try {
      if (dataContainer == null) {
        dataContainer =
            new TabletInsertionDataContainer(pipeTaskMeta, this, getInsertNode(), getPattern());
      }
      return dataContainer.processTablet(consumer);
    } catch (Exception e) {
      throw new PipeException("Process tablet error.", e);
    }
  }

  /////////////////////////// convertToTablet ///////////////////////////

  public boolean isAligned() {
    return isAligned;
  }

  public Tablet convertToTablet() {
    try {
      if (dataContainer == null) {
        dataContainer =
            new TabletInsertionDataContainer(pipeTaskMeta, this, getInsertNode(), getPattern());
      }
      return dataContainer.convertToTablet();
    } catch (Exception e) {
      throw new PipeException("Convert to tablet error.", e);
    }
  }

  /////////////////////////// parsePatternOrTime ///////////////////////////

  public TabletInsertionEvent parseEventWithPatternOrTime() {
    return new PipeRawTabletInsertionEvent(
        convertToTablet(), isAligned, pipeName, pipeTaskMeta, this, true);
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeInsertNodeTabletInsertionEvent{walEntryHandler=%s, progressIndex=%s, isAligned=%s, isGeneratedByPipe=%s, dataContainer=%s}",
            walEntryHandler, progressIndex, isAligned, isGeneratedByPipe, dataContainer)
        + " - "
        + super.toString();
  }
}
