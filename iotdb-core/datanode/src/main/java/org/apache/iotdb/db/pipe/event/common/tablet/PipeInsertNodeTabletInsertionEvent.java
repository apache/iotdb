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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
    this(walEntryHandler, progressIndex, isAligned, isGeneratedByPipe, null, null);
  }

  private PipeInsertNodeTabletInsertionEvent(
      WALEntryHandler walEntryHandler,
      ProgressIndex progressIndex,
      boolean isAligned,
      boolean isGeneratedByPipe,
      PipeTaskMeta pipeTaskMeta,
      String pattern) {
    super(pipeTaskMeta, pattern);
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
    return progressIndex;
  }

  @Override
  public PipeInsertNodeTabletInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeInsertNodeTabletInsertionEvent(
        walEntryHandler, progressIndex, isAligned, isGeneratedByPipe, pipeTaskMeta, pattern);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
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

  /////////////////////////// parsePattern ///////////////////////////

  public TabletInsertionEvent parseEventWithPattern() {
    return new PipeRawTabletInsertionEvent(convertToTablet(), isAligned, pipeTaskMeta, this, true);
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return "PipeInsertNodeTabletInsertionEvent{"
        + "walEntryHandler="
        + walEntryHandler
        + ", progressIndex="
        + progressIndex
        + ", isAligned="
        + isAligned
        + '}';
  }
}
