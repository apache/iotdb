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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.chunk.IChunkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MemPointIterator implements IPointReader {

  protected final Ordering scanOrder;
  // Only used when returning by batch
  protected Filter pushDownFilter;
  // Only used when returning by batch
  protected PaginationController paginationController =
      PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
  protected TimeRange timeRange;
  protected List<TsBlock> tsBlocks;
  protected boolean streamingQueryMemChunk = true;

  public MemPointIterator(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public abstract TsBlock getBatch(int tsBlockIndex);

  // When returned by nextBatch, the pagination controller and push down filter are applied. This
  // can only be used when there is no overlap with other pages.
  public abstract boolean hasNextBatch();

  public abstract TsBlock nextBatch();

  public abstract void encodeBatch(
      IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times);

  public void setPushDownFilter(Filter pushDownFilter) {
    this.pushDownFilter = pushDownFilter;
  }

  public void setLimitAndOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  public void setCurrentPageTimeRange(TimeRange timeRange) {
    this.timeRange = timeRange;
    skipToCurrentTimeRangeStartPosition();
  }

  protected void skipToCurrentTimeRangeStartPosition() {}

  protected boolean isCurrentTimeExceedTimeRange(long time) {
    return timeRange != null
        && (scanOrder.isAscending() ? (time > timeRange.getMax()) : (time < timeRange.getMin()));
  }

  public void setStreamingQueryMemChunk(boolean streamingQueryMemChunk) {
    this.streamingQueryMemChunk = streamingQueryMemChunk;
  }

  protected void addTsBlock(TsBlock tsBlock) {
    if (streamingQueryMemChunk) {
      return;
    }
    tsBlocks = tsBlocks == null ? new ArrayList<>() : tsBlocks;
    tsBlocks.add(tsBlock);
  }

  @Override
  public void close() throws IOException {
    if (tsBlocks != null) {
      tsBlocks.clear();
    }
  }
}
