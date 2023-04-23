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

package org.apache.iotdb.db.mpp.execution.operator.window.subWindowQueue;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;

public abstract class AbstractWindowQueue {
  TimeColumn timeColumn;
  Column[] columns;
  Deque<TsBlock> queue;

  int valueColumnCount;

  long totalRowSize = 0;

  int cachedBytes = 0;
  long count;
  long interval;

  public AbstractWindowQueue(int valueCount, long interval) {
    genTsBlockColumns(valueCount);
    this.interval = interval;
    this.count = 0;
    this.queue = new LinkedList<>();
  }

  // add tsBlock in queue, and maintain a total row size
  public void cache(TsBlock tsBlock) {
    queue.add(tsBlock);
    totalRowSize += tsBlock.getPositionCount();
  }

  // check if queue has enough data to output a window
  public abstract boolean isReady();

  // add data of window to builder
  public abstract boolean buildWindow();

  public abstract void stepToNextWindow();

  // return the data in Builder
  public TsBlock build() {
    if (cachedBytes == 0) {
      return null;
    }

    if (count == interval || count == totalRowSize) {
      stepToNextWindow();
    }
    cachedBytes = 0;
    TsBlock newTsBlock = new TsBlock(timeColumn, columns);
    timeColumn = null;
    Arrays.fill(columns, null);

    return newTsBlock;
  }

  public void genTsBlockColumns(int valueColumnCount) {
    this.valueColumnCount = valueColumnCount;
    this.columns = new Column[valueColumnCount];
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  protected boolean writeTsBlock(TsBlock tsBlock) {
    TimeColumn newTimeColumns = tsBlock.getTimeColumn();
    Column[] newColumns = tsBlock.getValueColumns();

    timeColumn =
        timeColumn == null ? newTimeColumns : (TimeColumn) timeColumn.mergeColumn(newTimeColumns);
    for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
      columns[i] = columns[i] == null ? newColumns[i] : columns[i].mergeColumn(newColumns[i]);
    }

    cachedBytes += tsBlock.getRetainedSizeInBytes();
    count += tsBlock.getPositionCount();

    //    strict the number of rows in a TsBlock
    //    return cachedBytes >= TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    return false;
  }

  protected void writeWindowIndex(long windowIndex) {
    columns[valueColumnCount] =
        new RunLengthEncodedColumn(
            new LongColumn(1, Optional.empty(), new long[] {windowIndex}), cachedBytes);
  }
}
