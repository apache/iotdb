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

package org.apache.iotdb.db.queryengine.execution.operator.process.gapfill;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

abstract class AbstractGapFillOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractGapFillOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;
  protected final int outputColumnCount;
  private final int timeColumnIndex;
  private final TsBlockBuilder resultBuilder;

  // start time(inclusive) of gapfill, already adjust according to the third parameter(origin) of
  // date_bin_gapfill
  private final long startTime;
  // end time(inclusive) of gapfill, already adjust according to the third parameter(origin) of
  // date_bin_gapfill
  private final long endTime;

  protected long currentTime;
  private SortKey lastGroupKey = null;

  AbstractGapFillOperator(
      OperatorContext operatorContext,
      Operator child,
      int timeColumnIndex,
      long startTime,
      long endTime,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.outputColumnCount = dataTypes.size();
    this.timeColumnIndex = timeColumnIndex;
    this.startTime = startTime;
    this.currentTime = startTime;
    this.endTime = endTime;
    this.resultBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public TsBlock next() throws Exception {

    // no more data, we need to gap fill the remaining time interval of previous group
    if (!child.hasNextWithTimer()) {
      if (hasRemainingGapInPreviousGroup()) {
        resultBuilder.reset();
        fillGaps(lastGroupKey.tsBlock, lastGroupKey.rowIndex, endTime);
        return resultBuilder.build(
            new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
      } else {
        return null;
      }
    }

    TsBlock block = child.nextWithTimer();
    if (block == null || block.isEmpty()) {
      return null;
    }
    checkArgument(
        outputColumnCount == block.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");

    resultBuilder.reset();
    SortKey previousGroupKey = lastGroupKey;
    int size = block.getPositionCount();
    // current TsBlock's first row is not same group as the last row of previous TsBlock
    // we need to gap fill the remaining time interval of last group and then reset the gap fill
    // time iterator
    for (int i = 0; i < size; i++) {
      SortKey currentGroupKey = new SortKey(block, i);
      if (isNewGroup(currentGroupKey, previousGroupKey)) {
        // don't belong to same group, we need to gap fill the remaining time interval of previous
        // group and then reset the gap fill time iterator
        if (currentTime <= endTime) {
          fillGaps(previousGroupKey.tsBlock, previousGroupKey.rowIndex, endTime);
        }
        resetTimeIterator();
        previousGroupKey = currentGroupKey;
      } else if (previousGroupKey == null) {
        previousGroupKey = currentGroupKey;
      }

      Column timeColumn = block.getColumn(timeColumnIndex);
      // -1 because we should not include current row, current row will be appended in
      // writeCurrentRow
      long currentEndTime =
          timeColumn.isNull(i) ? endTime : block.getColumn(timeColumnIndex).getLong(i) - 1;
      fillGaps(block, i, currentEndTime);
      writeCurrentRow(block, i);
    }
    lastGroupKey = new SortKey(block, size - 1);
    return resultBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
  }

  private void resetTimeIterator() {
    currentTime = startTime;
  }

  private void writeCurrentRow(TsBlock block, int rowIndex) {
    resultBuilder.declarePosition();
    for (int i = 0; i < outputColumnCount; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      Column column = block.getColumn(i);
      if (column.isNull(rowIndex)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(column, rowIndex);
      }
    }
    nextTime();
  }

  private void fillGaps(TsBlock block, int rowIndex, long currentEndTime) {
    while (currentTime <= currentEndTime) {
      gapFillRow(currentTime, block, rowIndex);
      nextTime();
    }
  }

  private void gapFillRow(long time, TsBlock block, int rowIndex) {
    resultBuilder.declarePosition();
    for (int i = 0; i < outputColumnCount; i++) {
      ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
      if (i == timeColumnIndex) { // time column
        columnBuilder.writeLong(time);
      } else if (isGroupKeyColumn(i)) { // group keys column
        Column column = block.getColumn(i);
        if (column.isNull(rowIndex)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, rowIndex);
        }
      } else { // other columns just append null
        columnBuilder.appendNull();
      }
    }
  }

  abstract boolean isNewGroup(SortKey currentGroupKey, SortKey previousGroupKey);

  abstract boolean isGroupKeyColumn(int index);

  abstract void nextTime();

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public void close() throws Exception {
    child.close();
    lastGroupKey = null;
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished() && !hasRemainingGapInPreviousGroup();
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNextWithTimer() || hasRemainingGapInPreviousGroup();
  }

  private boolean hasRemainingGapInPreviousGroup() {
    return lastGroupKey != null && currentTime < endTime;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // while doing previous fill, we may need to copy the corresponding column if there
    // exists null values so the max peek memory may be double
    return 2 * child.calculateMaxPeekMemory() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
