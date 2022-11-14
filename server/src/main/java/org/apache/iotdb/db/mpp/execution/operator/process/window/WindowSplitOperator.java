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

package org.apache.iotdb.db.mpp.execution.operator.process.window;

import org.apache.iotdb.db.mpp.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.TsBlockUtil;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class WindowSplitOperator implements ProcessOperator {

  protected final OperatorContext operatorContext;

  protected final Operator child;
  protected TsBlock inputTsBlock;
  protected boolean canCallNext;

  private final ITimeRangeIterator sampleTimeRangeIterator;
  private TimeRange curTimeRange;

  private final ITimeRangeIterator sampleTimeRangeSliceIterator;
  private TimeRange curTimeRangeSlice;

  private final TsBlockBuilder resultTsBlockBuilder;

  public WindowSplitOperator(
      OperatorContext operatorContext,
      Operator child,
      ITimeRangeIterator sampleTimeRangeIterator,
      ITimeRangeIterator sampleTimeRangeSliceIterator,
      List<TSDataType> outputDataTypes) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.sampleTimeRangeIterator = sampleTimeRangeIterator;
    this.sampleTimeRangeSliceIterator = sampleTimeRangeSliceIterator;
    this.resultTsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    // reset operator state
    canCallNext = true;

    if (curTimeRange == null && sampleTimeRangeIterator.hasNextTimeRange()) {
      curTimeRange = sampleTimeRangeIterator.nextTimeRange();
    }

    while (curTimeRangeSlice == null && sampleTimeRangeSliceIterator.hasNextTimeRange()) {
      curTimeRangeSlice = sampleTimeRangeSliceIterator.nextTimeRange();
      if (curTimeRangeSlice.getMin() > curTimeRange.getMax()) {
        if (sampleTimeRangeIterator.hasNextTimeRange()) {
          curTimeRange = sampleTimeRangeIterator.nextTimeRange();
        }
        if (curTimeRangeSlice.getMin() > curTimeRange.getMax()
            || curTimeRangeSlice.getMax() < curTimeRange.getMin()) {
          curTimeRangeSlice = null;
        }
      }
    }

    if (!fetchData()) {
      return null;
    } else {
      curTimeRangeSlice = null;
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      return resultTsBlock;
    }
  }

  private boolean fetchData() {
    while (!consumeInput()) {
      // NOTE: child.next() can only be invoked once
      if (child.hasNext() && canCallNext) {
        inputTsBlock = child.next();
        canCallNext = false;
      } else {
        return false;
      }
    }
    return true;
  }

  private boolean consumeInput() {
    if (inputTsBlock == null) {
      return false;
    }

    inputTsBlock = TsBlockUtil.skipPointsOutOfTimeRange(inputTsBlock, curTimeRangeSlice, true);
    if (inputTsBlock == null) {
      return false;
    }

    for (int readIndex = 0; readIndex < inputTsBlock.getPositionCount(); readIndex++) {
      long time = inputTsBlock.getTimeByIndex(readIndex);
      if (curTimeRangeSlice.contains(time)) {
        writeData(readIndex);
      } else {
        inputTsBlock = inputTsBlock.subTsBlock(readIndex);
        return true;
      }
    }
    return false;
  }

  private void writeData(int readIndex) {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    timeColumnBuilder.writeLong(inputTsBlock.getTimeByIndex(readIndex));
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    for (int columnIndex = 0; columnIndex < columnBuilders.length; columnIndex++) {
      columnBuilders[columnIndex].write(inputTsBlock.getColumn(columnIndex), readIndex);
    }
    resultTsBlockBuilder.declarePosition();
  }

  @Override
  public boolean hasNext() {
    return curTimeRangeSlice != null || sampleTimeRangeSliceIterator.hasNextTimeRange();
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }
}
