/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

/**
 * TableChangePointOperator implements "last of run" change-point detection. It buffers the latest
 * row seen and emits it when the monitored column changes value or input is exhausted. The output
 * includes all child columns plus a "next" column containing the new value (or NULL at end).
 *
 * <p>This replaces a Filter(Window(LEAD(...))) pattern: SELECT * FROM (SELECT *, LEAD(col) OVER
 * (PARTITION BY ... ORDER BY ...) AS next FROM t) WHERE col != next OR next IS NULL
 */
public class TableChangePointOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableChangePointOperator.class);

  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final int measurementColumnIndex;
  private final TSDataType measurementDataType;
  private final List<TSDataType> childOutputTypes;
  private final TsBlockBuilder tsBlockBuilder;

  private boolean hasBufferedRow = false;
  private TsBlock bufferedBlock;
  private int bufferedPosition;

  private boolean childExhausted = false;
  private boolean finished = false;

  public TableChangePointOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      int measurementColumnIndex,
      List<TSDataType> childOutputTypes) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.measurementColumnIndex = measurementColumnIndex;
    this.childOutputTypes = childOutputTypes;
    this.measurementDataType = childOutputTypes.get(measurementColumnIndex);

    List<TSDataType> outputDataTypes = new ArrayList<>(childOutputTypes);
    outputDataTypes.add(measurementDataType);
    this.tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    if (finished) {
      return null;
    }

    tsBlockBuilder.reset();

    if (!childExhausted) {
      TsBlock inputBlock = inputOperator.nextWithTimer();
      if (inputBlock == null) {
        return null;
      }
      processBlock(inputBlock);
    }

    if (childExhausted && hasBufferedRow) {
      emitBufferedRow(true);
      hasBufferedRow = false;
    }

    if (childExhausted && !hasBufferedRow) {
      finished = true;
    }

    if (tsBlockBuilder.isEmpty()) {
      return null;
    }

    return tsBlockBuilder.build(
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
  }

  private void processBlock(TsBlock inputBlock) {
    Column measurementColumn = inputBlock.getColumn(measurementColumnIndex);
    int rowCount = inputBlock.getPositionCount();

    for (int i = 0; i < rowCount; i++) {
      if (measurementColumn.isNull(i)) {
        continue;
      }

      if (!hasBufferedRow) {
        bufferRow(inputBlock, i);
        continue;
      }

      if (valueChanged(measurementColumn, i)) {
        emitBufferedRowWithNext(inputBlock, i);
        bufferRow(inputBlock, i);
      } else {
        bufferRow(inputBlock, i);
      }
    }
  }

  private boolean valueChanged(Column newMeasurementColumn, int newPosition) {
    Column bufferedMeasurementColumn = bufferedBlock.getColumn(measurementColumnIndex);
    switch (measurementDataType) {
      case BOOLEAN:
        return bufferedMeasurementColumn.getBoolean(bufferedPosition)
            != newMeasurementColumn.getBoolean(newPosition);
      case INT32:
        return bufferedMeasurementColumn.getInt(bufferedPosition)
            != newMeasurementColumn.getInt(newPosition);
      case INT64:
      case TIMESTAMP:
        return bufferedMeasurementColumn.getLong(bufferedPosition)
            != newMeasurementColumn.getLong(newPosition);
      case FLOAT:
        return Float.compare(
                bufferedMeasurementColumn.getFloat(bufferedPosition),
                newMeasurementColumn.getFloat(newPosition))
            != 0;
      case DOUBLE:
        return Double.compare(
                bufferedMeasurementColumn.getDouble(bufferedPosition),
                newMeasurementColumn.getDouble(newPosition))
            != 0;
      case TEXT:
      case STRING:
      case BLOB:
        Binary bufferedVal = bufferedMeasurementColumn.getBinary(bufferedPosition);
        Binary newVal = newMeasurementColumn.getBinary(newPosition);
        return !bufferedVal.equals(newVal);
      default:
        return false;
    }
  }

  private void bufferRow(TsBlock block, int position) {
    hasBufferedRow = true;
    bufferedBlock = block;
    bufferedPosition = position;
  }

  private void emitBufferedRowWithNext(TsBlock nextBlock, int nextPosition) {
    for (int col = 0; col < childOutputTypes.size(); col++) {
      Column column = bufferedBlock.getColumn(col);
      ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(col);
      if (column.isNull(bufferedPosition)) {
        builder.appendNull();
      } else {
        builder.write(column, bufferedPosition);
      }
    }

    int nextCol = childOutputTypes.size();
    ColumnBuilder nextBuilder = tsBlockBuilder.getColumnBuilder(nextCol);
    Column nextMeasurementColumn = nextBlock.getColumn(measurementColumnIndex);
    if (nextMeasurementColumn.isNull(nextPosition)) {
      nextBuilder.appendNull();
    } else {
      nextBuilder.write(nextMeasurementColumn, nextPosition);
    }

    tsBlockBuilder.declarePosition();
  }

  private void emitBufferedRow(boolean nextIsNull) {
    for (int col = 0; col < childOutputTypes.size(); col++) {
      Column column = bufferedBlock.getColumn(col);
      ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(col);
      if (column.isNull(bufferedPosition)) {
        builder.appendNull();
      } else {
        builder.write(column, bufferedPosition);
      }
    }

    int nextCol = childOutputTypes.size();
    ColumnBuilder nextBuilder = tsBlockBuilder.getColumnBuilder(nextCol);
    if (nextIsNull) {
      nextBuilder.appendNull();
    }

    tsBlockBuilder.declarePosition();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }
    if (hasBufferedRow && childExhausted) {
      return true;
    }
    if (inputOperator.hasNext()) {
      return true;
    }
    childExhausted = true;
    return hasBufferedRow;
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemoryFromInput = inputOperator.calculateMaxPeekMemoryWithCounter();
    long maxPeekMemoryFromCurrent =
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    return Math.max(maxPeekMemoryFromInput, maxPeekMemoryFromCurrent)
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext()
        + TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }
}
