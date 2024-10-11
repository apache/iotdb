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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/** Used for table previous fill with group. */
public class PreviousFillWithGroupOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PreviousFillWithGroupOperator.class);
  private final OperatorContext operatorContext;
  private final IFill[] fillArray;
  private final Operator child;
  private final int outputColumnCount;

  // start from 0, never will be -1
  private final int helperColumnIndex;

  private final Comparator<SortKey> groupKeyComparator;

  private final TsBlockBuilder resultBuilder;

  private SortKey lastRow = null;

  public PreviousFillWithGroupOperator(
      OperatorContext operatorContext,
      IFill[] fillArray,
      Operator child,
      int helperColumnIndex,
      Comparator<SortKey> groupKeyComparator,
      List<TSDataType> dataTypes) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    checkArgument(
        fillArray != null && fillArray.length > 0, "fillArray should not be null or empty");
    this.fillArray = fillArray;
    this.child = requireNonNull(child, "child operator is null");
    this.outputColumnCount = fillArray.length;
    checkArgument(
        helperColumnIndex >= 0,
        "helperColumnIndex for PreviousFillWithGroupOperator should never be negative");
    this.helperColumnIndex = helperColumnIndex;
    this.groupKeyComparator = groupKeyComparator;
    this.resultBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock block = child.nextWithTimer();
    if (block == null) {
      return null;
    }

    checkArgument(
        outputColumnCount == block.getValueColumnCount(),
        "outputColumnCount is not equal to value column count of child operator's TsBlock");

    // current TsBlock's first row is not same group as the last row of previous TsBlock
    // we need to call reset the fill
    if (lastRow != null && groupKeyComparator.compare(lastRow, new SortKey(block, 0)) != 0) {
      resetFill();
    }

    // update lastRow
    this.lastRow = new SortKey(block, block.getPositionCount() - 1);
    // all rows in current TsBlock belong to same group
    if (lastRow.rowIndex == 0 || groupKeyComparator.compare(new SortKey(block, 0), lastRow) == 0) {
      Column[] valueColumns = new Column[outputColumnCount];

      for (int i = 0; i < outputColumnCount; i++) {
        valueColumns[i] = fillArray[i].fill(block.getColumn(helperColumnIndex), block.getColumn(i));
      }
      return TsBlock.wrapBlocksWithoutCopy(
          block.getPositionCount(), block.getTimeColumn(), valueColumns);
    } else {
      // otherwise, we need to split the current TsBlock and reset the fill between different groups
      resultBuilder.reset();
      SortKey currentGroupKey = new SortKey(block, 0);
      for (int i = 1; i <= lastRow.rowIndex; i++) {
        SortKey nextGroupKey = new SortKey(block, i);

        if (groupKeyComparator.compare(currentGroupKey, nextGroupKey) != 0) {
          // don't belong to same group, need to split here and build result for current group, then
          // reset the fill
          buildResultForCurrentGroup(block, currentGroupKey.rowIndex, i - 1);
          resetFill();
          currentGroupKey = nextGroupKey;
        }
      }

      // take care of the last group
      // we don't need to reset the fill, because we can only decide whether to reset once we get
      // next TsBlock
      buildResultForCurrentGroup(block, currentGroupKey.rowIndex, lastRow.rowIndex);

      return resultBuilder.build(block.getTimeColumn());
    }
  }

  private void resetFill() {
    for (IFill fill : fillArray) {
      fill.reset();
    }
  }

  // startIndex and endIndex are all including.
  // we won't build timeColumn in this method
  private void buildResultForCurrentGroup(TsBlock block, int startIndex, int endIndex) {

    int length = endIndex - startIndex + 1;
    resultBuilder.declarePositions(length);

    Column helperColumn = block.getColumn(helperColumnIndex).getRegion(startIndex, length);
    for (int i = 0; i < outputColumnCount; i++) {
      Column column =
          fillArray[i].fill(helperColumn, block.getColumn(i).getRegion(startIndex, length));
      ColumnBuilder builder = resultBuilder.getColumnBuilder(i);
      for (int rowIndex = 0; rowIndex < length; rowIndex++) {
        if (column.isNull(rowIndex)) {
          builder.appendNull();
        } else {
          builder.write(column, rowIndex);
        }
      }
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // while doing previous fill, we may need to copy the corresponding column if there
    // exists null values so the max peek memory may be double
    return 2 * child.calculateMaxPeekMemory() + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    // we may need to cache the last tsblock in groupKeyComparator
    return child.calculateRetainedSizeAfterCallingNext() + child.calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public void close() throws Exception {
    child.close();
    lastRow = null;
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished();
  }

  @Override
  public boolean hasNext() throws Exception {
    return child.hasNextWithTimer();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext);
  }
}
