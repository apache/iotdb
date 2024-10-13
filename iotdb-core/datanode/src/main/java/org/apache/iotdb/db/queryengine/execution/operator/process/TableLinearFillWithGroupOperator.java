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

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableLinearFillWithGroupOperator extends TableLinearFillOperator {

  private final List<Boolean> groupSplitter;

  private final List<Boolean> noMoreTsBlockForCurrentGroup;

  private final Comparator<SortKey> groupKeyComparator;

  private final TsBlockBuilder resultBuilder;

  private SortKey lastRow = null;

  public TableLinearFillWithGroupOperator(
      OperatorContext operatorContext,
      ILinearFill[] fillArray,
      Operator child,
      int helperColumnIndex,
      Comparator<SortKey> groupKeyComparator,
      List<TSDataType> dataTypes) {
    super(operatorContext, fillArray, child, helperColumnIndex);
    this.groupSplitter = new ArrayList<>();
    this.noMoreTsBlockForCurrentGroup = new ArrayList<>();
    this.groupKeyComparator = groupKeyComparator;
    this.resultBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  // we won't build timeColumn in this method
  TsBlock append(int length, Column timeColumn, Column[] valueColumns) {
    for (int i = 0; i < outputColumnCount; i++) {
      Column column = valueColumns[i];
      ColumnBuilder builder = resultBuilder.getColumnBuilder(i);
      for (int rowIndex = 0; rowIndex < length; rowIndex++) {
        if (column.isNull(rowIndex)) {
          builder.appendNull();
        } else {
          builder.write(column, rowIndex);
        }
      }
    }
    resultBuilder.declarePositions(length);
    return null;
  }

  @Override
  TsBlock buildFinalResult(TsBlock tempResult) {
    TsBlock result = null;
    if (!resultBuilder.isEmpty()) {
      Column timeColumn =
          new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount());
      result = resultBuilder.build(timeColumn);
      resultBuilder.reset();
    }
    return result;
  }

  @Override
  boolean noMoreTsBlockForCurrentGroup() {
    return noMoreTsBlock || noMoreTsBlockForCurrentGroup.get(0);
  }

  @Override
  void resetFill() {
    // if current tsblock belongs to another group, we need to reset fill.
    if (Boolean.TRUE.equals(groupSplitter.remove(0))) {
      for (ILinearFill fill : fillArray) {
        fill.reset();
      }
    }
    noMoreTsBlockForCurrentGroup.remove(0);
  }

  @Override
  public void close() throws Exception {
    super.close();
    lastRow = null;
  }

  @Override
  void updateCachedData(TsBlock tsBlock) {

    boolean isFirstGroupOfCurrentTsBlock = true;
    SortKey currentGroupKey = new SortKey(tsBlock, 0);
    int size = tsBlock.getPositionCount();
    for (int i = 1; i < size; i++) {
      SortKey nextGroupKey = new SortKey(tsBlock, i);

      if (groupKeyComparator.compare(currentGroupKey, nextGroupKey) != 0) {
        int length = i - currentGroupKey.rowIndex;
        TsBlock currentGroup = tsBlock.getRegion(currentGroupKey.rowIndex, length);
        super.updateCachedData(currentGroup);
        if (isFirstGroupOfCurrentTsBlock) {
          isFirstGroupOfCurrentTsBlock = false;
          boolean isNewGroup = isNewGroup(currentGroupKey);
          // if it's new group, update noMoreTsBlockFor previous group in previous TsBlock if there
          // exists
          if (isNewGroup && !noMoreTsBlockForCurrentGroup.isEmpty()) {
            noMoreTsBlockForCurrentGroup.set(noMoreTsBlockForCurrentGroup.size() - 1, true);
          }
          groupSplitter.add(isNewGroup);
        } else {
          groupSplitter.add(true);
        }
        noMoreTsBlockForCurrentGroup.add(true);
        currentGroupKey = nextGroupKey;
      }
    }

    int length = size - currentGroupKey.rowIndex;
    TsBlock currentGroup = tsBlock.getRegion(currentGroupKey.rowIndex, length);
    super.updateCachedData(currentGroup);
    if (isFirstGroupOfCurrentTsBlock) {
      boolean isNewGroup = isNewGroup(currentGroupKey);
      // if it's new group, update noMoreTsBlockFor previous group in previous TsBlock if there
      // exists
      if (isNewGroup && !noMoreTsBlockForCurrentGroup.isEmpty()) {
        noMoreTsBlockForCurrentGroup.set(noMoreTsBlockForCurrentGroup.size() - 1, true);
      }
      groupSplitter.add(isNewGroup);
    } else {
      groupSplitter.add(true);
    }
    noMoreTsBlockForCurrentGroup.add(false);
    lastRow = currentGroupKey;
  }

  // judge whether the first group of current TsBlock belongs to the same group of last row of
  // previous TsBlock
  private boolean isNewGroup(SortKey currentGroupKey) {
    return lastRow == null || groupKeyComparator.compare(lastRow, currentGroupKey) != 0;
  }
}
