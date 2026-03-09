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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

public class GroupedTopNRowNumberBuilder implements GroupedTopNBuilder {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedTopNRowNumberBuilder.class);

  private final List<TSDataType> sourceTypes;
  private final boolean produceRowNumber;
  private final int[] groupByChannels;
  private final GroupByHash groupByHash;
  private final RowReferenceTsBlockManager tsBlockManager = new RowReferenceTsBlockManager();
  private final GroupedTopNRowNumberAccumulator groupedTopNRowNumberAccumulator;
  private final TsBlockWithPositionComparator comparator;

  public GroupedTopNRowNumberBuilder(
      List<TSDataType> sourceTypes,
      TsBlockWithPositionComparator comparator,
      int topN,
      boolean produceRowNumber,
      int[] groupByChannels,
      GroupByHash groupByHash) {
    this.sourceTypes = sourceTypes;
    this.produceRowNumber = produceRowNumber;
    this.groupByChannels = groupByChannels;
    this.groupByHash = groupByHash;
    this.comparator = comparator;

    this.groupedTopNRowNumberAccumulator =
        new GroupedTopNRowNumberAccumulator(
            (leftRowId, rightRowId) -> {
              TsBlock leftTsBlock = tsBlockManager.getTsBlock(leftRowId);
              int leftPosition = tsBlockManager.getPosition(leftRowId);
              TsBlock rightTsBlock = tsBlockManager.getTsBlock(rightRowId);
              int rightPosition = tsBlockManager.getPosition(rightRowId);
              return comparator.compareTo(leftTsBlock, leftPosition, rightTsBlock, rightPosition);
            },
            topN,
            tsBlockManager::dereference);
  }

  @Override
  public void addTsBlock(TsBlock tsBlock) {
    int[] groupIds = groupByHash.getGroupIds(tsBlock.getColumns(groupByChannels));
    int groupCount = groupByHash.getGroupCount();

    processTsBlock(tsBlock, groupCount, groupIds);
  }

  @Override
  public Iterator<TsBlock> getResult() {
    return new ResultIterator();
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return INSTANCE_SIZE
        + groupByHash.getEstimatedSize()
        + tsBlockManager.sizeOf()
        + groupedTopNRowNumberAccumulator.sizeOf();
  }

  private void processTsBlock(TsBlock newTsBlock, int groupCount, int[] groupIds) {
    int firstPositionToAdd =
        groupedTopNRowNumberAccumulator.findFirstPositionToAdd(
            newTsBlock, groupCount, groupIds, comparator, tsBlockManager);
    if (firstPositionToAdd < 0) {
      return;
    }

    try (RowReferenceTsBlockManager.LoadCursor loadCursor =
        tsBlockManager.add(newTsBlock, firstPositionToAdd)) {
      for (int position = firstPositionToAdd;
          position < newTsBlock.getPositionCount();
          position++) {
        int groupId = groupIds[position];
        loadCursor.advance();
        groupedTopNRowNumberAccumulator.add(groupId, loadCursor);
      }
    }

    tsBlockManager.compactIfNeeded();
  }

  private class ResultIterator extends AbstractIterator<TsBlock> {
    private final TsBlockBuilder tsBlockBuilder;
    private final int groupIdCount = groupByHash.getGroupCount();
    private int currentGroupId = -1;
    private final LongBigArray rowIdOutput = new LongBigArray();
    private long currentGroupSize;
    private int currentIndexInGroup;

    ResultIterator() {
      ImmutableList.Builder<TSDataType> sourceTypesBuilders =
          ImmutableList.<TSDataType>builder().addAll(sourceTypes);
      if (produceRowNumber) {
        sourceTypesBuilders.add(TSDataType.INT64);
      }
      tsBlockBuilder = new TsBlockBuilder(sourceTypesBuilders.build());
    }

    @Override
    protected TsBlock computeNext() {
      tsBlockBuilder.reset();
      while (!tsBlockBuilder.isFull()) {
        while (currentIndexInGroup >= currentGroupSize) {
          if (currentGroupId + 1 >= groupIdCount) {
            if (tsBlockBuilder.isEmpty()) {
              return endOfData();
            }
            return tsBlockBuilder.build(
                new RunLengthEncodedColumn(
                    TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
          }
          currentGroupId++;
          currentGroupSize = groupedTopNRowNumberAccumulator.drainTo(currentGroupId, rowIdOutput);
          currentIndexInGroup = 0;
        }

        long rowId = rowIdOutput.get(currentIndexInGroup);
        TsBlock tsBlock = tsBlockManager.getTsBlock(rowId);
        int position = tsBlockManager.getPosition(rowId);
        for (int i = 0; i < sourceTypes.size(); i++) {
          ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(i);
          Column column = tsBlock.getColumn(i);
          builder.write(column, position);
        }
        if (produceRowNumber) {
          ColumnBuilder builder = tsBlockBuilder.getColumnBuilder(sourceTypes.size());
          builder.writeLong(currentIndexInGroup + 1);
        }
        tsBlockBuilder.declarePosition();
        currentIndexInGroup++;

        // Deference the row for hygiene, but no need to compact them at this point
        tsBlockManager.dereference(rowId);
      }

      if (tsBlockBuilder.isEmpty()) {
        return endOfData();
      }
      return tsBlockBuilder.build(
          new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, tsBlockBuilder.getPositionCount()));
    }
  }
}
