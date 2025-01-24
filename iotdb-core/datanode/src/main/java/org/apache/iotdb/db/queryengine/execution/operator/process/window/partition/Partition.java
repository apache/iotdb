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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Partition {
  private final List<TsBlock> tsBlocks;
  private int cachedPositionCount = -1;

  public Partition(List<TsBlock> tsBlocks, int startIndexInFirstBlock, int endIndexInLastBlock) {
    if (tsBlocks.size() == 1) {
      int length = endIndexInLastBlock - startIndexInFirstBlock;
      this.tsBlocks =
          Collections.singletonList(tsBlocks.get(0).getRegion(startIndexInFirstBlock, length));
      return;
    }

    this.tsBlocks = new ArrayList<>(tsBlocks.size());
    // First TsBlock
    TsBlock firstBlock = tsBlocks.get(0).subTsBlock(startIndexInFirstBlock);
    this.tsBlocks.add(firstBlock);
    // Middle TsBlock
    for (int i = 1; i < tsBlocks.size() - 1; i++) {
      this.tsBlocks.add(tsBlocks.get(i));
    }
    // Last TsBlock
    TsBlock lastBlock = tsBlocks.get(tsBlocks.size() - 1).getRegion(0, endIndexInLastBlock);
    this.tsBlocks.add(lastBlock);
  }

  public int getPositionCount() {
    if (cachedPositionCount == -1) {
      // Lazy initialized
      cachedPositionCount = 0;
      for (TsBlock block : tsBlocks) {
        cachedPositionCount += block.getPositionCount();
      }
    }

    return cachedPositionCount;
  }

  public int getValueColumnCount() {
    return tsBlocks.get(0).getValueColumnCount();
  }

  public TsBlock getTsBlock(int tsBlockIndex) {
    return tsBlocks.get(tsBlockIndex);
  }

  public List<Column[]> getAllColumns() {
    List<Column[]> allColumns = new ArrayList<>();
    for (TsBlock block : tsBlocks) {
      allColumns.add(block.getAllColumns());
    }

    return allColumns;
  }

  public boolean getBoolean(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getBoolean(offsetInTsBlock);
  }

  public int getInt(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getInt(offsetInTsBlock);
  }

  public long getLong(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getLong(offsetInTsBlock);
  }

  public float getFloat(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getFloat(offsetInTsBlock);
  }

  public double getDouble(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getDouble(offsetInTsBlock);
  }

  public Binary getBinary(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).getBinary(offsetInTsBlock);
  }

  public boolean isNull(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    return tsBlock.getColumn(channel).isNull(offsetInTsBlock);
  }

  public void writeTo(ColumnBuilder builder, int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();

    Column column = tsBlocks.get(tsBlockIndex).getColumn(channel);
    builder.write(column, offsetInTsBlock);
  }

  public static class PartitionIndex {
    private final int tsBlockIndex;
    private final int offsetInTsBlock;

    PartitionIndex(int tsBlockIndex, int offsetInTsBlock) {
      this.tsBlockIndex = tsBlockIndex;
      this.offsetInTsBlock = offsetInTsBlock;
    }

    public int getTsBlockIndex() {
      return tsBlockIndex;
    }

    public int getOffsetInTsBlock() {
      return offsetInTsBlock;
    }
  }

  // start and end are indexes within partition
  // Both of them are inclusive, i.e. [start, end]
  public Partition getRegion(int start, int end) {
    PartitionIndex startPartitionIndex = getPartitionIndex(start);
    PartitionIndex endPartitionIndex = getPartitionIndex(end);

    List<TsBlock> tsBlockList = new ArrayList<>();
    int startTsBlockIndex = startPartitionIndex.getTsBlockIndex();
    int endTsBlockIndex = endPartitionIndex.getTsBlockIndex();
    for (int i = startTsBlockIndex; i <= endTsBlockIndex; i++) {
      tsBlockList.add(tsBlocks.get(i));
    }

    int startIndexInFirstBlock = startPartitionIndex.getOffsetInTsBlock();
    int endIndexInLastBlock = endPartitionIndex.getOffsetInTsBlock();
    return new Partition(tsBlockList, startIndexInFirstBlock, endIndexInLastBlock + 1);
  }

  // rowIndex is index within partition
  public PartitionIndex getPartitionIndex(int rowIndex) {
    int tsBlockIndex = 0;
    while (tsBlockIndex < tsBlocks.size()
        && rowIndex >= tsBlocks.get(tsBlockIndex).getPositionCount()) {
      rowIndex -= tsBlocks.get(tsBlockIndex).getPositionCount();
      // Enter next TsBlock
      tsBlockIndex++;
    }

    if (tsBlockIndex != tsBlocks.size()) {
      return new PartitionIndex(tsBlockIndex, rowIndex);
    } else {
      // Unlikely
      throw new IndexOutOfBoundsException("Index out of Partition's bounds!");
    }
  }

  public List<ColumnList> getSortedColumnList(List<Integer> sortedChannels) {
    List<ColumnList> columnLists = new ArrayList<>();

    for (Integer sortedChannel : sortedChannels) {
      List<Column> columns = new ArrayList<>();
      for (TsBlock tsBlock : tsBlocks) {
        columns.add(tsBlock.getColumn(sortedChannel));
      }
      columnLists.add(new ColumnList(columns));
    }

    return columnLists;
  }
}
