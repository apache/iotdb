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

package org.apache.iotdb.calc.execution.operator.process.window.partition;

import org.apache.iotdb.calc.execution.operator.process.function.partition.Slice;
import org.apache.iotdb.calc.execution.operator.process.window.utils.ColumnList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public class Partition {
  private final List<Column[]> segments;
  private int cachedPositionCount = -1;

  public Partition(List<TsBlock> tsBlocks, int startIndexInFirstBlock, int endIndexInLastBlock) {
    this.segments = new ArrayList<>(tsBlocks.size());
    if (tsBlocks.size() == 1) {
      int length = endIndexInLastBlock - startIndexInFirstBlock;
      TsBlock region = tsBlocks.get(0).getRegion(startIndexInFirstBlock, length);
      segments.add(region.getValueColumns());
    } else {
      TsBlock firstBlock = tsBlocks.get(0).subTsBlock(startIndexInFirstBlock);
      segments.add(firstBlock.getValueColumns());
      for (int i = 1; i < tsBlocks.size() - 1; i++) {
        segments.add(tsBlocks.get(i).getValueColumns());
      }
      TsBlock lastBlock = tsBlocks.get(tsBlocks.size() - 1).getRegion(0, endIndexInLastBlock);
      segments.add(lastBlock.getValueColumns());
    }
  }

  public Partition(List<Slice> slices) {
    this.segments = new ArrayList<>(slices.size());
    for (Slice slice : slices) {
      segments.add(slice.getRequiredColumns());
    }
  }

  private Partition(List<Column[]> segments, boolean directSegments) {
    this.segments = segments;
  }

  public int getPositionCount() {
    if (cachedPositionCount == -1) {
      cachedPositionCount = 0;
      for (Column[] segment : segments) {
        cachedPositionCount += segment[0].getPositionCount();
      }
    }

    return cachedPositionCount;
  }

  public int getValueColumnCount() {
    return segments.get(0).length;
  }

  public List<Column[]> getAllColumns() {
    return segments;
  }

  public boolean getBoolean(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getBoolean(offset);
  }

  public int getInt(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getInt(offset);
  }

  public long getLong(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getLong(offset);
  }

  public float getFloat(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getFloat(offset);
  }

  public double getDouble(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getDouble(offset);
  }

  public Binary getBinary(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].getBinary(offset);
  }

  public boolean isNull(int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    return segments.get(segmentIndex)[channel].isNull(offset);
  }

  public void writeTo(ColumnBuilder builder, int channel, int rowIndex) {
    PartitionIndex partitionIndex = getPartitionIndex(rowIndex);
    int segmentIndex = partitionIndex.getSegmentIndex();
    int offset = partitionIndex.getOffsetInSegment();
    Column column = segments.get(segmentIndex)[channel];
    builder.write(column, offset);
  }

  public static class PartitionIndex {
    private final int segmentIndex;
    private final int offsetInSegment;

    PartitionIndex(int segmentIndex, int offsetInSegment) {
      this.segmentIndex = segmentIndex;
      this.offsetInSegment = offsetInSegment;
    }

    public int getSegmentIndex() {
      return segmentIndex;
    }

    public int getOffsetInSegment() {
      return offsetInSegment;
    }
  }

  // start and end are indexes within partition
  // Both of them are inclusive, i.e. [start, end]
  public Partition getRegion(int start, int end) {
    PartitionIndex startPI = getPartitionIndex(start);
    PartitionIndex endPI = getPartitionIndex(end);

    int startSeg = startPI.getSegmentIndex();
    int endSeg = endPI.getSegmentIndex();
    int columnCount = segments.get(0).length;

    List<Column[]> regionSegments = new ArrayList<>();

    if (startSeg == endSeg) {
      int offset = startPI.getOffsetInSegment();
      int length = endPI.getOffsetInSegment() - offset + 1;
      Column[] cols = segments.get(startSeg);
      Column[] region = new Column[columnCount];
      for (int c = 0; c < columnCount; c++) {
        region[c] = cols[c].getRegion(offset, length);
      }
      regionSegments.add(region);
    } else {
      // First segment
      Column[] firstCols = segments.get(startSeg);
      int firstOffset = startPI.getOffsetInSegment();
      int firstLen = firstCols[0].getPositionCount() - firstOffset;
      Column[] firstRegion = new Column[columnCount];
      for (int c = 0; c < columnCount; c++) {
        firstRegion[c] = firstCols[c].getRegion(firstOffset, firstLen);
      }
      regionSegments.add(firstRegion);

      // Middle segments
      for (int i = startSeg + 1; i < endSeg; i++) {
        regionSegments.add(segments.get(i));
      }

      // Last segment
      Column[] lastCols = segments.get(endSeg);
      int lastLen = endPI.getOffsetInSegment() + 1;
      Column[] lastRegion = new Column[columnCount];
      for (int c = 0; c < columnCount; c++) {
        lastRegion[c] = lastCols[c].getRegion(0, lastLen);
      }
      regionSegments.add(lastRegion);
    }

    return new Partition(regionSegments, true);
  }

  public PartitionIndex getPartitionIndex(int rowIndex) {
    int segmentIndex = 0;
    while (segmentIndex < segments.size()
        && rowIndex >= segments.get(segmentIndex)[0].getPositionCount()) {
      rowIndex -= segments.get(segmentIndex)[0].getPositionCount();
      segmentIndex++;
    }

    if (segmentIndex != segments.size()) {
      return new PartitionIndex(segmentIndex, rowIndex);
    } else {
      throw new IndexOutOfBoundsException("Index out of Partition's bounds!");
    }
  }

  public List<ColumnList> getSortedColumnList(List<Integer> sortedChannels) {
    List<ColumnList> columnLists = new ArrayList<>();

    for (Integer sortedChannel : sortedChannels) {
      List<Column> columns = new ArrayList<>();
      for (Column[] segment : segments) {
        columns.add(segment[sortedChannel]);
      }
      columnLists.add(new ColumnList(columns));
    }

    return columnLists;
  }
}
