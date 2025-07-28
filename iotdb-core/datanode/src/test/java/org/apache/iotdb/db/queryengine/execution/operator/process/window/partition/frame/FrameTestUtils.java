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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FrameTestUtils {
  private final Partition partition;
  private final List<ColumnList> sortedColumns;

  private final int partitionStart;
  private final int partitionEnd;

  private int currentGroupIndex = -1;
  private int peerGroupStart;
  private int peerGroupEnd;

  private final RowComparator peerGroupComparator;
  private final Frame frame;

  private final List<Integer> frameStarts;
  private final List<Integer> frameEnds;

  public FrameTestUtils(TsBlock tsBlock, TSDataType inputDataType, FrameInfo frameInfo) {
    this.partition = tsBlockToPartition(tsBlock);
    this.sortedColumns = this.partition.getSortedColumnList(Collections.singletonList(0));
    this.partitionStart = 0;
    this.partitionEnd = tsBlock.getPositionCount();

    this.peerGroupComparator = new RowComparator(Collections.singletonList(inputDataType));

    updatePeerGroup(0);
    this.frame = createFrame(frameInfo);

    this.frameStarts = new ArrayList<>();
    this.frameEnds = new ArrayList<>();
  }

  public void processAllRows() {
    for (int i = partitionStart; i < partitionEnd; i++) {
      if (i == peerGroupEnd) {
        updatePeerGroup(i);
      }

      Range range = frame.getRange(i, currentGroupIndex, peerGroupStart, peerGroupEnd);
      this.frameStarts.add(range.getStart());
      this.frameEnds.add(range.getEnd());
    }
  }

  public List<Integer> getFrameStarts() {
    return frameStarts;
  }

  public List<Integer> getFrameEnds() {
    return frameEnds;
  }

  private void updatePeerGroup(int index) {
    currentGroupIndex++;
    peerGroupStart = index;
    // Find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while (peerGroupEnd < partitionEnd
        && peerGroupComparator.equalColumnLists(sortedColumns, peerGroupStart, peerGroupEnd)) {
      peerGroupEnd++;
    }
  }

  private Partition tsBlockToPartition(TsBlock tsBlock) {
    return new Partition(Collections.singletonList(tsBlock), 0, tsBlock.getPositionCount());
  }

  private List<ColumnList> tsBlockToColumnLists(TsBlock tsBlock) {
    Column[] allColumns = tsBlock.getValueColumns();

    List<ColumnList> columnLists = new ArrayList<>();
    for (Column column : allColumns) {
      ColumnList columnList = new ColumnList(Collections.singletonList(column));
      columnLists.add(columnList);
    }

    return columnLists;
  }

  private Frame createFrame(FrameInfo frameInfo) {
    Frame frame;
    switch (frameInfo.getFrameType()) {
      case RANGE:
        frame = new RangeFrame(partition, frameInfo, sortedColumns, peerGroupComparator);
        break;
      case ROWS:
        frame = new RowsFrame(partition, frameInfo);
        break;
      case GROUPS:
        frame =
            new GroupsFrame(
                partition,
                frameInfo,
                sortedColumns,
                peerGroupComparator,
                peerGroupEnd - partitionStart - 1);
        break;
      default:
        // Unreachable
        throw new UnsupportedOperationException("Unreachable!");
    }

    return frame;
  }
}
