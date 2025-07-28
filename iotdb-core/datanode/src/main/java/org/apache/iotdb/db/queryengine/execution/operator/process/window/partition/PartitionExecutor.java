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

import org.apache.iotdb.db.queryengine.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.Frame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.GroupsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.RangeFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.RowsFrame;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.List;

public final class PartitionExecutor {
  private final int partitionStart;
  private final int partitionEnd;
  private final Partition partition;

  private final List<WindowFunction> windowFunctions;

  private final List<ColumnList> sortedColumns;
  private final RowComparator peerGroupComparator;
  private int peerGroupStart;
  private int peerGroupEnd;

  private final List<Integer> outputChannels;

  private int currentGroupIndex = -1;
  private int currentPosition;

  private final List<Frame> frames;

  private final boolean needPeerGroup;

  public PartitionExecutor(
      List<TsBlock> tsBlocks,
      List<TSDataType> dataTypes,
      int startIndexInFirstBlock,
      int endIndexInLastBlock,
      List<Integer> outputChannels,
      List<WindowFunction> windowFunctions,
      List<FrameInfo> frameInfoList,
      List<Integer> sortChannels) {
    // Partition
    this.partition = new Partition(tsBlocks, startIndexInFirstBlock, endIndexInLastBlock);
    this.partitionStart = startIndexInFirstBlock;
    this.partitionEnd = startIndexInFirstBlock + this.partition.getPositionCount();
    // Window functions and frames
    this.windowFunctions = ImmutableList.copyOf(windowFunctions);
    this.frames = new ArrayList<>();

    this.outputChannels = ImmutableList.copyOf(outputChannels);

    // Prepare for peer group comparing
    List<TSDataType> sortDataTypes = new ArrayList<>();
    for (int channel : sortChannels) {
      TSDataType dataType = dataTypes.get(channel);
      sortDataTypes.add(dataType);
    }
    peerGroupComparator = new RowComparator(sortDataTypes);
    sortedColumns = partition.getSortedColumnList(sortChannels);

    currentPosition = partitionStart;
    needPeerGroup =
        windowFunctions.stream().anyMatch(WindowFunction::needPeerGroup)
            || frameInfoList.stream()
                .anyMatch(frameInfo -> frameInfo.getFrameType() != FrameInfo.FrameType.ROWS);
    if (needPeerGroup) {
      updatePeerGroup();
    }

    for (int i = 0; i < frameInfoList.size(); i++) {
      Frame frame = null;
      if (windowFunctions.get(i).needFrame()) {
        FrameInfo frameInfo = frameInfoList.get(i);
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
      }
      frames.add(frame);
    }
  }

  public boolean hasNext() {
    return currentPosition < partitionEnd;
  }

  public void processNextRow(TsBlockBuilder builder) {
    // Copy origin data
    int index = currentPosition - partitionStart;
    Partition.PartitionIndex partitionIndex = partition.getPartitionIndex(index);
    int tsBlockIndex = partitionIndex.getTsBlockIndex();
    int offsetInTsBlock = partitionIndex.getOffsetInTsBlock();
    TsBlock tsBlock = partition.getTsBlock(tsBlockIndex);

    int channel = 0;
    for (int i = 0; i < outputChannels.size(); i++) {
      Column column = tsBlock.getColumn(outputChannels.get(i));
      ColumnBuilder columnBuilder = builder.getColumnBuilder(i);
      if (column.isNull(offsetInTsBlock)) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.write(column, offsetInTsBlock);
      }
      channel++;
    }

    if (needPeerGroup && currentPosition == peerGroupEnd) {
      updatePeerGroup();
    }

    for (int i = 0; i < windowFunctions.size(); i++) {
      Frame frame = frames.get(i);
      WindowFunction windowFunction = windowFunctions.get(i);

      Range frameRange =
          windowFunction.needFrame()
              ? frame.getRange(
                  index,
                  currentGroupIndex,
                  peerGroupStart - partitionStart,
                  peerGroupEnd - partitionStart)
              : new Range(-1, -1);
      windowFunction.transform(
          partition,
          builder.getColumnBuilder(channel),
          currentPosition - partitionStart,
          frameRange.getStart(),
          frameRange.getEnd(),
          peerGroupStart - partitionStart,
          peerGroupEnd - partitionStart - 1);

      channel++;
    }

    currentPosition++;
    builder.declarePosition();
  }

  private void updatePeerGroup() {
    currentGroupIndex++;
    peerGroupStart = currentPosition;
    // Find end of peer group
    peerGroupEnd = peerGroupStart + 1;
    while (peerGroupEnd < partitionEnd
        && peerGroupComparator.equalColumnLists(
            sortedColumns, peerGroupStart - partitionStart, peerGroupEnd - partitionStart)) {
      peerGroupEnd++;
    }
  }

  public void resetWindowFunctions() {
    for (WindowFunction windowFunction : windowFunctions) {
      windowFunction.reset();
    }
  }
}
