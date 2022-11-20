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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class WindowSliceQueue {

  // cached window slice
  private final Deque<TsBlock> deque = new LinkedList<>();

  private TimeRange curTimeRange;

  private final TsBlockBuilder windowBuilder;

  public WindowSliceQueue(List<TSDataType> dataTypeList) {
    this.windowBuilder = new TsBlockBuilder(dataTypeList);
  }

  public void processTsBlock(TsBlock tsBlock) {
    deque.addLast(tsBlock);
  }

  public void updateTimeRange(TimeRange curTimeRange) {
    this.curTimeRange = curTimeRange;
    evictingExpiredSlice();
  }

  public void evictingExpiredSlice() {
    while (!deque.isEmpty() && !curTimeRange.contains(deque.getFirst().getStartTime())) {
      deque.removeFirst();
    }
  }

  public TsBlock outputWindow() {
    windowBuilder.reset();

    TimeColumnBuilder timeColumnBuilder = windowBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = windowBuilder.getValueColumnBuilders();
    int valueColumnCount = columnBuilders.length;

    for (TsBlock windowSlice : deque) {
      int positionCount = windowSlice.getPositionCount();
      for (int index = 0; index < positionCount; index++) {
        timeColumnBuilder.write(windowSlice.getTimeColumn(), index);
        for (int columnIndex = 0; columnIndex < valueColumnCount; columnIndex++) {
          columnBuilders[columnIndex].write(windowSlice.getColumn(columnIndex), index);
        }
      }
      windowBuilder.declarePositions(positionCount);
    }
    return windowBuilder.build();
  }

  public boolean isEmpty() {
    return deque.isEmpty();
  }
}
