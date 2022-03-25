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
package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.mpp.common.TimeColumn;
import org.apache.iotdb.db.mpp.common.TsBlock;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class TimeJoinOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private final List<Operator> children;

  private final int inputCount;

  private final TsBlock[] inputTsBlocks;

  private final int[] inputIndex;

  private final boolean[] noMoreTsBlocks;

  private final TimeSelector timeSelector;

  private final int columnCount;

  public TimeJoinOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      OrderBy mergeOrder,
      int columnCount) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.inputCount = children.size();
    this.inputTsBlocks = new TsBlock[this.inputCount];
    this.inputIndex = new int[this.inputCount];
    this.noMoreTsBlocks = new boolean[this.inputCount];
    this.timeSelector = new TimeSelector(this.inputCount << 1, OrderBy.TIMESTAMP_ASC == mergeOrder);
    this.columnCount = columnCount;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < inputCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        ListenableFuture<Void> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          return blocked;
        }
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    // end time for returned TsBlock this time, it's the min end time among all the children
    // TsBlocks
    long currentEndTime = 0;
    boolean init = false;
    for (int i = 0; i < inputCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        inputIndex[i] = 0;
        inputTsBlocks[i] = children.get(i).next();
        if (!empty(i)) {
          int rowSize = inputTsBlocks[i].getCount();
          for (int row = 0; row < rowSize; row++) {
            timeSelector.add(inputTsBlocks[i].getTimeByIndex(row));
          }
        }
      }
      // update the currentEndTime if the TsBlock is not empty
      if (!empty(i)) {
        currentEndTime =
            init
                ? Math.min(currentEndTime, inputTsBlocks[i].getEndTime())
                : inputTsBlocks[i].getEndTime();
        init = true;
      }
    }

    if (timeSelector.isEmpty()) {
      // TODO need to discuss whether to return null or return an empty TSBlock with TsBlockMetadata
      return null;
    }

    TsBlock res = new TsBlock(columnCount);
    while (!timeSelector.isEmpty() && timeSelector.first() <= currentEndTime) {
      res.addTime(timeSelector.pollFirst());
    }

    for (int i = 0, column = 0; i < inputCount; i++) {
      TsBlock block = inputTsBlocks[i];
      TimeColumn timeColumn = block.getTimeColumn();
      int valueColumnCount = block.getValueColumnCount();
      int startIndex = inputIndex[i];
      for (int j = 0; j < valueColumnCount; j++) {
        inputIndex[i] =
            res.addValues(column++, timeColumn, block.getColumn(j), startIndex, currentEndTime);
      }
    }
    return res;
  }

  @Override
  public boolean hasNext() {
    for (int i = 0; i < inputCount; i++) {
      if (!empty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (children.get(i).hasNext()) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  private boolean empty(int columnIndex) {
    return inputTsBlocks[columnIndex] == null
        || inputTsBlocks[columnIndex].getCount() == inputIndex[columnIndex];
  }
}
