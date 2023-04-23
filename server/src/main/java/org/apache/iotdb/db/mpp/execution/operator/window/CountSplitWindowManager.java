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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.util.List;

public class CountSplitWindowManager implements RawDataWindowManager {

  private boolean initial = false;
  private long leftCount = 0;
  private final CountIterator countIterator;
  private final TsBlockBuilder reservedTsBlocks;
  private boolean needSkip;

  public CountSplitWindowManager(long interval, long step, List<TSDataType> dataTypes) {
    this.countIterator = new CountIterator(interval, step);
    this.reservedTsBlocks = new TsBlockBuilder(dataTypes);
    this.needSkip = false;
  }

  @Override
  public boolean isCurWindowInit() {
    return initial;
  }

  @Override
  public boolean isCurWindowFinished() {
    return leftCount == 0;
  }

  @Override
  public void nextWindow() {
    initial = false;
    needSkip = true;
  }

  @Override
  public boolean hasNext() {
    return !reservedTsBlocks.isEmpty();
  }

  @Override
  public TsBlock initWindow(TsBlock tsBlock) {
    if (needSkip) {
      // when step > interval, we need to skip some data
      int skipCount = countIterator.getSkipCount();
      if (tsBlock.getPositionCount() <= skipCount) {
        return null;
      }
      tsBlock = tsBlock.subTsBlock(skipCount);
    }
    initial = true;
    leftCount = countIterator.nextCount();
    return tsBlock;
  }

  @Override
  public int process(TsBlock tsBlock) {
    int tsBlockSize = tsBlock.getPositionCount();
    int sizeToProcess;
    if (tsBlockSize <= leftCount) {
      leftCount = leftCount - tsBlockSize;
      sizeToProcess = tsBlockSize;
    } else {
      sizeToProcess = (int) leftCount;
      leftCount = 0;
    }

    for (int i = 0; i < sizeToProcess; i++) {
      reservedTsBlocks.getTimeColumnBuilder().write(tsBlock.getTimeColumn(), i);
      for (int j = 0; j < tsBlock.getValueColumnCount(); j++) {
        Column targetColumn = tsBlock.getColumn(j);
        if (targetColumn.isNull(i)) reservedTsBlocks.getColumnBuilder(j).appendNull();
        else reservedTsBlocks.getColumnBuilder(j).write(targetColumn, i);
      }
      reservedTsBlocks.declarePosition();
    }

    return sizeToProcess;
  }

  @Override
  public TsBlock buildTsBlock() {
    if (!reservedTsBlocks.isEmpty()) {
      TsBlock tsBlock = reservedTsBlocks.build();
      reservedTsBlocks.reset();
      return tsBlock;
    }
    return null;
  }

  public static class CountIterator {
    private final long interval;
    private final long step;
    private long curInterval;
    private long curStep;
    private boolean needCycle = false;
    private boolean cycle;

    public CountIterator(long interval, long step) {
      this.interval = interval;
      this.step = step;
      initialIntervalAndStep();
    }

    public long nextCount() {
      long temp = curInterval;
      updateIntervalAndStep();
      return temp;
    }

    public int getSkipCount() {
      if (step <= interval) return 0;
      return (int) (curStep - curInterval);
    }

    private void initialIntervalAndStep() {
      if (interval < step) {
        curInterval = interval;
        curStep = step;
      } else if (interval % step == 0) {
        curInterval = step;
        curStep = step;
      } else {
        needCycle = true;
        cycle = true;
        curInterval = interval % step;
        curStep = curInterval;
      }
    }

    private void updateIntervalAndStep() {
      if (!needCycle) return;

      curStep = curInterval;
      if (cycle) {
        curInterval = step - interval % step;
      } else {
        curInterval = interval % step;
      }
      cycle = !cycle;
    }
  }
}
