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
package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.LinkedList;
import java.util.List;

public class TimeMergeToolKit implements MergeSortToolKit {

  Ordering deviceOrdering;
  Ordering timeOrdering;
  long[] startKey;
  long[] endKey;
  TsBlock[] tsBlocks;
  boolean[] tsBlocksExist;
  long targetKey;
  int tsBlockCount;

  public TimeMergeToolKit(List<SortItem> sortItemList, int childNum) {
    this.deviceOrdering = sortItemList.get(1).getOrdering();
    this.timeOrdering = sortItemList.get(0).getOrdering();
    this.tsBlockCount = childNum;
    this.startKey = new long[tsBlockCount];
    this.endKey = new long[tsBlockCount];
    this.tsBlocksExist = new boolean[tsBlockCount];
    this.tsBlocks = new TsBlock[tsBlockCount];
  }

  @Override
  public void addTsBlock(TsBlock tsBlock, int index) {
    this.tsBlocks[index] = tsBlock;
    startKey[index] = tsBlock.getStartTime();
    endKey[index] = tsBlock.getEndTime();
    tsBlocksExist[index] = true;
  }

  @Override
  public void updateTsBlock(int index, TsBlock tsBlock) {
    if (tsBlock == null) {
      tsBlocks[index] = null;
      tsBlocksExist[index] = false;
    } else {
      tsBlocks[index] = tsBlock;
      startKey[index] = tsBlocks[index].getTimeByIndex(0);
    }
  }

  @Override
  public List<Integer> getTargetTsBlockIndex() {
    List<Integer> targetTsBlockIndex = new LinkedList<>();
    if (tsBlockCount == 1) {
      targetTsBlockIndex.add(0);
      return targetTsBlockIndex;
    }
    // find the targetValue in TsBlocks
    // it is:
    // (1) the smallest endKey when ordering is asc
    // (2) the biggest endKey when ordering is desc
    // which is controlled by greater method
    long minEndKey = Long.MIN_VALUE;
    int index = Integer.MAX_VALUE;
    for (int i = 0; i < tsBlockCount; i++) {
      if (tsBlocksExist[i]) {
        minEndKey = endKey[i];
        index = i;
        break;
      }
    }
    for (int i = index + 1; i < tsBlockCount; i++) {
      if (tsBlocksExist[i] && greater(minEndKey, endKey[i])) {
        minEndKey = endKey[i];
      }
    }
    this.targetKey = minEndKey;
    for (int i = 0; i < tsBlockCount; i++) {
      if (tsBlocksExist[i] && (greater(minEndKey, startKey[i]) || minEndKey == startKey[i])) {
        targetTsBlockIndex.add(i);
      }
    }
    return targetTsBlockIndex;
  }

  /** Comparator */
  @Override
  public boolean greater(
      TsBlock.TsBlockSingleColumnIterator t, TsBlock.TsBlockSingleColumnIterator s) {
    if (t.currentTime() == s.currentTime()) {
      return deviceOrdering == Ordering.ASC
          ? (t.currentValue().toString()).compareTo(s.currentValue().toString()) > 0
          : (t.currentValue().toString()).compareTo(s.currentValue().toString()) < 0;
    } else {
      return timeOrdering == Ordering.ASC
          ? t.currentTime() > s.currentTime()
          : t.currentTime() < s.currentTime();
    }
  }

  public boolean greater(long t, long s) {
    return timeOrdering == Ordering.ASC ? t > s : t < s;
  }

  @Override
  public boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator) {
    return timeOrdering == Ordering.ASC
        ? tsBlockIterator.currentTime() <= targetKey
        : tsBlockIterator.currentTime() >= targetKey;
  }
}
