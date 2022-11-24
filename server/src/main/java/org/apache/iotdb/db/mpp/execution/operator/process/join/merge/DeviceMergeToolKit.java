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
import java.util.Objects;

public class DeviceMergeToolKit implements MergeSortToolKit {

  Ordering timeOrdering;
  Ordering deviceOrdering;
  String[] startKey;
  String[] endKey;
  TsBlock[] tsBlocks;
  boolean[] tsBlocksEmpty;
  String targetKey;
  int tsBlockCount;

  public DeviceMergeToolKit(List<SortItem> sortItemList, int childNum) {
    this.deviceOrdering = sortItemList.get(0).getOrdering();
    this.timeOrdering = sortItemList.get(1).getOrdering();
    this.tsBlockCount = childNum;
    this.startKey = new String[tsBlockCount];
    this.endKey = new String[tsBlockCount];
    this.tsBlocksEmpty = new boolean[tsBlockCount];
    this.tsBlocks = new TsBlock[tsBlockCount];
  }

  @Override
  public void addTsBlock(TsBlock tsBlock, int index) {
    this.tsBlocks[index] = tsBlock;
    startKey[index] = tsBlock.getColumn(0).getBinary(0).toString();
    endKey[index] = startKey[index];
    tsBlocksEmpty[index] = false;
  }

  @Override
  public void updateTsBlock(int index, TsBlock tsBlock) {
    if (tsBlock == null) {
      tsBlocks[index] = null;
      tsBlocksEmpty[index] = true;
    } else {
      tsBlocks[index] = tsBlock;
      startKey[index] = tsBlocks[index].getColumn(0).getBinary(0).toString();
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
    String minEndKey = "";
    int index = Integer.MAX_VALUE;
    for (int i = 0; i < tsBlockCount; i++) {
      if (!tsBlocksEmpty[i]) {
        minEndKey = endKey[i];
        index = i;
        break;
      }
    }
    for (int i = index + 1; i < tsBlockCount; i++) {
      if (!tsBlocksEmpty[i] && greater(minEndKey, endKey[i])) {
        minEndKey = endKey[i];
      }
    }
    this.targetKey = minEndKey;
    for (int i = 0; i < tsBlockCount; i++) {
      if (!tsBlocksEmpty[i] && (greater(minEndKey, startKey[i]) || minEndKey.equals(startKey[i]))) {
        targetTsBlockIndex.add(i);
      }
    }
    return targetTsBlockIndex;
  }

  /** Comparator */
  @Override
  public boolean greater(
      TsBlock.TsBlockSingleColumnIterator t, TsBlock.TsBlockSingleColumnIterator s) {
    if (Objects.equals(t.currentValue().toString(), s.currentValue().toString())) {
      return timeOrdering == Ordering.ASC
          ? t.currentTime() > s.currentTime()
          : t.currentTime() < s.currentTime();
    } else {
      return deviceOrdering == Ordering.ASC
          ? t.currentValue().toString().compareTo(s.currentValue().toString()) > 0
          : t.currentValue().toString().compareTo(s.currentValue().toString()) < 0;
    }
  }

  public boolean greater(String t, String s) {
    return deviceOrdering == Ordering.ASC ? t.compareTo(s) > 0 : t.compareTo(s) < 0;
  }

  @Override
  public boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator) {
    return deviceOrdering == Ordering.ASC
        ? targetKey.compareTo(tsBlockIterator.currentValue().toString()) >= 0
        : targetKey.compareTo(tsBlockIterator.currentValue().toString()) <= 0;
  }
}
