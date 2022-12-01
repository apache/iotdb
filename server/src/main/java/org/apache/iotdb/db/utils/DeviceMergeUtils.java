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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DeviceMergeUtils extends MergeSortUtils {

  String[] startKey;
  String[] endKey;

  public DeviceMergeUtils(List<SortItem> sortItemList, int childNum) {
    this.deviceOrdering = sortItemList.get(0).getOrdering();
    this.timeOrdering = sortItemList.get(1).getOrdering();
    this.tsBlockCount = childNum;
    this.startKey = new String[tsBlockCount];
    this.endKey = new String[tsBlockCount];
    this.tsBlocksExist = new boolean[tsBlockCount];
    this.tsBlocks = new TsBlock[tsBlockCount];
    this.keyValueSelector = new KeyValueSelector(tsBlockCount);
  }

  @Override
  public void addTsBlock(TsBlock tsBlock, int index) {
    this.tsBlocks[index] = tsBlock;
    startKey[index] = tsBlock.getColumn(0).getBinary(0).toString();
    endKey[index] = startKey[index];
    tsBlocksExist[index] = true;
  }

  @Override
  public void updateTsBlock(int index, TsBlock tsBlock) {
    if (tsBlock == null) {
      tsBlocks[index] = null;
      tsBlocksExist[index] = false;
    } else {
      tsBlocks[index] = tsBlock;
      startKey[index] = tsBlocks[index].getColumn(0).getBinary(0).toString();
    }
  }

  @Override
  public List<Integer> getTargetTsBlockIndex() {
    if (tsBlockCount == 1) {
      return Collections.singletonList(0);
    }
    return getTargetIndex(
        startKey,
        endKey,
        (a, b) -> deviceOrdering == Ordering.ASC ? a.compareTo(b) > 0 : a.compareTo(b) < 0);
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

  @Override
  public boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator) {
    return deviceOrdering == Ordering.ASC
        ? endKey[targetKeyIndex].compareTo(tsBlockIterator.currentValue().toString()) >= 0
        : endKey[targetKeyIndex].compareTo(tsBlockIterator.currentValue().toString()) <= 0;
  }
}
