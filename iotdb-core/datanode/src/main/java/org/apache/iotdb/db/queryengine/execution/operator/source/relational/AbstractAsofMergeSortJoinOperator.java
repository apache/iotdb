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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

public abstract class AbstractAsofMergeSortJoinOperator extends AbstractMergeSortJoinOperator {
  private final JoinKeyComparator asofComparator;
  private final int leftAsofJoinKeyIndex;
  private final int rightAsofJoinKeyIndex;

  public AbstractAsofMergeSortJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int[] leftJoinKeyPositions,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int[] rightJoinKeyPositions,
      int[] rightOutputSymbolIdx,
      List<JoinKeyComparator> joinKeyComparators,
      List<TSDataType> dataTypes) {
    super(
        operatorContext,
        leftChild,
        leftJoinKeyPositions,
        leftOutputSymbolIdx,
        rightChild,
        rightJoinKeyPositions,
        rightOutputSymbolIdx,
        joinKeyComparators,
        dataTypes);
    this.asofComparator = joinKeyComparators.get(joinKeyComparators.size() - 1);
    this.leftAsofJoinKeyIndex = leftJoinKeyPositions[leftJoinKeyPositions.length - 1];
    this.rightAsofJoinKeyIndex = rightJoinKeyPositions[rightJoinKeyPositions.length - 1];
  }

  // check if the last value of the right is less or equal than left
  protected boolean allRightLessOrEqualThanLeft() {
    return lessThanOrEqual(
        rightBlockList.get(rightBlockList.size() - 1),
        rightJoinKeyPositions,
        rightBlockList.get(rightBlockList.size() - 1).getPositionCount() - 1,
        leftBlock,
        leftJoinKeyPositions,
        leftIndex);
  }

  protected boolean lessThanOrEqual(
      TsBlock leftBlock,
      int[] leftPositions,
      int lIndex,
      TsBlock rightBlock,
      int[] rightPositions,
      int rIndex) {
    // if join key size equals to 1, can return true in inner join
    if (rightPositions.length == 1 && rightBlock.getColumn(rightPositions[0]).isNull(rIndex)) {
      return true;
    }

    int lastIndex = comparators.size() - 1;
    for (int i = 0; i < lastIndex; i++) {
      if (comparators
          .get(i)
          .lessThan(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return true;
      } else if (!comparators
          .get(i)
          .equalsTo(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return false;
      }
    }

    return comparators
        .get(lastIndex)
        .lessThanOrEqual(
            leftBlock,
            leftPositions[lastIndex],
            lIndex,
            rightBlock,
            rightPositions[lastIndex],
            rIndex)
        .orElse(false);
  }

  /**
   * Examine if stop this round and rebuild rightBlockLists.
   *
   * @return true if rightBlockListIdx more than zero.
   */
  protected boolean currentRoundNeedStop() {
    if (rightBlockListIdx > 0) {
      for (int i = 0; i < rightBlockListIdx; i++) {
        long size = rightBlockList.get(i).getRetainedSizeInBytes();
        usedMemory -= size;
        memoryReservationManager.releaseMemoryCumulatively(size);
      }
      rightBlockList = rightBlockList.subList(rightBlockListIdx, rightBlockList.size());
      rightBlockListIdx = 0;
      return true;
    }

    return false;
  }

  public boolean hasMatchedRightValueToProbeLeft() {
    int tmpBlockIdx = rightBlockListIdx;
    int tmpIdx = rightIndex;
    boolean hasMatched = false;
    long matchedTime = Long.MIN_VALUE;
    while (equalsIgnoreAsof(
            leftBlock,
            leftJoinKeyPositions,
            leftIndex,
            rightBlockList.get(tmpBlockIdx),
            rightJoinKeyPositions,
            tmpIdx)
        && asofComparator
            .lessThan(
                leftBlock,
                leftAsofJoinKeyIndex,
                leftIndex,
                rightBlockList.get(rightBlockListIdx),
                rightAsofJoinKeyIndex,
                rightIndex)
            .orElse(false)) {
      long currentTime =
          rightBlockList.get(tmpBlockIdx).getColumn(rightAsofJoinKeyIndex).getLong(tmpIdx);
      if (matchedTime == Long.MIN_VALUE) {
        matchedTime = currentTime;
      } else if (currentTime != matchedTime) {
        break;
      }

      hasMatched = true;
      appendValueToResultWhenMatches(tmpBlockIdx, tmpIdx);

      tmpIdx++;
      if (tmpIdx >= rightBlockList.get(tmpBlockIdx).getPositionCount()) {
        tmpIdx = 0;
        tmpBlockIdx++;
      }

      if (tmpBlockIdx >= rightBlockList.size()) {
        break;
      }
    }
    return hasMatched;
  }

  protected boolean equalsIgnoreAsof(
      TsBlock leftBlock,
      int[] leftPositions,
      int lIndex,
      TsBlock rightBlock,
      int[] rightPositions,
      int rIndex) {
    for (int i = 0; i < comparators.size() - 1; i++) {
      if (!comparators
          .get(i)
          .equalsTo(leftBlock, leftPositions[i], lIndex, rightBlock, rightPositions[i], rIndex)
          .orElse(false)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void recordsWhenDataMatches() {
    // do nothing
  }
}
