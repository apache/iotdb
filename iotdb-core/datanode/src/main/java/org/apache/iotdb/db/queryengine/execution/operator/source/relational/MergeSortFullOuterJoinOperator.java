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

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class MergeSortFullOuterJoinOperator extends AbstractMergeSortJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortFullOuterJoinOperator.class);

  public MergeSortFullOuterJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int leftJoinKeyPosition,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int rightJoinKeyPosition,
      int[] rightOutputSymbolIdx,
      JoinKeyComparator joinKeyComparator,
      List<TSDataType> dataTypes,
      Type joinKeyType) {
    super(
        operatorContext,
        leftChild,
        leftJoinKeyPosition,
        leftOutputSymbolIdx,
        rightChild,
        rightJoinKeyPosition,
        rightOutputSymbolIdx,
        joinKeyComparator,
        dataTypes,
        joinKeyType);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return !leftFinished || !rightFinished;
  }

  @Override
  protected boolean prepareInput() throws Exception {
    gotCandidateBlocks();

    if (leftFinished) {
      return rightBlockNotEmpty() && gotNextRightBlock();
    }
    if (rightFinished) {
      return leftBlockNotEmpty();
    }
    return leftBlockNotEmpty() && rightBlockNotEmpty() && gotNextRightBlock();
  }

  @Override
  protected boolean processFinished() {
    if (leftFinished || rightFinished) {
      buildUseRemainingBlocks();
      return true;
    }

    // all the rightTsBlock is less than leftTsBlock, append right with empty left
    if (allRightLessThanLeft()) {
      appendRightWithEmptyLeft();
      resetRightBlockList();
      return true;
    }

    // all the leftTsBlock is less than rightTsBlock, append left with empty right
    if (allLeftLessThanRight()) {
      appendLeftWithEmptyRight();
      resetLeftBlock();
      return true;
    }

    // continue right < left, unless right >= left
    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex)) {
      if (lastMatchedRightBlock == null) {
        appendOneRightRowWithEmptyLeft();
      } else {
        // CurrentRight can only be greater or equals than lastMatchedRight.
        if (comparator.lessThan(
            lastMatchedRightBlock,
            0,
            0,
            rightBlockList.get(rightBlockListIdx),
            rightJoinKeyPosition,
            rightIndex)) {
          appendOneRightRowWithEmptyLeft();
        }
      }

      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // continue left < right, unless left >= right
    while (comparator.lessThan(
        leftBlock,
        leftJoinKeyPosition,
        leftIndex,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex)) {
      appendOneLeftRowWithEmptyRight();
      leftIndex++;
      if (leftIndex >= leftBlock.getPositionCount()) {
        resetLeftBlock();
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // has right values equals to current left, append to join result, inc leftIndex
    if (hasMatchedRightValueToProbeLeft()) {
      leftIndex++;

      if (leftIndex >= leftBlock.getPositionCount()) {
        resetLeftBlock();
        return true;
      }
    }

    return false;
  }

  @Override
  protected void recordsWhenDataMatches() {
    lastMatchedRightBlock = updateLastMatchedRightBlock(leftBlock, leftJoinKeyPosition, leftIndex);
  }

  private void buildUseRemainingBlocks() {
    if (leftFinished) {
      appendRightWithEmptyLeft();
      resetRightBlockList();
    } else {
      appendLeftWithEmptyRight();
      resetLeftBlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(leftChild)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(rightChild)
        + RamUsageEstimator.sizeOf(leftOutputSymbolIdx)
        + RamUsageEstimator.sizeOf(rightOutputSymbolIdx)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes();
  }
}
