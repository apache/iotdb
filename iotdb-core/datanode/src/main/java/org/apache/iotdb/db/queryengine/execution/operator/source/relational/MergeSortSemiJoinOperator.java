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

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Collections;
import java.util.List;

public class MergeSortSemiJoinOperator extends AbstractMergeSortJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortSemiJoinOperator.class);

  private final int outputColumnNum;

  private boolean rightHasNullValue = false;

  public MergeSortSemiJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int leftJoinKeyPosition,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int rightJoinKeyPosition,
      JoinKeyComparator joinKeyComparator,
      List<TSDataType> dataTypes) {
    super(
        operatorContext,
        leftChild,
        new int[] {leftJoinKeyPosition},
        leftOutputSymbolIdx,
        rightChild,
        new int[] {rightJoinKeyPosition},
        null,
        Collections.singletonList(joinKeyComparator),
        dataTypes);
    outputColumnNum = dataTypes.size();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return !leftFinished;
  }

  @Override
  protected boolean prepareInput() throws Exception {
    gotCandidateBlocks();
    if (rightFinished) {
      return leftBlockNotEmpty();
    }
    return leftBlockNotEmpty() && rightBlockNotEmpty() && gotNextRightBlock();
  }

  @Override
  protected boolean processFinished() {
    if (rightFinished) {
      appendAllLeftBlock();
      return true;
    }

    // skip all NULL values in right, because NULL value will not match the left value
    while (currentRightHasNullValue()) {
      rightHasNullValue = true;
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }
    // all the join keys in rightTsBlock are less than leftTsBlock, just skip right
    if (allRightLessThanLeft()) {
      resetRightBlockList();
      return true;
    }

    // all the join Keys in leftTsBlock are less than rightTsBlock, just append the left value
    if (allLeftLessThanRight()) {
      appendAllLeftBlock();
      resetLeftBlock();
      return true;
    }

    // continue right < left, until right >= left
    while (lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPositions,
        rightIndex,
        leftBlock,
        leftJoinKeyPositions,
        leftIndex)) {
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // if current left is null, append null to result
    while (currentLeftHasNullValue()) {
      appendNullValueToResult();
      if (leftFinishedWithIncIndex()) {
        return true;
      }
    }

    // continue left < right, until left >= right
    while (lessThan(
        leftBlock,
        leftJoinKeyPositions,
        leftIndex,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPositions,
        rightIndex)) {
      appendWhenNotMatch();
      leftIndex++;
      if (leftIndex >= leftBlock.getPositionCount()) {
        resetLeftBlock();
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // has right value equals to current left, append to join result, inc leftIndex
    return hasMatchedRightValueToProbeLeft() && leftFinishedWithIncIndex();
  }

  @Override
  protected boolean hasMatchedRightValueToProbeLeft() {
    boolean matches =
        equalsTo(
            leftBlock,
            leftJoinKeyPositions,
            leftIndex,
            rightBlockList.get(rightBlockListIdx),
            rightJoinKeyPositions,
            rightIndex);
    if (matches) {
      appendValueToResult(true);
    } else {
      appendWhenNotMatch();
    }

    return matches;
  }

  private void appendWhenNotMatch() {
    // current left won't match any right, append left with false SemiJoin result
    if (!rightHasNullValue) {
      appendValueToResult(false);
    } else {
      // if right has null value, append null to result. This behaves like MySQL and Trino.
      appendNullValueToResult();
    }
  }

  private void appendValueToResult(boolean matches) {
    appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);
    ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(outputColumnNum - 1);
    columnBuilder.writeBoolean(matches);
    resultBuilder.declarePosition();
  }

  private void appendNullValueToResult() {
    appendLeftBlockData(leftOutputSymbolIdx, resultBuilder, leftBlock, leftIndex);
    ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(outputColumnNum - 1);
    columnBuilder.appendNull();
    resultBuilder.declarePosition();
  }

  private void appendAllLeftBlock() {
    if (rightHasNullValue) {
      while (leftBlockNotEmpty()) {
        appendNullValueToResult();
        leftIndex++;
      }
    } else {
      while (leftBlockNotEmpty()) {
        appendValueToResult(false);
        leftIndex++;
      }
    }
  }

  @Override
  protected void recordsWhenDataMatches() {
    // do nothing
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
