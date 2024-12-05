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

public class MergeSortInnerJoinOperator extends AbstractMergeSortJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortInnerJoinOperator.class);

  public MergeSortInnerJoinOperator(
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

    return !leftFinished && !rightFinished;
  }

  @Override
  protected boolean prepareInput() throws Exception {
    gotCandidateBlocks();
    return leftBlockNotEmpty() && rightBlockNotEmpty() && gotNextRightBlock();
  }

  @Override
  protected boolean processFinished() {
    // all the join keys in rightTsBlock are less than leftTsBlock, just skip right
    if (allRightLessThanLeft()) {
      resetRightBlockList();
      return true;
    }

    // all the join Keys in leftTsBlock are less than rightTsBlock, just skip left
    if (allLeftLessThanRight()) {
      resetLeftBlock();
      return true;
    }

    // continue right < left, until right >= left
    while (comparator.lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex,
        leftBlock,
        leftJoinKeyPosition,
        leftIndex)) {
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // continue left < right, until left >= right
    while (comparator.lessThan(
        leftBlock,
        leftJoinKeyPosition,
        leftIndex,
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPosition,
        rightIndex)) {
      leftIndex++;
      if (leftIndex >= leftBlock.getPositionCount()) {
        resetLeftBlock();
        return true;
      }
    }
    if (currentRoundNeedStop()) {
      return true;
    }

    // has right values equal to current left, append to join result, inc leftIndex
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
