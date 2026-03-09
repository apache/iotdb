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
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class AsofMergeSortInnerJoinOperator extends AbstractAsofMergeSortJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AsofMergeSortInnerJoinOperator.class);

  public AsofMergeSortInnerJoinOperator(
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
    // all the join keys in rightTsBlock are less or equal than leftTsBlock, just skip right
    if (allRightLessOrEqualThanLeft()) {
      resetRightBlockList();
      return true;
    }

    // skip all NULL values in left, because NULL value can not appear in the inner join result
    while (currentLeftHasNullValue()) {
      if (leftFinishedWithIncIndex()) {
        return true;
      }
    }

    // skip all NULL values in right, because NULL value can not appear in the inner join result
    while (currentRightHasNullValue()) {
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }

    // find first candidate of right meets the conditions
    while (lessThanOrEqual(
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

    // has right values meet condition, append to join result
    hasMatchedRightValueToProbeLeft();
    // always inc leftIndex after current left result appended
    return leftFinishedWithIncIndex();
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
