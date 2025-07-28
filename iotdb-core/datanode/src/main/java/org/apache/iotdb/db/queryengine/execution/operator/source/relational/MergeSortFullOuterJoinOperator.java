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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.function.BiFunction;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class MergeSortFullOuterJoinOperator extends AbstractMergeSortJoinOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MergeSortFullOuterJoinOperator.class);

  // stores last row matched join criteria, only used in outer join
  private TsBlock lastMatchedRightBlock = null;
  private final int[] lastMatchedBlockPositions;
  private final List<BiFunction<Column, Integer, Column>> updateLastMatchedRowFunctions;

  public MergeSortFullOuterJoinOperator(
      OperatorContext operatorContext,
      Operator leftChild,
      int[] leftJoinKeyPositions,
      int[] leftOutputSymbolIdx,
      Operator rightChild,
      int[] rightJoinKeyPositions,
      int[] rightOutputSymbolIdx,
      List<JoinKeyComparator> joinKeyComparators,
      List<TSDataType> dataTypes,
      List<BiFunction<Column, Integer, Column>> updateLastMatchedRowFunctions) {
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
    lastMatchedBlockPositions = new int[joinKeyComparators.size()];
    for (int i = 0; i < lastMatchedBlockPositions.length; i++) {
      lastMatchedBlockPositions[i] = i;
    }
    this.updateLastMatchedRowFunctions = updateLastMatchedRowFunctions;
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

    // all the join keys in rightTsBlocks are less than leftTsBlock, append right with empty left
    if (allRightLessThanLeft()) {
      appendRightWithEmptyLeft();
      resetRightBlockList();
      return true;
    }

    // all the join keys in leftTsBlock are less than rightTsBlock, append left with empty right
    if (allLeftLessThanRight()) {
      appendLeftWithEmptyRight();
      resetLeftBlock();
      return true;
    }

    // if exist NULL values in right, just output this row with empty left
    while (currentRightHasNullValue()) {
      appendOneRightRowWithEmptyLeft();
      if (rightFinishedWithIncIndex()) {
        return true;
      }
    }
    // continue right < left, until right >= left
    while (lessThan(
        rightBlockList.get(rightBlockListIdx),
        rightJoinKeyPositions,
        rightIndex,
        leftBlock,
        leftJoinKeyPositions,
        leftIndex)) {
      if (lastMatchedRightBlock == null) {
        appendOneRightRowWithEmptyLeft();
      } else {
        // CurrentRight can only be greater than or equal to lastMatchedRight.
        if (!equalsTo(
            lastMatchedRightBlock,
            lastMatchedBlockPositions,
            0,
            rightBlockList.get(rightBlockListIdx),
            rightJoinKeyPositions,
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

    // if exist NULL values in left, just output this row with empty right
    while (currentLeftHasNullValue()) {
      appendOneLeftRowWithEmptyRight();
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
      appendOneLeftRowWithEmptyRight();
      if (leftFinishedWithIncIndex()) {
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
  protected void recordsWhenDataMatches() {
    Column[] valueColumns = new Column[leftJoinKeyPositions.length];
    for (int i = 0; i < leftJoinKeyPositions.length; i++) {
      valueColumns[i] =
          updateLastMatchedRowFunctions
              .get(i)
              .apply(leftBlock.getColumn(leftJoinKeyPositions[i]), leftIndex);
    }
    lastMatchedRightBlock = new TsBlock(1, TIME_COLUMN_TEMPLATE, valueColumns);
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

  /** This method will be invoked only when `allRightLessThanLeft` or `leftFinished`. */
  private void appendRightWithEmptyLeft() {
    while (rightBlockListIdx < rightBlockList.size()) {

      // if `lastMatchedRightBlock` is not null, the value in `lastMatchedRightBlock` must not be
      // NULL,
      // if current right value is null, the right row with empty left will be appended in the join
      // result.
      if (lastMatchedRightBlock == null
          || !equalsTo(
              lastMatchedRightBlock,
              lastMatchedBlockPositions,
              0,
              rightBlockList.get(rightBlockListIdx),
              rightJoinKeyPositions,
              rightIndex)) {
        for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
          ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
          columnBuilder.appendNull();
        }

        appendRightBlockData(
            rightBlockList,
            rightBlockListIdx,
            rightIndex,
            leftOutputSymbolIdx,
            rightOutputSymbolIdx,
            resultBuilder);

        resultBuilder.declarePosition();
      }

      rightIndex++;
      if (rightIndex >= rightBlockList.get(rightBlockListIdx).getPositionCount()) {
        rightIndex = 0;
        rightBlockListIdx++;
      }
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
