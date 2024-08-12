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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.db.utils.sort.TableDiskSpiller;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;

public class TableStreamSortOperator extends AbstractSortOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableStreamSortOperator.class);

  private final Comparator<SortKey> streamSortComparator;

  private final int minLinesToOutput;

  // accumulated unprinted line count
  private long remainingCount = 0;

  // child operator has no more data
  private boolean noMoreDataFromChild = false;

  // unprocessed data from the previous iteration
  private TsBlock currentTsBlock = null;

  // be able to start stream outputting partial data
  private boolean canStreamOutput = false;

  private SortKey lastRow = null;

  public TableStreamSortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      String folderPath,
      Comparator<SortKey> comparator,
      Comparator<SortKey> streamSortComparator,
      int minLinesToOutput) {
    super(
        operatorContext,
        inputOperator,
        dataTypes,
        new TableDiskSpiller(folderPath, folderPath + operatorContext.getOperatorId(), dataTypes),
        comparator);
    this.streamSortComparator = streamSortComparator;
    this.minLinesToOutput = minLinesToOutput;
  }

  @Override
  public TsBlock next() throws Exception {
    if (canStreamOutput || !tsBlockBuilder.isEmpty()) {

      buildResult();

      // this time stream output data has been consumed up
      if (!hasMoreSortedData()) {
        canStreamOutput = false;
        // clear spilled disk space and memory data structure
        resetSortRelatedResource();
      }

      if (tsBlockBuilder.isFull() || consumedUp()) {
        int rowCount = tsBlockBuilder.getPositionCount();
        TsBlock res =
            tsBlockBuilder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, rowCount));
        remainingCount -= rowCount;
        tsBlockBuilder.reset();
        return res;
      }
    }

    if (currentTsBlock != null) {
      cacheTsBlock(currentTsBlock);
      currentTsBlock = null;
    }

    long startTime = System.nanoTime();
    if (!inputOperator.hasNextWithTimer()) {
      noMoreDataFromChild = true;
      canStreamOutput = true;
    } else {
      try {
        // init currentTsBlock from child operator
        currentTsBlock = inputOperator.nextWithTimer();
        if (currentTsBlock == null || currentTsBlock.isEmpty()) {
          currentTsBlock = null;
          return null;
        }
        // record total sorted data size
        dataSize += currentTsBlock.getRetainedSizeInBytes();

        // if currentTsBlock line count + remainingCount is still less than minLinesToOutput, just
        // cache it
        if (currentTsBlock.getPositionCount() + remainingCount < minLinesToOutput) {
          cacheTsBlock(currentTsBlock);
          remainingCount += currentTsBlock.getPositionCount();
          currentTsBlock = null;
        } else {
          // if stream compare key of the last row of currentTsBlock is same as the last row of
          // previous TsBlock, we cannot stream output, just cache it
          if (isStreamCompareKeySame()) {
            cacheTsBlock(currentTsBlock);
            remainingCount += currentTsBlock.getPositionCount();
            currentTsBlock = null;
          } else {
            // traverse the current TsBlock backward until encountering a row with a StreamCompKey
            // different from the last row, and return the index of that row
            int endIndex = getEndIndexFromCurrentTsBlock();
            // if total count of `canOutput` lines is less than minLinesToOutput, we won't output
            // them, we just cache the whole currentTsBlock
            if (endIndex == -1 || endIndex + remainingCount + 1 < minLinesToOutput) {
              cacheTsBlock(currentTsBlock);
              remainingCount += currentTsBlock.getPositionCount();
              currentTsBlock = null;
            } else {
              // `canOutput` lines count is larger than minLinesToOutput, so we can stream output
              cacheTsBlock(currentTsBlock.getRegion(0, endIndex + 1));
              remainingCount += currentTsBlock.getPositionCount();
              canStreamOutput = true;
              currentTsBlock = currentTsBlock.subTsBlock(endIndex + 1);
            }
          }
        }
      } catch (IoTDBException e) {
        clear();
        throw e;
      } finally {
        prepareUntilReadyCost += System.nanoTime() - startTime;
      }
    }
    return null;
  }

  private boolean consumedUp() {
    return remainingCount == tsBlockBuilder.getPositionCount() && noMoreDataFromChild;
  }

  // return -1, if not found
  private int getEndIndexFromCurrentTsBlock() {
    SortKey sortKeyOfLastRow = new SortKey(currentTsBlock, currentTsBlock.getPositionCount() - 1);
    SortKey sortKeyOfFoundRow = new SortKey(currentTsBlock, currentTsBlock.getPositionCount() - 2);
    for (int index = currentTsBlock.getPositionCount() - 2; index >= 0; index--) {
      sortKeyOfFoundRow.rowIndex = index;
      if (streamSortComparator.compare(sortKeyOfFoundRow, sortKeyOfLastRow) != 0) {
        return index;
      }
    }
    return -1;
  }

  private boolean isStreamCompareKeySame() {
    return lastRow == null
        || streamSortComparator.compare(
                lastRow, new SortKey(currentTsBlock, currentTsBlock.getPositionCount() - 1))
            == 0;
  }

  protected void cacheTsBlock(TsBlock tsBlock) throws IoTDBException {
    super.cacheTsBlock(tsBlock);
    lastRow = new SortKey(tsBlock, tsBlock.getPositionCount() - 1);
  }

  @Override
  protected void appendTime(TimeColumnBuilder timeBuilder, long time) {
    // do nothing for Table related Operator
  }

  @Override
  public boolean hasNext() throws Exception {
    return super.hasNext() || !tsBlockBuilder.isEmpty() || currentTsBlock != null;
  }

  @Override
  public void close() throws Exception {
    super.close();
    lastRow = null;
    currentTsBlock = null;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(noMoreData)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
