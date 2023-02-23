/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution.operator.process.join;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.AbstractConsumeAllOperator;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This operator is used to horizontally concatenate TsBlocks with the same timestamp column.
 *
 * <p>For example, TsBlock A is: [1, 1.0; 2, 2.0], TsBlock B is: [1, true; 2, false]
 *
 * <p>HorizontallyConcat(A,B) is: [1, 1.0, true; 2, 2.0, false]
 */
public class HorizontallyConcatOperator extends AbstractConsumeAllOperator {

  /** start index for each input TsBlocks and size of it is equal to inputTsBlocks */
  private final int[] inputIndex;

  private final int outputColumnCount;

  private final TsBlockBuilder tsBlockBuilder;

  private boolean finished;

  public HorizontallyConcatOperator(
      OperatorContext operatorContext, List<Operator> children, List<TSDataType> dataTypes) {
    super(operatorContext, children);
    checkArgument(
        !children.isEmpty(), "child size of VerticallyConcatOperator should be larger than 0");
    this.inputIndex = new int[this.inputOperatorsCount];
    this.outputColumnCount = dataTypes.size();
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
  }

  @Override
  public TsBlock next() {
    if (!prepareInput()) {
      return null;
    }
    tsBlockBuilder.reset();
    // indicates how many rows can be built in this calculate
    int maxRowCanBuild = Integer.MAX_VALUE;
    for (int i = 0; i < inputOperatorsCount; i++) {
      maxRowCanBuild =
          Math.min(maxRowCanBuild, inputTsBlocks[i].getPositionCount() - inputIndex[i]);
    }

    TimeColumn firstTimeColumn = inputTsBlocks[0].getTimeColumn();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();

    // build TimeColumn according to the first inputTsBlock
    int currTsBlockIndex = inputIndex[0];
    for (int row = 0; row < maxRowCanBuild; row++) {
      timeColumnBuilder.writeLong(firstTimeColumn.getLong(currTsBlockIndex + row));
      tsBlockBuilder.declarePosition();
    }

    // build ValueColumns according to inputTsBlocks
    int valueBuilderIndex = 0; // indicate which valueColumnBuilder should use
    for (int i = 0; i < inputOperatorsCount; i++) {
      currTsBlockIndex = inputIndex[i];
      for (Column column : inputTsBlocks[i].getValueColumns()) {
        for (int row = 0; row < maxRowCanBuild; row++) {
          if (column.isNull(currTsBlockIndex + row)) {
            valueColumnBuilders[valueBuilderIndex].appendNull();
          } else {
            valueColumnBuilders[valueBuilderIndex].write(column, currTsBlockIndex + row);
          }
        }
        valueBuilderIndex++;
      }
      inputIndex[i] += maxRowCanBuild;
    }
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    return !isEmpty(readyChildIndex) || children.get(readyChildIndex).hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    return finished = isEmpty(readyChildIndex) && !children.get(readyChildIndex).hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemory());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }

    maxPeekMemory += calculateMaxReturnSize();
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    return (1L + outputColumnCount)
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0, minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock
    return currentRetainedSize - minChildReturnSize;
  }

  /**
   * If the tsBlock of tsBlockIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  @Override
  protected boolean isEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == inputIndex[tsBlockIndex];
  }

  @Override
  protected TsBlock getNextTsBlock(int childIndex) {
    inputIndex[childIndex] = 0;
    return children.get(childIndex).nextWithTimer();
  }
}
