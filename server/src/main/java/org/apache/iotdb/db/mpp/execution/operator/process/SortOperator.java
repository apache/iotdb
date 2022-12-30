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
package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TsBlockBuilder tsBlockBuilder;

  private List<MergeSortKey> cachedData;
  private final Comparator<MergeSortKey> comparator;

  public SortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      Comparator<MergeSortKey> comparator) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.cachedData = new ArrayList<>();
    this.comparator = comparator;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public TsBlock next() {
    TsBlock tsBlock = inputOperator.nextWithTimer();
    if (tsBlock == null) {
      return null;
    }
    // add data of each TsBlock from child into list
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      cachedData.add(new MergeSortKey(tsBlock, i));
    }
    // child has more data, can't calculate
    if (inputOperator.hasNextWithTimer()) {
      return null;
    }

    if (cachedData.size() > 1) {
      cachedData.sort(comparator);
    }
    TsBlock result = buildTsBlock();
    cachedData = null;
    return result;
  }

  private TsBlock buildTsBlock() {
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    cachedData.forEach(
        mergeSortKey -> {
          TsBlock tsBlock = mergeSortKey.tsBlock;
          int row = mergeSortKey.rowIndex;
          timeColumnBuilder.writeLong(tsBlock.getTimeByIndex(row));
          for (int i = 0; i < valueColumnBuilders.length; i++) {
            valueColumnBuilders[i].write(tsBlock.getColumn(i), row);
          }
          tsBlockBuilder.declarePosition();
        });
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return inputOperator.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    inputOperator.close();
  }

  @Override
  public boolean isFinished() {
    return cachedData == null;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // In fact, we need to cache all data from input.
    // Now the child of this Operator only will be ShowQueries, it only returns one Block.
    return inputOperator.calculateMaxPeekMemory()
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return inputOperator.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }
}
