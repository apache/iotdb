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

import org.apache.iotdb.commons.exception.runtime.UnSupportedDataTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class TopKOperator implements ProcessOperator {
  private final OperatorContext operatorContext;

  private final List<Operator> deviceOperators;
  private int deviceIndex;

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;

  // max heap, revered of MergeSortHeap in MergeSortOperator
  private final MergeSortHeap mergeSortHeap;
  private final Comparator<SortKey> comparator;
  // value in LIMIT
  private final int topValue;

  // final query result of TopKOperator and the returned size of topKResult
  private MergeSortKey[] topKResult;
  private int resultReturnSize = 0;

  // represent the key of mergeSortHeap, which is `TsBlock and its index`
  private TsBlock tmpResultTsBlock;
  private int tmpResultTsBlockIdx;

  // if order by time, timeOrderPriority is highest, and no order by expression
  // the data of every childOperator is in order
  private final boolean childrenDataInOrder;

  public TopKOperator(
      OperatorContext operatorContext,
      List<Operator> deviceOperators,
      List<TSDataType> dataTypes,
      Comparator<SortKey> comparator,
      int topValue,
      boolean childrenDataInOrder) {
    this.operatorContext = operatorContext;
    this.deviceOperators = deviceOperators;
    this.dataTypes = dataTypes;
    this.mergeSortHeap = new MergeSortHeap(topValue, comparator.reversed());
    this.comparator = comparator;
    this.tsBlockBuilder = new TsBlockBuilder(topValue, dataTypes);
    this.topValue = topValue;
    this.childrenDataInOrder = childrenDataInOrder;
    initResultTsBlock();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (deviceIndex >= deviceOperators.size()) {
      return NOT_BLOCKED;
    }
    ListenableFuture<?> blocked = getCurDeviceOperator().isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !(deviceIndex >= deviceOperators.size() && resultReturnSize == topKResult.length);
  }

  @Override
  public TsBlock next() throws Exception {
    if (deviceIndex >= deviceOperators.size() && resultReturnSize < topKResult.length) {
      return getResultFromCachedTopKResult();
    }

    if (!getCurDeviceOperator().hasNextWithTimer()) {
      closeCurDeviceOperator();
      if (deviceIndex == deviceOperators.size()) {
        return getResultFromMaxHeap(mergeSortHeap);
      }
      return null;
    }

    TsBlock currentTsBlock = getCurDeviceOperator().nextWithTimer();
    if (currentTsBlock == null) {
      return null;
    }

    boolean skipCurrentBatch = false;
    for (int idx = 0; idx < currentTsBlock.getPositionCount(); idx++) {
      if (mergeSortHeap.getHeapSize() < topValue) {
        updateTsBlockValue(currentTsBlock, idx, -1);
      } else {
        if (comparator.compare(new MergeSortKey(currentTsBlock, idx), mergeSortHeap.peek()) < 0) {
          MergeSortKey peek = mergeSortHeap.poll();
          updateTsBlockValue(currentTsBlock, idx, peek.rowIndex);
        } else if (childrenDataInOrder) {
          skipCurrentBatch = true;
          break;
        }
      }
    }

    // if current childIdx TsBlock has no value to put into heap
    // the remaining data will also have no value to put int heap
    if (skipCurrentBatch) {
      closeCurDeviceOperator();
      if (deviceIndex == deviceOperators.size()) {
        return getResultFromMaxHeap(mergeSortHeap);
      }
      return null;
    }

    return null;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // traverse each child serial,
    // so no need to accumulate the returnSize and retainedSize of each child
    long maxPeekMemory = calculateMaxReturnSize();
    for (Operator operator : deviceOperators) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemory());
    }
    return Math.max(maxPeekMemory, topValue * getMemoryUsageOfOneMergeSortKey() * 2);
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return (topValue - resultReturnSize) * getMemoryUsageOfOneMergeSortKey();
  }

  private void initResultTsBlock() {
    int positionCount = topValue;
    Column[] columns = new Column[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      switch (dataTypes.get(i)) {
        case BOOLEAN:
          columns[i] =
              new BooleanColumn(
                  positionCount,
                  Optional.of(new boolean[positionCount]),
                  new boolean[positionCount]);
          break;
        case INT32:
          columns[i] =
              new IntColumn(
                  positionCount, Optional.of(new boolean[positionCount]), new int[positionCount]);
          break;
        case INT64:
          columns[i] =
              new LongColumn(
                  positionCount, Optional.of(new boolean[positionCount]), new long[positionCount]);
          break;
        case FLOAT:
          columns[i] =
              new FloatColumn(
                  positionCount, Optional.of(new boolean[positionCount]), new float[positionCount]);
          break;
        case DOUBLE:
          columns[i] =
              new DoubleColumn(
                  positionCount,
                  Optional.of(new boolean[positionCount]),
                  new double[positionCount]);
          break;
        case TEXT:
          columns[i] =
              new BinaryColumn(
                  positionCount,
                  Optional.of(new boolean[positionCount]),
                  new Binary[positionCount]);
          break;
        default:
          throw new UnSupportedDataTypeException("Unknown datatype: " + dataTypes.get(i));
      }
    }
    this.tmpResultTsBlock =
        new TsBlock(positionCount, new TimeColumn(positionCount, new long[positionCount]), columns);
  }

  private TsBlock getResultFromMaxHeap(MergeSortHeap mergeSortHeap) {
    int cnt = mergeSortHeap.getHeapSize();
    topKResult = new MergeSortKey[cnt];
    while (!mergeSortHeap.isEmpty()) {
      topKResult[--cnt] = mergeSortHeap.poll();
    }

    return getResultFromCachedTopKResult();
  }

  private TsBlock getResultFromCachedTopKResult() {
    tsBlockBuilder.reset();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = resultReturnSize; i < topKResult.length; i++) {
      MergeSortKey mergeSortKey = topKResult[i];
      TsBlock targetBlock = mergeSortKey.tsBlock;
      tsBlockBuilder
          .getTimeColumnBuilder()
          .writeLong(targetBlock.getTimeByIndex(mergeSortKey.rowIndex));
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (targetBlock.getColumn(j).isNull(mergeSortKey.rowIndex)) {
          valueColumnBuilders[j].appendNull();
          continue;
        }
        valueColumnBuilders[j].write(targetBlock.getColumn(j), mergeSortKey.rowIndex);
      }
      resultReturnSize += 1;
      tsBlockBuilder.declarePosition();

      if (tsBlockBuilder.isFull()) {
        return tsBlockBuilder.build();
      }
    }

    return tsBlockBuilder.build();
  }

  private long getMemoryUsageOfOneMergeSortKey() {
    long memory = 0;
    for (TSDataType dataType : dataTypes) {
      switch (dataType) {
        case BOOLEAN:
          memory += 1;
          break;
        case INT32:
        case FLOAT:
          memory += 4;
          break;
        case INT64:
        case DOUBLE:
        case VECTOR:
          memory += 8;
          break;
        case TEXT:
          memory += 16;
          break;
        default:
          throw new UnSupportedDataTypeException("Unknown datatype: " + dataType);
      }
    }
    return memory;
  }

  private void updateTsBlockValue(TsBlock sourceTsBlock, int sourceIndex, int peekIndex) {
    if (peekIndex < 0) {
      tmpResultTsBlock.update(tmpResultTsBlockIdx, sourceTsBlock, sourceIndex);
      mergeSortHeap.push(new MergeSortKey(tmpResultTsBlock, tmpResultTsBlockIdx++));
      return;
    }

    tmpResultTsBlock.update(peekIndex, sourceTsBlock, sourceIndex);
    mergeSortHeap.push(new MergeSortKey(tmpResultTsBlock, peekIndex));
  }

  private Operator getCurDeviceOperator() {
    return deviceOperators.get(deviceIndex);
  }

  private void closeCurDeviceOperator() throws Exception {
    // close finished child
    getCurDeviceOperator().close();
    deviceOperators.set(deviceIndex, null);
    // increment index, move to next child
    deviceIndex++;
  }
}
