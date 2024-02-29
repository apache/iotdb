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

import org.apache.iotdb.db.queryengine.execution.aggregation.Accumulator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class AggregationMergeSortOperator extends AbstractConsumeAllOperator {

  private List<Accumulator> accumulators;

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;

  private final boolean[] noMoreTsBlocks;

  private boolean finished;

  private final TimeComparator timeComparator;

  private final Comparator<Binary> deviceComparator;

  private long currentTime;

  private final int[] readIndex;

  public AggregationMergeSortOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      List<TSDataType> dataTypes,
      List<Accumulator> accumulators,
      TimeComparator timeComparator,
      Comparator<Binary> deviceComparator) {
    super(operatorContext, children);
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
    this.accumulators = accumulators;
    this.timeComparator = timeComparator;
    this.deviceComparator = deviceComparator;
    this.readIndex = new int[inputTsBlocks.length];
  }

  @Override
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    // init all element in inputTsBlocks
    if (!prepareInput()) {
      return null;
    }

    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();

    while (true) {
      Binary currentDevice = null;
      boolean hashChildFinished = false;

      for (int idx = 0; idx < inputTsBlocks.length; idx++) {
        TsBlock tsBlock = inputTsBlocks[idx];
        if (noMoreTsBlocks[idx]) {
          continue;
        }

        if (tsBlock == null || readIndex[idx] >= tsBlock.getPositionCount()) {
          hashChildFinished = true;
          inputTsBlocks[idx] = null;
          readIndex[idx] = 0;
          currentDevice = null;
          break;
        }

        // if group by time, columnIndex may be greater than 0
        Binary device = tsBlock.getColumn(0).getBinary(readIndex[idx]);
        if (currentDevice == null || deviceComparator.compare(device, currentDevice) < 0) {
          currentDevice = device;
        }
      }

      if (hashChildFinished) {
        break;
      }

      for (int idx = 0; idx < inputTsBlocks.length; idx++) {
        TsBlock tsBlock = inputTsBlocks[idx];
        if (noMoreTsBlocks[idx]) {
          continue;
        }

        Binary device = tsBlock.getColumn(0).getBinary(readIndex[idx]);
        if (device.equals(currentDevice)) {
          currentTime = tsBlock.getTimeColumn().getLong(readIndex[idx]);
          int cnt = 1;
          for (Accumulator accumulator : accumulators) {
            if (accumulator.getPartialResultSize() == 2) {
              // TODO only has group by, use subColumn
              accumulator.addIntermediate(
                  new Column[] {
                    tsBlock.getColumn(cnt++).subColumn(readIndex[idx]),
                    tsBlock.getColumn(cnt++).subColumn(readIndex[idx])
                  });
            } else {
              accumulator.addIntermediate(
                  new Column[] {tsBlock.getColumn(cnt++).subColumn(readIndex[idx])});
            }
          }
          readIndex[idx]++;
        }
      }

      timeBuilder.writeLong(currentTime);
      valueColumnBuilders[0].writeBinary(currentDevice);
      for (int i = 1; i < dataTypes.size(); i++) {
        accumulators.get(i - 1).outputFinal(valueColumnBuilders[i]);
      }
      tsBlockBuilder.declarePosition();
      accumulators.forEach(Accumulator::reset);

      if (System.nanoTime() - startTime > maxRuntime || tsBlockBuilder.isFull()) {
        break;
      }
    }

    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (finished) {
      return false;
    }

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (isInputNotEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
          return true;
        } else {
          handleFinishedChild(i);
        }
      }
    }

    return false;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    boolean hasReadyChild = false;
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();

    for (int i = 0; i < inputOperatorsCount; i++) {
      if (noMoreTsBlocks[i] || isInputNotEmpty(i) || children.get(i) == null) {
        continue;
      }
      ListenableFuture<?> blocked = children.get(i).isBlocked();
      if (blocked.isDone()) {
        hasReadyChild = true;
        // only when not blocked, canCallNext[i] equals true
        canCallNext[i] = true;
      } else {
        listenableFutures.add(blocked);
      }
    }

    return (hasReadyChild || listenableFutures.isEmpty())
        ? NOT_BLOCKED
        : successfulAsList(listenableFutures);
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }

    finished = true;
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] || isInputNotEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  @Override
  public void close() throws Exception {
    for (int i = 0; i < inputOperatorsCount; i++) {
      final Operator operator = children.get(i);
      if (operator != null) {
        operator.close();
      }
    }
  }

  @Override
  protected void handleFinishedChild(int currentChildIndex) throws Exception {
    // invoking this method when children.get(currentChildIndex).hasNext return false
    noMoreTsBlocks[currentChildIndex] = true;
    inputTsBlocks[currentChildIndex] = null;
    children.get(currentChildIndex).close();
    children.set(currentChildIndex, null);
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  private boolean isInputNotEmpty(int index) {
    return inputTsBlocks[index] != null && !inputTsBlocks[index].isEmpty();
  }
}
