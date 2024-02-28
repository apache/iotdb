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
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;
import static org.apache.iotdb.db.queryengine.execution.aggregation.AccumulatorFactory.createAccumulator;

public class AggregationMergeSortOperator extends AbstractConsumeAllOperator {

  private List<Accumulator> accumulators;

  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;

  private final boolean[] noMoreTsBlocks;

  private boolean finished;

  private Map<String, List<Aggregator>> aggMap;

  private final TimeComparator timeComparator;

  private final Comparator<Binary> deviceComparator;

  private boolean currentFinished;

  private Binary currentDevice;

  private long currentTime;

  private int[] readIndex;

  List<Integer> newAggregationIdx;

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
    readIndex = new int[inputTsBlocks.length];
  }

  @Override
  public TsBlock next() throws Exception {
    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    if (!prepareInput()) {
      return null;
    }

    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();

    while (true) {
      currentDevice = null;

      for (int idx = 0; idx < inputTsBlocks.length; idx++) {
        TsBlock tsBlock = inputTsBlocks[idx];
        if (!noMoreTsBlocks[idx] && tsBlock == null) {
          return null;
        }

        if (readIndex[idx] >= tsBlock.getPositionCount()) {
          inputTsBlocks[idx] = null;
        }

        Binary device = tsBlock.getColumn(0).getBinary(readIndex[idx]);

        if (currentDevice == null) {
          currentDevice = device;
        } else {
          if (deviceComparator.compare(device, currentDevice) < 0) {
            currentDevice = device;
          }
        }
      }

      if (currentDevice == null) {
        break;
      }

      for (int idx = 0; idx < inputTsBlocks.length; idx++) {
        TsBlock tsBlock = inputTsBlocks[idx];
        if (tsBlock == null) {
          continue;
        }

        if (readIndex[idx] >= tsBlock.getPositionCount()) {
          inputTsBlocks[idx] = null;
        }

        Binary device = tsBlock.getColumn(0).getBinary(readIndex[idx]);
        if (device.equals(currentDevice)) {
          currentTime = tsBlock.getTimeColumn().getLong(readIndex[idx]);
          int cnt = 0;
          for (int i = 0; i < accumulators.size(); i++) {
            Accumulator accumulator = accumulators.get(i);
            if (newAggregationIdx.get(i) == 2) {
              accumulator.addIntermediate(tsBlock.getColumns(new int[2]{cnt++, cnt+}));
            } else {
              accumulator.addIntermediate(tsBlock.getColumns(new int[]));
            }
          }
          readIndex[idx] ++;
        }
      }

      timeBuilder.writeLong(currentTime);
      for (int i = 1; i < dataTypes.size(); i++) {
        accumulators.get(i).outputFinal(valueColumnBuilders[i]);
      }

      currentDevice = null;

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
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  private boolean isInputNotEmpty(int index) {
    return inputTsBlocks[index] != null && !inputTsBlocks[index].isEmpty();
  }
}
