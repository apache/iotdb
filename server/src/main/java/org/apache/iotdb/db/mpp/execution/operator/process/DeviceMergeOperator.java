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
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.util.concurrent.Futures.successfulAsList;

/**
 * DeviceMergeOperator is responsible for merging tsBlock coming from DeviceViewOperators.
 *
 * <p>If the devices in different dataNodes are different, we need to output tsBlocks of each node
 * in order of device. If the same device exists in different nodes, the tsBlocks need to be merged
 * by time within the device.
 *
 * <p>The form of tsBlocks from input operators should be the same strictly, which is transferred by
 * DeviceViewOperator.
 */
@Deprecated
public class DeviceMergeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  private final List<String> devices;
  private final List<Operator> deviceOperators;
  private final List<TSDataType> dataTypes;
  private final TsBlockBuilder tsBlockBuilder;

  private final int inputOperatorsCount;
  private final TsBlock[] inputTsBlocks;
  // device name of inputTsBlocks[], e.g. d1 in tsBlock1, d2 in tsBlock2
  private final String[] deviceOfInputTsBlocks;
  private final boolean[] noMoreTsBlocks;

  private int curDeviceIndex;
  // the index of curDevice in inputTsBlocks
  private LinkedList<Integer> curDeviceTsBlockIndexList = new LinkedList<>();

  private boolean finished;

  private final TimeSelector timeSelector;
  private final TimeComparator comparator;

  public DeviceMergeOperator(
      OperatorContext operatorContext,
      List<String> devices,
      List<Operator> deviceOperators,
      List<TSDataType> dataTypes,
      TimeSelector selector,
      TimeComparator comparator) {
    this.operatorContext = operatorContext;
    this.devices = devices;
    this.deviceOperators = deviceOperators;
    this.inputOperatorsCount = deviceOperators.size();
    this.inputTsBlocks = new TsBlock[inputOperatorsCount];
    this.deviceOfInputTsBlocks = new String[inputOperatorsCount];
    this.noMoreTsBlocks = new boolean[inputOperatorsCount];
    this.dataTypes = dataTypes;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.timeSelector = selector;
    this.comparator = comparator;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i)) {
        ListenableFuture<?> blocked = deviceOperators.get(i).isBlocked();
        if (!blocked.isDone()) {
          listenableFutures.add(blocked);
        }
      }
    }
    return listenableFutures.isEmpty() ? NOT_BLOCKED : successfulAsList(listenableFutures);
  }

  @Override
  public TsBlock next() {
    // get new input TsBlock
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!noMoreTsBlocks[i] && isTsBlockEmpty(i) && deviceOperators.get(i).hasNextWithTimer()) {
        inputTsBlocks[i] = deviceOperators.get(i).nextWithTimer();
        if (inputTsBlocks[i] == null || inputTsBlocks[i].isEmpty()) {
          return null;
        }
        deviceOfInputTsBlocks[i] = getDeviceNameFromTsBlock(inputTsBlocks[i]);
        tryToAddCurDeviceTsBlockList(i);
      }
    }
    // move to next device
    while (curDeviceTsBlockIndexList.isEmpty() && curDeviceIndex + 1 < devices.size()) {
      getNextDeviceTsBlocks();
    }
    // process the curDeviceTsBlocks
    if (curDeviceTsBlockIndexList.size() == 1) {
      TsBlock resultTsBlock = inputTsBlocks[curDeviceTsBlockIndexList.get(0)];
      inputTsBlocks[curDeviceTsBlockIndexList.get(0)] = null;
      curDeviceTsBlockIndexList.clear();
      return resultTsBlock;
    } else {
      tsBlockBuilder.reset();
      int tsBlockSizeOfCurDevice = curDeviceTsBlockIndexList.size();
      TsBlock[] deviceTsBlocks = new TsBlock[tsBlockSizeOfCurDevice];
      TsBlockSingleColumnIterator[] tsBlockIterators =
          new TsBlockSingleColumnIterator[tsBlockSizeOfCurDevice];
      for (int i = 0; i < tsBlockSizeOfCurDevice; i++) {
        deviceTsBlocks[i] = inputTsBlocks[curDeviceTsBlockIndexList.get(i)];
        tsBlockIterators[i] = deviceTsBlocks[i].getTsBlockSingleColumnIterator();
      }
      // Use the min end time of all tsBlocks as the end time of result tsBlock
      // i.e. only one tsBlock will be consumed totally
      long currentEndTime = deviceTsBlocks[0].getEndTime();
      for (int i = 1; i < tsBlockSizeOfCurDevice; i++) {
        currentEndTime =
            comparator.getCurrentEndTime(currentEndTime, deviceTsBlocks[i].getEndTime());
      }

      TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
      ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
      while (!timeSelector.isEmpty()
          && comparator.satisfyCurEndTime(timeSelector.first(), currentEndTime)) {
        long timestamp = timeSelector.pollFirst();
        timeBuilder.writeLong(timestamp);
        // TODO process by column
        // Try to find the tsBlock that timestamp belongs to
        for (int i = 0; i < tsBlockSizeOfCurDevice; i++) {
          // TODO the same timestamp in different data region
          if (tsBlockIterators[i].hasNext() && tsBlockIterators[i].currentTime() == timestamp) {
            int rowIndex = tsBlockIterators[i].getRowIndex();
            for (int j = 0; j < valueColumnBuilders.length; j++) {
              // the jth column of rowIndex of ith tsBlock
              if (deviceTsBlocks[i].getColumn(j).isNull(rowIndex)) {
                valueColumnBuilders[j].appendNull();
                continue;
              }
              valueColumnBuilders[j].write(deviceTsBlocks[i].getColumn(j), rowIndex);
            }
            tsBlockIterators[i].next();
            break;
          }
        }
        tsBlockBuilder.declarePosition();
      }
      // update tsBlock after consuming
      int consumedTsBlockIndex = 0;
      for (int i = 0; i < tsBlockSizeOfCurDevice; i++) {
        if (tsBlockIterators[i].hasNext()) {
          int rowIndex = tsBlockIterators[i].getRowIndex();
          inputTsBlocks[curDeviceTsBlockIndexList.get(i)] =
              inputTsBlocks[curDeviceTsBlockIndexList.get(i)].subTsBlock(rowIndex);
        } else {
          inputTsBlocks[curDeviceTsBlockIndexList.get(i)] = null;
          consumedTsBlockIndex = i;
        }
      }
      curDeviceTsBlockIndexList.remove(consumedTsBlockIndex);
      return tsBlockBuilder.build();
    }
  }

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    for (int i = 0; i < inputOperatorsCount; i++) {
      if (!isTsBlockEmpty(i)) {
        return true;
      } else if (!noMoreTsBlocks[i]) {
        if (deviceOperators.get(i).hasNextWithTimer()) {
          return true;
        } else {
          noMoreTsBlocks[i] = true;
          inputTsBlocks[i] = null;
        }
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    for (Operator deviceOperator : deviceOperators) {
      deviceOperator.close();
    }
  }

  @Override
  public boolean isFinished() {
    if (finished) {
      return true;
    }
    finished = true;

    for (int i = 0; i < inputOperatorsCount; i++) {
      // has more tsBlock output from children[i] or has cached tsBlock in inputTsBlocks[i]
      if (!noMoreTsBlocks[i] || !isTsBlockEmpty(i)) {
        finished = false;
        break;
      }
    }
    return finished;
  }

  /** DeviceColumn must be the first value column of tsBlock transferred by DeviceViewOperator. */
  private String getDeviceNameFromTsBlock(TsBlock tsBlock) {
    if (tsBlock == null || tsBlock.getPositionCount() == 0 || tsBlock.getColumn(0).isNull(0)) {
      return null;
    }
    return tsBlock.getColumn(0).getBinary(0).toString();
  }

  private String getCurDeviceName() {
    return devices.get(curDeviceIndex);
  }

  private void getNextDeviceTsBlocks() {
    curDeviceIndex++;
    for (int i = 0; i < inputOperatorsCount; i++) {
      tryToAddCurDeviceTsBlockList(i);
    }
  }

  private void tryToAddCurDeviceTsBlockList(int tsBlockIndex) {
    if (deviceOfInputTsBlocks[tsBlockIndex] != null
        && deviceOfInputTsBlocks[tsBlockIndex].equals(getCurDeviceName())) {
      // add tsBlock of curDevice to a list
      curDeviceTsBlockIndexList.add(tsBlockIndex);
      // add all timestamp of curDevice to timeSelector
      int rowSize = inputTsBlocks[tsBlockIndex].getPositionCount();
      for (int row = 0; row < rowSize; row++) {
        timeSelector.add(inputTsBlocks[tsBlockIndex].getTimeByIndex(row));
      }
    }
  }

  /**
   * If the tsBlock of tsBlockIndex is null or has no more data in the tsBlock, return true; else
   * return false;
   */
  private boolean isTsBlockEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == 0;
  }

  @Override
  public long calculateMaxPeekMemory() {
    // timeSelector will cache time, we use a single time column to represent max memory cost
    long maxPeekMemory = TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    // inputTsBlocks will cache all TsBlocks returned by deviceOperators
    for (Operator operator : deviceOperators) {
      maxPeekMemory += operator.calculateMaxReturnSize();
      maxPeekMemory += operator.calculateRetainedSizeAfterCallingNext();
    }
    for (Operator operator : deviceOperators) {
      maxPeekMemory = Math.max(maxPeekMemory, operator.calculateMaxPeekMemory());
    }
    return Math.max(maxPeekMemory, calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    return (1L + dataTypes.size()) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0, minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : deviceOperators) {
      long maxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock
    return currentRetainedSize - minChildReturnSize;
  }
}
