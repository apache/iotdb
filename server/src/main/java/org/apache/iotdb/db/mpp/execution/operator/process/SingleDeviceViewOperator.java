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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.NullColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * The SingleDeviceViewOperator plays a similar role with DeviceViewOperator of adding a device
 * column to current resultSet.
 *
 * <p>Different from DeviceViewOperator which merge the resultSet from different devices,
 * SingleDeviceViewOperator only focuses on one single device, the goal of it is to add a device
 * view. It's just a transition and won't change the way data flows.
 */
public class SingleDeviceViewOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  // The size devices and deviceOperators should be the same.
  private final String device;
  private final Operator deviceOperator;
  // Used to fill columns and leave null columns which doesn't exist in some devices.
  private final List<Integer> deviceColumnIndex;
  // Column dataTypes that includes device column
  private final List<TSDataType> dataTypes;

  public SingleDeviceViewOperator(
      OperatorContext operatorContext,
      String device,
      Operator deviceOperator,
      List<Integer> deviceColumnIndex,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.device = device;
    this.deviceOperator = deviceOperator;
    this.deviceColumnIndex = deviceColumnIndex;
    this.dataTypes = dataTypes;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> blocked = deviceOperator.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    TsBlock tsBlock = deviceOperator.next();
    if (tsBlock == null) {
      return null;
    }

    // fill existing columns
    Column[] newValueColumns = new Column[dataTypes.size()];
    for (int i = 0; i < deviceColumnIndex.size(); i++) {
      newValueColumns[deviceColumnIndex.get(i)] = tsBlock.getColumn(i);
    }
    // construct device column
    ColumnBuilder deviceColumnBuilder = new BinaryColumnBuilder(null, 1);
    deviceColumnBuilder.writeBinary(new Binary(device));
    newValueColumns[0] =
        new RunLengthEncodedColumn(deviceColumnBuilder.build(), tsBlock.getPositionCount());
    // construct other null columns
    for (int i = 0; i < dataTypes.size(); i++) {
      if (newValueColumns[i] == null) {
        newValueColumns[i] = NullColumn.create(dataTypes.get(i), tsBlock.getPositionCount());
      }
    }
    return new TsBlock(tsBlock.getPositionCount(), tsBlock.getTimeColumn(), newValueColumns);
  }

  @Override
  public boolean hasNext() {
    return deviceOperator.hasNext();
  }

  @Override
  public void close() throws Exception {
    deviceOperator.close();
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = calculateMaxReturnSize() + calculateRetainedSizeAfterCallingNext();
    maxPeekMemory = Math.max(maxPeekMemory, deviceOperator.calculateMaxPeekMemory());
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    return (long) (dataTypes.size())
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return deviceOperator.calculateRetainedSizeAfterCallingNext();
  }
}
