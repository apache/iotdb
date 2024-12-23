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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

/**
 * Since devices have been sorted by the merge order as expected, what {@link DeviceViewOperator}
 * need to do is traversing the device child operators, get all tsBlocks of one device and transform
 * it to the form we need, adding the device column and allocating value column to its expected
 * location, then get the next device operator until no next device.
 *
 * <p>The deviceOperators can be timeJoinOperator or seriesScanOperator that have not transformed
 * the result form.
 *
 * <p>Attention! If some columns are not existing in one device, those columns will be null. e.g.
 * [s1,s2,s3] is query, but only [s1, s3] exists in device1, then the column of s2 will be filled
 * with NullColumn.
 */
public class DeviceViewOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceViewOperator.class);
  private final OperatorContext operatorContext;
  // The size devices and deviceOperators should be the same.
  private final List<IDeviceID> devices;
  private final List<Operator> deviceOperators;
  // Used to fill columns and leave null columns which doesn't exist in some devices.
  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private final List<List<Integer>> deviceColumnIndex;
  // Column dataTypes that includes device column
  private final List<TSDataType> dataTypes;

  private int deviceIndex;

  public DeviceViewOperator(
      OperatorContext operatorContext,
      List<IDeviceID> devices,
      List<Operator> deviceOperators,
      List<List<Integer>> deviceColumnIndex,
      List<TSDataType> dataTypes) {
    this.operatorContext = operatorContext;
    this.devices = devices;
    this.deviceOperators = deviceOperators;
    this.deviceColumnIndex = deviceColumnIndex;
    this.dataTypes = dataTypes;

    this.deviceIndex = 0;
  }

  private String getCurDeviceName() {
    return devices.get(deviceIndex).toString();
  }

  private Operator getCurDeviceOperator() {
    return deviceOperators.get(deviceIndex);
  }

  private List<Integer> getCurDeviceIndexes() {
    return deviceColumnIndex.get(deviceIndex);
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
  public TsBlock next() throws Exception {
    if (!getCurDeviceOperator().hasNextWithTimer()) {
      // close finished child
      getCurDeviceOperator().close();
      deviceOperators.set(deviceIndex, null);
      // increment index, move to next child
      deviceIndex++;
      return null;
    }

    TsBlock tsBlock = getCurDeviceOperator().nextWithTimer();
    if (tsBlock == null) {
      return null;
    }
    List<Integer> indexes = getCurDeviceIndexes();

    // fill existing columns
    Column[] newValueColumns = new Column[dataTypes.size()];
    for (int i = 0; i < indexes.size(); i++) {
      newValueColumns[indexes.get(i)] = tsBlock.getColumn(i);
    }
    // construct device column
    ColumnBuilder deviceColumnBuilder = new BinaryColumnBuilder(null, 1);
    deviceColumnBuilder.writeBinary(new Binary(getCurDeviceName(), TSFileConfig.STRING_CHARSET));
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
  public boolean hasNext() throws Exception {
    return deviceIndex < devices.size();
  }

  @Override
  public void close() throws Exception {
    for (int i = deviceIndex, n = deviceOperators.size(); i < n; i++) {
      Operator currentChild = deviceOperators.get(i);
      if (currentChild != null) {
        deviceOperators.get(i).close();
      }
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = calculateMaxReturnSize() + calculateRetainedSizeAfterCallingNext();
    for (Operator child : deviceOperators) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemoryWithCounter());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    // null columns would be filled, so return size equals to
    // (numberOfValueColumns(dataTypes.size() - 1) + 1(timeColumn)) * columnSize + deviceColumnSize
    // size of device name column is ignored
    return (long) (dataTypes.size())
        * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long max = 0;
    for (Operator operator : deviceOperators) {
      max = Math.max(max, operator.calculateRetainedSizeAfterCallingNext());
    }
    return max;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + devices.stream().mapToLong(IDeviceID::ramBytesUsed).sum()
        + deviceOperators.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum();
  }
}
