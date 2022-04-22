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
package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.NullColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Since childDeviceOperatorMap should be sorted by the merge order as expected, what
 * DeviceMergeOperator need to do is traversing the device child operators, get all tsBlocks of one
 * device and transform it to the form we need, then get the next device operator until no next
 * device.
 *
 * <p>Attention! If some columns are not existing in one device, those columns will be null. e.g.
 * [s1,s2,s3] is query, but only [s1, s3] exists in device1, then the column of s2 will be filled
 * with NullColumn.
 */
public class DeviceMergeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  // <deviceName, corresponding query result operator responsible for that device>
  private Map<String, Operator> childDeviceOperatorMap;
  // Column dataTypes that includes device column
  private final List<TSDataType> dataTypes;
  // Used to fill columns and leave null columns which doesn't exist in some devices.
  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private Map<String, List<Integer>> deviceToColumnIndexMap;

  private Iterator<Entry<String, Operator>> deviceIterator;
  private Entry<String, Operator> curDeviceEntry;

  public DeviceMergeOperator(
      OperatorContext operatorContext,
      Map<String, Operator> childDeviceOperatorMap,
      List<TSDataType> dataTypes,
      Map<String, List<Integer>> deviceToColumnIndexMap) {
    this.operatorContext = operatorContext;
    this.childDeviceOperatorMap = childDeviceOperatorMap;
    this.dataTypes = dataTypes;
    this.deviceToColumnIndexMap = deviceToColumnIndexMap;

    this.deviceIterator = childDeviceOperatorMap.entrySet().iterator();
    if (deviceIterator.hasNext()) {
      curDeviceEntry = deviceIterator.next();
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    ListenableFuture<Void> blocked = curDeviceEntry.getValue().isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    TsBlock tsBlock = curDeviceEntry.getValue().next();
    List<Integer> indexes = deviceToColumnIndexMap.get(curDeviceEntry.getKey());

    // fill existing columns
    Column[] newValueColumns = new Column[dataTypes.size()];
    for (int i = 0; i < indexes.size(); i++) {
      newValueColumns[indexes.get(i)] = tsBlock.getColumn(i);
    }
    // construct device column
    ColumnBuilder deviceColumnBuilder = new BinaryColumnBuilder(null, 1);
    deviceColumnBuilder.writeObject(new Binary(curDeviceEntry.getKey()));
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
    while (!curDeviceEntry.getValue().hasNext()) {
      if (deviceIterator.hasNext()) {
        curDeviceEntry = deviceIterator.next();
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : childDeviceOperatorMap.values()) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return !this.hasNext();
  }
}
