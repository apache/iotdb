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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.doubleToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

public class GroupedAvgAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedAvgAccumulator.class);
  private final TSDataType argumentDataType;
  private final LongBigArray countValues = new LongBigArray();
  private final DoubleBigArray sumValues = new DoubleBigArray();

  public GroupedAvgAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + countValues.sizeOf() + sumValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    countValues.ensureCapacity(groupCount);
    sumValues.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of Avg should be one column");
    switch (argumentDataType) {
      case INT32:
        addIntInput(groupIds, arguments[0]);
        return;
      case INT64:
        addLongInput(groupIds, arguments[0]);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0]);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0]);
        return;
      case TEXT:
      case BLOB:
      case STRING:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", argumentDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of Avg should be BinaryColumn");

    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        deserialize(groupIds[i], argument.getBinary(i).getValues());
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of Avg should be BinaryColumn");
    if (countValues.get(groupId) == 0) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeState(groupId)));
    }
  }

  private void deserialize(int groupId, byte[] bytes) {
    countValues.add(groupId, BytesUtils.bytesToLong(bytes, Long.BYTES));
    sumValues.add(groupId, BytesUtils.bytesToDouble(bytes, Long.BYTES));
  }

  private byte[] serializeState(int groupId) {
    byte[] bytes = new byte[Long.BYTES + Double.BYTES];
    longToBytes(countValues.get(groupId), bytes, 0);
    doubleToBytes(sumValues.get(groupId), bytes, Long.BYTES);
    return bytes;
  }

  private void addIntInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        countValues.increment(groupIds[i]);
        sumValues.add(groupIds[i], column.getInt(i));
      }
    }
  }

  private void addLongInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        countValues.increment(groupIds[i]);
        sumValues.add(groupIds[i], column.getLong(i));
      }
    }
  }

  private void addFloatInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        countValues.increment(groupIds[i]);
        sumValues.add(groupIds[i], column.getFloat(i));
      }
    }
  }

  private void addDoubleInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        countValues.increment(groupIds[i]);
        sumValues.add(groupIds[i], column.getDouble(i));
      }
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    long countValue = countValues.get(groupId);
    if (countValue == 0) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId) / countValue);
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    countValues.reset();
    sumValues.reset();
  }
}
