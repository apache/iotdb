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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupedSumAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedSumAccumulator.class);
  private final TSDataType argumentDataType;
  private final BooleanBigArray initResult = new BooleanBigArray();
  private final DoubleBigArray sumValues = new DoubleBigArray();

  public GroupedSumAccumulator(TSDataType argumentDataType) {
    this.argumentDataType = argumentDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + initResult.sizeOf() + sumValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    sumValues.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    checkArgument(arguments.length == 1, "argument of Sum should be one column");
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

    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], argument.getDouble(i));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    if (!initResult.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId));
    }
  }

  private void addIntInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], column.getInt(i));
      }
    }
  }

  private void addLongInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], column.getLong(i));
      }
    }
  }

  private void addFloatInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], column.getFloat(i));
      }
    }
  }

  private void addDoubleInput(int[] groupIds, Column column) {
    int count = column.getPositionCount();
    for (int i = 0; i < count; i++) {
      if (!column.isNull(i)) {
        initResult.set(groupIds[i], true);
        sumValues.add(groupIds[i], column.getDouble(i));
      }
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!initResult.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(sumValues.get(groupId));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    initResult.reset();
    sumValues.reset();
  }
}
