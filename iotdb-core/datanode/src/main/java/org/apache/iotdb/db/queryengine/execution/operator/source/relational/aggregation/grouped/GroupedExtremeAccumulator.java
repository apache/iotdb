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
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class GroupedExtremeAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedExtremeAccumulator.class);
  private final TSDataType seriesDataType;

  private final BooleanBigArray inits = new BooleanBigArray();

  private LongBigArray longValues;
  private IntBigArray intValues;
  private FloatBigArray floatValues;
  private DoubleBigArray doubleValues;

  public GroupedExtremeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    switch (seriesDataType) {
      case INT32:
        intValues = new IntBigArray();
        return;
      case INT64:
        longValues = new LongBigArray();
        return;
      case FLOAT:
        floatValues = new FloatBigArray();
        return;
      case DOUBLE:
        doubleValues = new DoubleBigArray();
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", seriesDataType));
    }
  }

  @Override
  public long getEstimatedSize() {
    long valuesSize = 0;
    switch (seriesDataType) {
      case INT32:
      case DATE:
        valuesSize += intValues.sizeOf();
        break;
      case INT64:
      case TIMESTAMP:
        valuesSize += longValues.sizeOf();
        break;
      case FLOAT:
        valuesSize += floatValues.sizeOf();
        break;
      case DOUBLE:
        valuesSize += doubleValues.sizeOf();
        break;
      case TEXT:
      case STRING:
      case BLOB:
        break;
      case BOOLEAN:
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", seriesDataType));
    }

    return INSTANCE_SIZE + valuesSize;
  }

  @Override
  public void setGroupCount(long groupCount) {
    switch (seriesDataType) {
      case INT32:
      case DATE:
        intValues.ensureCapacity(groupCount);
        return;
      case INT64:
      case TIMESTAMP:
        longValues.ensureCapacity(groupCount);
        return;
      case FLOAT:
        floatValues.ensureCapacity(groupCount);
        return;
      case DOUBLE:
        doubleValues.ensureCapacity(groupCount);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", seriesDataType));
    }
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {

    switch (seriesDataType) {
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
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {

    for (int i = 0; i < groupIds.length; i++) {
      if (argument.isNull(i)) {
        continue;
      }

      switch (seriesDataType) {
        case INT32:
          updateIntValue(groupIds[i], Math.abs(argument.getInt(i)));
          break;
        case INT64:
          updateLongValue(groupIds[i], Math.abs(argument.getLong(i)));
          break;
        case FLOAT:
          updateFloatValue(groupIds[i], Math.abs(argument.getFloat(i)));
          break;
        case DOUBLE:
          updateDoubleValue(groupIds[i], Math.abs(argument.getDouble(i)));
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case BOOLEAN:
        case DATE:
        case TIMESTAMP:
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      switch (seriesDataType) {
        case INT32:
          columnBuilder.writeInt(intValues.get(groupId));
          break;
        case INT64:
          columnBuilder.writeLong(longValues.get(groupId));
          break;
        case FLOAT:
          columnBuilder.writeFloat(floatValues.get(groupId));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(doubleValues.get(groupId));
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case BOOLEAN:
        case DATE:
        case TIMESTAMP:
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      switch (seriesDataType) {
        case INT32:
          columnBuilder.writeInt(intValues.get(groupId));
          break;
        case INT64:
          columnBuilder.writeLong(longValues.get(groupId));
          break;
        case FLOAT:
          columnBuilder.writeFloat(floatValues.get(groupId));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(doubleValues.get(groupId));
          break;
        case TEXT:
        case STRING:
        case BLOB:
        case BOOLEAN:
        case DATE:
        case TIMESTAMP:
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type: %s", seriesDataType));
      }
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    inits.reset();
    switch (seriesDataType) {
      case INT32:
        intValues.reset();
        return;
      case INT64:
        longValues.reset();
        return;
      case FLOAT:
        floatValues.reset();
        return;
      case DOUBLE:
        doubleValues.reset();
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", seriesDataType));
    }
  }

  private void addIntInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateIntValue(groupIds[i], Math.abs(valueColumn.getInt(i)));
      }
    }
  }

  protected void updateIntValue(int groupId, int value) {
    int max = intValues.get(groupId);
    if (value >= max) {
      inits.set(groupId, true);
      intValues.set(groupId, value);
    }
  }

  private void addLongInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateLongValue(groupIds[i], Math.abs(valueColumn.getLong(i)));
      }
    }
  }

  protected void updateLongValue(int groupId, long value) {
    long max = longValues.get(groupId);
    if (value >= max) {
      inits.set(groupId, true);
      longValues.set(groupId, value);
    }
  }

  private void addFloatInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatValue(groupIds[i], Math.abs(valueColumn.getFloat(i)));
      }
    }
  }

  protected void updateFloatValue(int groupId, float value) {
    float max = floatValues.get(groupId);
    if (value >= max) {
      inits.set(groupId, true);
      floatValues.set(groupId, value);
    }
  }

  private void addDoubleInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleValue(groupIds[i], Math.abs(valueColumn.getDouble(i)));
      }
    }
  }

  protected void updateDoubleValue(int groupId, double value) {
    double max = doubleValues.get(groupId);
    if (value >= max) {
      inits.set(groupId, true);
      doubleValues.set(groupId, value);
    }
  }
}
