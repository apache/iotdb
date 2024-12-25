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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class GroupedMaxAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedMaxAccumulator.class);
  private final TSDataType seriesDataType;

  private final BooleanBigArray inits = new BooleanBigArray();

  private LongBigArray longValues;
  private IntBigArray intValues;
  private FloatBigArray floatValues;
  private DoubleBigArray doubleValues;
  private BinaryBigArray binaryValues;
  private BooleanBigArray booleanValues;

  public GroupedMaxAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    switch (seriesDataType) {
      case INT32:
      case DATE:
        // Use MIN_VALUE to init NumericType to optimize updateValue method
        intValues = new IntBigArray(Integer.MIN_VALUE);
        return;
      case INT64:
      case TIMESTAMP:
        longValues = new LongBigArray(Long.MIN_VALUE);
        return;
      case FLOAT:
        floatValues = new FloatBigArray(Float.MIN_VALUE);
        return;
      case DOUBLE:
        doubleValues = new DoubleBigArray(Double.MIN_VALUE);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        binaryValues = new BinaryBigArray();
        return;
      case BOOLEAN:
        booleanValues = new BooleanBigArray();
        return;
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
        valuesSize += binaryValues.sizeOf();
        break;
      case BOOLEAN:
        valuesSize += booleanValues.sizeOf();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", seriesDataType));
    }

    return INSTANCE_SIZE + valuesSize + inits.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    inits.ensureCapacity(groupCount);
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
        binaryValues.ensureCapacity(groupCount);
        return;
      case BOOLEAN:
        booleanValues.ensureCapacity(groupCount);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", seriesDataType));
    }
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {

    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0]);
        return;
      case INT64:
      case TIMESTAMP:
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
        addBinaryInput(groupIds, arguments[0]);
        return;
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0]);
        return;
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
        case DATE:
          updateIntValue(groupIds[i], argument.getInt(i));
          break;
        case INT64:
        case TIMESTAMP:
          updateLongValue(groupIds[i], argument.getLong(i));
          break;
        case FLOAT:
          updateFloatValue(groupIds[i], argument.getFloat(i));
          break;
        case DOUBLE:
          updateDoubleValue(groupIds[i], argument.getDouble(i));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          updateBinaryValue(groupIds[i], argument.getBinary(i));
          break;
        case BOOLEAN:
          updateBooleanValue(groupIds[i], argument.getBoolean(i));
          break;
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
        case DATE:
          columnBuilder.writeInt(intValues.get(groupId));
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(longValues.get(groupId));
          break;
        case FLOAT:
          columnBuilder.writeFloat(floatValues.get(groupId));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(doubleValues.get(groupId));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          columnBuilder.writeBinary(binaryValues.get(groupId));
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(booleanValues.get(groupId));
          break;
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
        case DATE:
          columnBuilder.writeInt(intValues.get(groupId));
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(longValues.get(groupId));
          break;
        case FLOAT:
          columnBuilder.writeFloat(floatValues.get(groupId));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(doubleValues.get(groupId));
          break;
        case TEXT:
        case BLOB:
        case STRING:
          columnBuilder.writeBinary(binaryValues.get(groupId));
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(booleanValues.get(groupId));
          break;
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
      case DATE:
        intValues.reset();
        return;
      case INT64:
      case TIMESTAMP:
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
        binaryValues.reset();
        return;
      case BOOLEAN:
        booleanValues.reset();
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", seriesDataType));
    }
  }

  private void addIntInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateIntValue(groupIds[i], valueColumn.getInt(i));
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
        updateLongValue(groupIds[i], valueColumn.getLong(i));
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
        updateFloatValue(groupIds[i], valueColumn.getFloat(i));
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
        updateDoubleValue(groupIds[i], valueColumn.getDouble(i));
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

  private void addBinaryInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryValue(groupIds[i], valueColumn.getBinary(i));
      }
    }
  }

  protected void updateBinaryValue(int groupId, Binary value) {
    Binary max = binaryValues.get(groupId);
    if (!inits.get(groupId) || value.compareTo(max) > 0) {
      inits.set(groupId, true);
      binaryValues.set(groupId, value);
    }
  }

  private void addBooleanInput(int[] groupIds, Column valueColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanValue(groupIds[i], valueColumn.getBoolean(i));
      }
    }
  }

  protected void updateBooleanValue(int groupId, boolean value) {
    if (!inits.get(groupId) || value) {
      inits.set(groupId, true);
      booleanValues.set(groupId, value);
    }
  }
}
