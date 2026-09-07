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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class GroupedMaxAccumulator
    implements GroupedAccumulator,
        TypeServices.GroupedValueAccessor,
        TypeServices.GroupedInputConsumer {
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
  private final TypeServices.GroupedValueService valueService;
  private final TypeServices.GroupedInputReader inputReader;

  public GroupedMaxAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    Type type = Type.fromTsDataType(seriesDataType);
    this.valueService = TypeServices.GROUPED_VALUE_SERVICE.call(type);
    this.inputReader = TypeServices.GROUPED_INPUT_READER_SERVICE.call(type);
    valueService.initialize(this);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + valueService.sizeOf(this) + inits.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    inits.ensureCapacity(groupCount);
    valueService.ensureCapacity(this, groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {

    inputReader.addInput(groupIds, arguments, mask, this);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {

    inputReader.addIntermediate(groupIds, argument, this);
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {

    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      valueService.write(this, groupId, columnBuilder);
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
    } else {
      valueService.write(this, groupId, columnBuilder);
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    inits.reset();
    valueService.reset(this);
  }

  private void addIntInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateIntValue(groupIds[i], valueColumn.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateIntValue(groupIds[position], valueColumn.getInt(position));
        }
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

  private void addLongInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateLongValue(groupIds[i], valueColumn.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateLongValue(groupIds[position], valueColumn.getLong(position));
        }
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

  private void addFloatInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateFloatValue(groupIds[i], valueColumn.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateFloatValue(groupIds[position], valueColumn.getFloat(position));
        }
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

  private void addDoubleInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateDoubleValue(groupIds[i], valueColumn.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateDoubleValue(groupIds[position], valueColumn.getDouble(position));
        }
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

  private void addBinaryInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateBinaryValue(groupIds[i], valueColumn.getBinary(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBinaryValue(groupIds[position], valueColumn.getBinary(position));
        }
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

  private void addBooleanInput(int[] groupIds, Column valueColumn, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!valueColumn.isNull(i)) {
          updateBooleanValue(groupIds[i], valueColumn.getBoolean(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!valueColumn.isNull(position)) {
          updateBooleanValue(groupIds[position], valueColumn.getBoolean(position));
        }
      }
    }
  }

  protected void updateBooleanValue(int groupId, boolean value) {
    if (!inits.get(groupId) || value) {
      inits.set(groupId, true);
      booleanValues.set(groupId, value);
    }
  }

  @Override
  public RuntimeException unsupportedException() {
    return new UnSupportedDataTypeException(
        String.format(CalcMessages.UNSUPPORTED_DATA_TYPE_IN_MAX_AGGREGATION, seriesDataType));
  }

  @Override
  public void initializeIntValues() {
    intValues = new IntBigArray(Integer.MIN_VALUE);
  }

  @Override
  public void initializeLongValues() {
    longValues = new LongBigArray(Long.MIN_VALUE);
  }

  @Override
  public void initializeFloatValues() {
    floatValues = new FloatBigArray(Float.MIN_VALUE);
  }

  @Override
  public void initializeDoubleValues() {
    doubleValues = new DoubleBigArray(Double.MIN_VALUE);
  }

  @Override
  public void initializeBinaryValues() {
    binaryValues = new BinaryBigArray();
  }

  @Override
  public void initializeBooleanValues() {
    booleanValues = new BooleanBigArray();
  }

  @Override
  public IntBigArray getIntValues() {
    return intValues;
  }

  @Override
  public LongBigArray getLongValues() {
    return longValues;
  }

  @Override
  public FloatBigArray getFloatValues() {
    return floatValues;
  }

  @Override
  public DoubleBigArray getDoubleValues() {
    return doubleValues;
  }

  @Override
  public BinaryBigArray getBinaryValues() {
    return binaryValues;
  }

  @Override
  public BooleanBigArray getBooleanValues() {
    return booleanValues;
  }

  @Override
  public void updateInt(int groupId, int value) {
    updateIntValue(groupId, value);
  }

  @Override
  public void updateLong(int groupId, long value) {
    updateLongValue(groupId, value);
  }

  @Override
  public void updateFloat(int groupId, float value) {
    updateFloatValue(groupId, value);
  }

  @Override
  public void updateDouble(int groupId, double value) {
    updateDoubleValue(groupId, value);
  }

  @Override
  public void updateBinary(int groupId, Binary value) {
    updateBinaryValue(groupId, value);
  }

  @Override
  public void updateBoolean(int groupId, boolean value) {
    updateBooleanValue(groupId, value);
  }
}
