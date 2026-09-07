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
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.boolToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

public class GroupedLastByAccumulator
    implements GroupedAccumulator,
        TypeServices.GroupedValueAccessor,
        TypeServices.GroupedByValueConsumer {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedLastByAccumulator.class);

  private final TSDataType xDataType;
  private final TSDataType yDataType;
  private final TypeServices.GroupedValueService valueService;
  private final TypeServices.GroupedValueSerializer valueSerializer;
  private final TypeServices.GroupedByInputReader inputReader;
  private final TypeServices.GroupedByValueDeserializer valueDeserializer;

  private final LongBigArray yLastTimes = new LongBigArray(Long.MIN_VALUE);

  private final BooleanBigArray inits = new BooleanBigArray();
  private final BooleanBigArray initNullTimeValues = new BooleanBigArray();

  private LongBigArray xLongValues;
  private IntBigArray xIntValues;
  private FloatBigArray xFloatValues;
  private DoubleBigArray xDoubleValues;
  private BinaryBigArray xBinaryValues;
  private BooleanBigArray xBooleanValues;

  private final BooleanBigArray xNulls = new BooleanBigArray(true);

  public GroupedLastByAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    Type type = Type.fromTsDataType(xDataType);
    this.valueService = TypeServices.GROUPED_VALUE_SERVICE.call(type);
    this.valueSerializer = TypeServices.GROUPED_VALUE_SERIALIZER_SERVICE.call(type);
    this.inputReader = TypeServices.GROUPED_BY_INPUT_READER_SERVICE.call(type);
    this.valueDeserializer = TypeServices.GROUPED_BY_VALUE_DESERIALIZER_SERVICE.call(type);
    valueService.initialize(this);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + valueService.sizeOf(this)
        + yLastTimes.sizeOf()
        + inits.sizeOf()
        + initNullTimeValues.sizeOf()
        + xNulls.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    yLastTimes.ensureCapacity(groupCount);
    inits.ensureCapacity(groupCount);
    initNullTimeValues.ensureCapacity(groupCount);
    xNulls.ensureCapacity(groupCount);
    valueService.ensureCapacity(this, groupCount);
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    yLastTimes.reset();
    inits.reset();
    initNullTimeValues.reset();
    xNulls.reset();
    valueService.reset(this);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 3, "Length of input Column[] for LAST_BY should be 3");

    // arguments[0] is x column, arguments[1] is y column, arguments[2] is time column
    inputReader.addInput(groupIds, arguments, mask, this);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of LAST_BY should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long curTime = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = Long.BYTES;
      boolean isOrderTimeNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;
      boolean isXValueNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;
      int groupId = groupIds[i];

      valueDeserializer.deserialize(
          bytes, offset, groupId, isXValueNull, curTime, isOrderTimeNull, this);
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of LAST_BY should be BinaryColumn");

    if (inits.get(groupId) || initNullTimeValues.get(groupId)) {
      columnBuilder.writeBinary(new Binary(serializeTimeWithValue(groupId)));
      return;
    }
    columnBuilder.appendNull();
  }

  private byte[] serializeTimeWithValue(int groupId) {
    boolean xNull = xNulls.get(groupId);
    int length = Long.BYTES + 2 + (xNull ? 0 : valueSerializer.calcTypeSize(this, groupId));
    byte[] bytes = new byte[length];
    boolean isOrderTimeNull = !inits.get(groupId);

    longToBytes(yLastTimes.get(groupId), bytes, 0);
    boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
    boolToBytes(xNull, bytes, Long.BYTES + 1);

    if (!xNull) {
      int valueOffset = Long.BYTES + 2;
      valueSerializer.serialize(this, groupId, bytes, valueOffset);
    }
    return bytes;
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (xNulls.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }

    valueService.write(this, groupId, columnBuilder);
  }

  private boolean checkAndUpdateLastTime(int groupId, boolean isXValueNull, long curTime) {
    if (!inits.get(groupId) || curTime > yLastTimes.get(groupId)) {
      inits.set(groupId, true);
      yLastTimes.set(groupId, curTime);

      if (isXValueNull) {
        xNulls.set(groupId, true);
        return false;
      } else {
        xNulls.set(groupId, false);
        return true;
      }
    }
    return false;
  }

  private boolean checkAndUpdateNullTime(int groupId, boolean isXValueNull) {
    if (!inits.get(groupId) && !initNullTimeValues.get(groupId)) {
      initNullTimeValues.set(groupId, true);

      if (isXValueNull) {
        xNulls.set(groupId, true);
        return false;
      } else {
        xNulls.set(groupId, false);
        return true;
      }
    }
    return false;
  }

  private void addIntInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateIntLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getInt(position),
            timeColumn.getLong(position));
      } else {
        updateIntNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getInt(position));
      }
    }
  }

  protected void updateIntLastValue(int groupId, boolean isXValueNull, int xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xIntValues.set(groupId, xValue);
    }
  }

  protected void updateIntNullTimeValue(int groupId, boolean isXValueNull, int xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xIntValues.set(groupId, xValue);
    }
  }

  private void addLongInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateLongLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getLong(position),
            timeColumn.getLong(position));
      } else {
        updateLongNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getLong(position));
      }
    }
  }

  protected void updateLongLastValue(int groupId, boolean isXValueNull, long xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xLongValues.set(groupId, xValue);
    }
  }

  protected void updateLongNullTimeValue(int groupId, boolean isXValueNull, long xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xLongValues.set(groupId, xValue);
    }
  }

  private void addFloatInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateFloatLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getFloat(position),
            timeColumn.getLong(position));
      } else {
        updateFloatNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatLastValue(
      int groupId, boolean isXValueNull, float xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xFloatValues.set(groupId, xValue);
    }
  }

  protected void updateFloatNullTimeValue(int groupId, boolean isXValueNull, float xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xFloatValues.set(groupId, xValue);
    }
  }

  private void addDoubleInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateDoubleLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getDouble(position),
            timeColumn.getLong(position));
      } else {
        updateDoubleNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleLastValue(
      int groupId, boolean isXValueNull, double xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xDoubleValues.set(groupId, xValue);
    }
  }

  protected void updateDoubleNullTimeValue(int groupId, boolean isXValueNull, double xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xDoubleValues.set(groupId, xValue);
    }
  }

  private void addBinaryInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBinaryLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getBinary(position),
            timeColumn.getLong(position));
      } else {
        updateBinaryNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryLastValue(
      int groupId, boolean isXValueNull, Binary xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xBinaryValues.set(groupId, xValue);
    }
  }

  protected void updateBinaryNullTimeValue(int groupId, boolean isXValueNull, Binary xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xBinaryValues.set(groupId, xValue);
    }
  }

  private void addBooleanInput(
      int[] groupIds, Column xColumn, Column yColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (yColumn.isNull(position)) {
        continue;
      }

      if (!timeColumn.isNull(position)) {
        updateBooleanLastValue(
            groupIds[position],
            xColumn.isNull(position),
            xColumn.getBoolean(position),
            timeColumn.getLong(position));
      } else {
        updateBooleanNullTimeValue(
            groupIds[position], xColumn.isNull(position), xColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanLastValue(
      int groupId, boolean isXValueNull, boolean xValue, long curTime) {
    if (checkAndUpdateLastTime(groupId, isXValueNull, curTime)) {
      xBooleanValues.set(groupId, xValue);
    }
  }

  protected void updateBooleanNullTimeValue(int groupId, boolean isXValueNull, boolean xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xBooleanValues.set(groupId, xValue);
    }
  }

  @Override
  public RuntimeException unsupportedException() {
    return new UnSupportedDataTypeException(
        String.format(CalcMessages.UNSUPPORTED_DATA_TYPE_IN_LAST_BY_AGGREGATION, xDataType));
  }

  @Override
  public void initializeIntValues() {
    xIntValues = new IntBigArray();
  }

  @Override
  public void initializeLongValues() {
    xLongValues = new LongBigArray();
  }

  @Override
  public void initializeFloatValues() {
    xFloatValues = new FloatBigArray();
  }

  @Override
  public void initializeDoubleValues() {
    xDoubleValues = new DoubleBigArray();
  }

  @Override
  public void initializeBinaryValues() {
    xBinaryValues = new BinaryBigArray();
  }

  @Override
  public void initializeBooleanValues() {
    xBooleanValues = new BooleanBigArray();
  }

  @Override
  public IntBigArray getIntValues() {
    return xIntValues;
  }

  @Override
  public LongBigArray getLongValues() {
    return xLongValues;
  }

  @Override
  public FloatBigArray getFloatValues() {
    return xFloatValues;
  }

  @Override
  public DoubleBigArray getDoubleValues() {
    return xDoubleValues;
  }

  @Override
  public BinaryBigArray getBinaryValues() {
    return xBinaryValues;
  }

  @Override
  public BooleanBigArray getBooleanValues() {
    return xBooleanValues;
  }

  @Override
  public void updateInt(int groupId, boolean xNull, int value, boolean timeNull, long time) {
    if (timeNull) {
      updateIntNullTimeValue(groupId, xNull, value);
    } else {
      updateIntLastValue(groupId, xNull, value, time);
    }
  }

  @Override
  public void updateLong(int groupId, boolean xNull, long value, boolean timeNull, long time) {
    if (timeNull) {
      updateLongNullTimeValue(groupId, xNull, value);
    } else {
      updateLongLastValue(groupId, xNull, value, time);
    }
  }

  @Override
  public void updateFloat(int groupId, boolean xNull, float value, boolean timeNull, long time) {
    if (timeNull) {
      updateFloatNullTimeValue(groupId, xNull, value);
    } else {
      updateFloatLastValue(groupId, xNull, value, time);
    }
  }

  @Override
  public void updateDouble(int groupId, boolean xNull, double value, boolean timeNull, long time) {
    if (timeNull) {
      updateDoubleNullTimeValue(groupId, xNull, value);
    } else {
      updateDoubleLastValue(groupId, xNull, value, time);
    }
  }

  @Override
  public void updateBinary(int groupId, boolean xNull, Binary value, boolean timeNull, long time) {
    if (timeNull) {
      updateBinaryNullTimeValue(groupId, xNull, value);
    } else {
      updateBinaryLastValue(groupId, xNull, value, time);
    }
  }

  @Override
  public void updateBoolean(
      int groupId, boolean xNull, boolean value, boolean timeNull, long time) {
    if (timeNull) {
      updateBooleanNullTimeValue(groupId, xNull, value);
    } else {
      updateBooleanLastValue(groupId, xNull, value, time);
    }
  }
}
