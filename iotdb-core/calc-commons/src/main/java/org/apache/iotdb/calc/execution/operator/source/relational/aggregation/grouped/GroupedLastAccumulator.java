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

public class GroupedLastAccumulator
    implements GroupedAccumulator,
        TypeServices.GroupedValueAccessor,
        TypeServices.GroupedTimeValueConsumer {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedLastAccumulator.class);
  private final TSDataType seriesDataType;
  private final TypeServices.GroupedValueService valueService;
  private final TypeServices.GroupedValueSerializer valueSerializer;
  private final TypeServices.GroupedTimeInputReader inputReader;
  private final TypeServices.GroupedTimeValueDeserializer valueDeserializer;
  private final LongBigArray maxTimes = new LongBigArray(Long.MIN_VALUE);
  private final BooleanBigArray inits = new BooleanBigArray();
  private final BooleanBigArray initNullTimeValues = new BooleanBigArray();

  private LongBigArray longValues;
  private IntBigArray intValues;
  private FloatBigArray floatValues;
  private DoubleBigArray doubleValues;
  private BinaryBigArray binaryValues;
  private BooleanBigArray booleanValues;

  public GroupedLastAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    Type type = Type.fromTsDataType(seriesDataType);
    this.valueService = TypeServices.GROUPED_VALUE_SERVICE.call(type);
    this.valueSerializer = TypeServices.GROUPED_VALUE_SERIALIZER_SERVICE.call(type);
    this.inputReader = TypeServices.GROUPED_TIME_INPUT_READER_SERVICE.call(type);
    this.valueDeserializer = TypeServices.GROUPED_TIME_VALUE_DESERIALIZER_SERVICE.call(type);
    valueService.initialize(this);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + inits.sizeOf()
        + maxTimes.sizeOf()
        + initNullTimeValues.sizeOf()
        + valueService.sizeOf(this);
  }

  @Override
  public void setGroupCount(long groupCount) {
    maxTimes.ensureCapacity(groupCount);
    inits.ensureCapacity(groupCount);
    initNullTimeValues.ensureCapacity(groupCount);
    valueService.ensureCapacity(this, groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    // arguments[0] is value column, arguments[1] is time column
    inputReader.addInput(groupIds, arguments, mask, this);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of LAST should be BinaryColumn");

    for (int i = 0; i < groupIds.length; i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = Long.BYTES;
      boolean isOrderTimeNull = BytesUtils.bytesToBool(bytes, offset);
      offset += 1;
      int groupId = groupIds[i];

      valueDeserializer.deserialize(bytes, offset, groupId, time, isOrderTimeNull, this);
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of LAST should be BinaryColumn");
    if (inits.get(groupId) || initNullTimeValues.get(groupId)) {
      columnBuilder.writeBinary(new Binary(serializeTimeWithValue(groupId)));
      return;
    }
    columnBuilder.appendNull();
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!inits.get(groupId) && !initNullTimeValues.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }

    valueService.write(this, groupId, columnBuilder);
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    maxTimes.reset();
    inits.reset();
    initNullTimeValues.reset();
    valueService.reset(this);
  }

  private byte[] serializeTimeWithValue(int groupId) {
    int length = Long.BYTES + 1 + valueSerializer.calcTypeSize(this, groupId);
    boolean isOrderTimeNull = !inits.get(groupId);
    byte[] bytes = new byte[length];
    longToBytes(maxTimes.get(groupId), bytes, 0);
    boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
    valueSerializer.serialize(this, groupId, bytes, Long.BYTES + 1);
    return bytes;
  }

  private boolean checkAndUpdateLastTime(int groupId, long curTime) {
    if (!inits.get(groupId) || curTime > maxTimes.get(groupId)) {
      inits.set(groupId, true);
      maxTimes.set(groupId, curTime);
      return true;
    }
    return false;
  }

  private boolean checkAndUpdateNullTime(int groupId) {
    if (!inits.get(groupId) && !initNullTimeValues.get(groupId)) {
      initNullTimeValues.set(groupId, true);
      return true;
    }
    return false;
  }

  private void addIntInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateIntValue(groupId, valueColumn.getInt(position), timeColumn.getLong(position));
      } else {
        updateIntNullTimeValue(groupId, valueColumn.getInt(position));
      }
    }
  }

  protected void updateIntValue(int groupId, int value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      intValues.set(groupId, value);
    }
  }

  protected void updateIntNullTimeValue(int groupId, int value) {
    if (checkAndUpdateNullTime(groupId)) {
      intValues.set(groupId, value);
    }
  }

  private void addLongInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateLongValue(groupId, valueColumn.getLong(position), timeColumn.getLong(position));
      } else {
        updateLongNullTimeValue(groupId, valueColumn.getLong(position));
      }
    }
  }

  protected void updateLongValue(int groupId, long value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      longValues.set(groupId, value);
    }
  }

  protected void updateLongNullTimeValue(int groupId, long value) {
    if (checkAndUpdateNullTime(groupId)) {
      longValues.set(groupId, value);
    }
  }

  private void addFloatInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateFloatValue(groupId, valueColumn.getFloat(position), timeColumn.getLong(position));
      } else {
        updateFloatNullTimeValue(groupId, valueColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatValue(int groupId, float value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      floatValues.set(groupId, value);
    }
  }

  protected void updateFloatNullTimeValue(int groupId, float value) {
    if (checkAndUpdateNullTime(groupId)) {
      floatValues.set(groupId, value);
    }
  }

  private void addDoubleInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateDoubleValue(groupId, valueColumn.getDouble(position), timeColumn.getLong(position));
      } else {
        updateDoubleNullTimeValue(groupId, valueColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleValue(int groupId, double value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      doubleValues.set(groupId, value);
    }
  }

  protected void updateDoubleNullTimeValue(int groupId, double value) {
    if (checkAndUpdateNullTime(groupId)) {
      doubleValues.set(groupId, value);
    }
  }

  private void addBinaryInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateBinaryValue(groupId, valueColumn.getBinary(position), timeColumn.getLong(position));
      } else {
        updateBinaryNullTimeValue(groupId, valueColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryValue(int groupId, Binary value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      binaryValues.set(groupId, value);
    }
  }

  protected void updateBinaryNullTimeValue(int groupId, Binary value) {
    if (checkAndUpdateNullTime(groupId)) {
      binaryValues.set(groupId, value);
    }
  }

  private void addBooleanInput(
      int[] groupIds, Column valueColumn, Column timeColumn, AggregationMask mask) {
    int selectPositionCount = mask.getSelectedPositionCount();

    boolean isSelectAll = mask.isSelectAll();
    int[] selectedPositions = isSelectAll ? null : mask.getSelectedPositions();

    for (int i = 0; i < selectPositionCount; i++) {
      int position = isSelectAll ? i : selectedPositions[i];
      if (valueColumn.isNull(position)) {
        continue;
      }

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateBooleanValue(groupId, valueColumn.getBoolean(position), timeColumn.getLong(position));
      } else {
        updateBooleanNullTimeValue(groupId, valueColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanValue(int groupId, boolean value, long curTime) {
    if (checkAndUpdateLastTime(groupId, curTime)) {
      booleanValues.set(groupId, value);
    }
  }

  protected void updateBooleanNullTimeValue(int groupId, boolean value) {
    if (checkAndUpdateNullTime(groupId)) {
      booleanValues.set(groupId, value);
    }
  }

  @Override
  public RuntimeException unsupportedException() {
    return new UnSupportedDataTypeException(
        String.format(CalcMessages.UNSUPPORTED_DATA_TYPE_IN_LAST_AGGREGATION, seriesDataType));
  }

  @Override
  public void initializeIntValues() {
    intValues = new IntBigArray();
  }

  @Override
  public void initializeLongValues() {
    longValues = new LongBigArray();
  }

  @Override
  public void initializeFloatValues() {
    floatValues = new FloatBigArray();
  }

  @Override
  public void initializeDoubleValues() {
    doubleValues = new DoubleBigArray();
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
  public void updateInt(int groupId, int value, boolean timeNull, long time) {
    if (timeNull) {
      updateIntNullTimeValue(groupId, value);
    } else {
      updateIntValue(groupId, value, time);
    }
  }

  @Override
  public void updateLong(int groupId, long value, boolean timeNull, long time) {
    if (timeNull) {
      updateLongNullTimeValue(groupId, value);
    } else {
      updateLongValue(groupId, value, time);
    }
  }

  @Override
  public void updateFloat(int groupId, float value, boolean timeNull, long time) {
    if (timeNull) {
      updateFloatNullTimeValue(groupId, value);
    } else {
      updateFloatValue(groupId, value, time);
    }
  }

  @Override
  public void updateDouble(int groupId, double value, boolean timeNull, long time) {
    if (timeNull) {
      updateDoubleNullTimeValue(groupId, value);
    } else {
      updateDoubleValue(groupId, value, time);
    }
  }

  @Override
  public void updateBinary(int groupId, Binary value, boolean timeNull, long time) {
    if (timeNull) {
      updateBinaryNullTimeValue(groupId, value);
    } else {
      updateBinaryValue(groupId, value, time);
    }
  }

  @Override
  public void updateBoolean(int groupId, boolean value, boolean timeNull, long time) {
    if (timeNull) {
      updateBooleanNullTimeValue(groupId, value);
    } else {
      updateBooleanValue(groupId, value, time);
    }
  }
}
