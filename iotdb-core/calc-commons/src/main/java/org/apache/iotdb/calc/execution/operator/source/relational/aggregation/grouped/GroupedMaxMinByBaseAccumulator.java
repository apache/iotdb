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
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.boolToBytes;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public abstract class GroupedMaxMinByBaseAccumulator
    implements GroupedAccumulator, TypeServices.GroupedMaxMinByValueUpdater {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedAccumulator.class);

  private final TSDataType xDataType;

  private final TSDataType yDataType;
  private final Type xType;
  private final TypeServices.GroupedValueService xValueService;
  private final TypeServices.GroupedValueService yValueService;
  private final TypeServices.GroupedValueSerializer xValueSerializer;
  private final TypeServices.GroupedValueSerializer yValueSerializer;
  private final TypeServices.GroupedValueSetter xValueSetter;
  private final TypeServices.GroupedMaxMinByInput inputReader;
  private final TypeServices.GroupedMaxMinByIntermediateInput intermediateInputReader;
  private final TypeServices.GroupedValueAccessor xValueAccessor = new ValueAccessor(true);
  private final TypeServices.GroupedValueAccessor yValueAccessor = new ValueAccessor(false);

  private final BooleanBigArray inits = new BooleanBigArray();

  private LongBigArray xLongValues;
  private IntBigArray xIntValues;
  private FloatBigArray xFloatValues;
  private DoubleBigArray xDoubleValues;
  private BinaryBigArray xBinaryValues;
  private BooleanBigArray xBooleanValues;

  private LongBigArray yLongValues;
  private IntBigArray yIntValues;
  private FloatBigArray yFloatValues;
  private DoubleBigArray yDoubleValues;
  private BinaryBigArray yBinaryValues;
  private BooleanBigArray yBooleanValues;

  private final BooleanBigArray xNulls = new BooleanBigArray(true);

  protected GroupedMaxMinByBaseAccumulator(TSDataType xDataType, TSDataType yDataType) {
    this.xDataType = xDataType;
    this.yDataType = yDataType;
    this.xType = Type.fromTsDataType(xDataType);
    Type yType = Type.fromTsDataType(yDataType);
    this.xValueService = TypeServices.GROUPED_VALUE_SERVICE.call(xType);
    this.yValueService = TypeServices.GROUPED_VALUE_SERVICE.call(yType);
    this.xValueSerializer = TypeServices.GROUPED_VALUE_SERIALIZER_SERVICE.call(xType);
    this.yValueSerializer = TypeServices.GROUPED_VALUE_SERIALIZER_SERVICE.call(yType);
    this.xValueSetter = TypeServices.GROUPED_VALUE_SETTER_SERVICE.call(xType);
    this.inputReader = TypeServices.GROUPED_MAX_MIN_BY_INPUT_SERVICE.call(yType);
    this.intermediateInputReader =
        TypeServices.GROUPED_MAX_MIN_BY_INTERMEDIATE_INPUT_SERVICE.call(yType);
    xValueService.initialize(xValueAccessor);
    yValueService.initialize(yValueAccessor);
  }

  public LongBigArray getXLongValues() {
    return xLongValues;
  }

  public void setXLongValues(LongBigArray xLongValues) {
    this.xLongValues = xLongValues;
  }

  public IntBigArray getXIntValues() {
    return xIntValues;
  }

  public void setXIntValues(IntBigArray xIntValues) {
    this.xIntValues = xIntValues;
  }

  public FloatBigArray getXFloatValues() {
    return xFloatValues;
  }

  public void setXFloatValues(FloatBigArray xFloatValues) {
    this.xFloatValues = xFloatValues;
  }

  public DoubleBigArray getXDoubleValues() {
    return xDoubleValues;
  }

  public void setXDoubleValues(DoubleBigArray xDoubleValues) {
    this.xDoubleValues = xDoubleValues;
  }

  public BinaryBigArray getXBinaryValues() {
    return xBinaryValues;
  }

  public void setXBinaryValues(BinaryBigArray xBinaryValues) {
    this.xBinaryValues = xBinaryValues;
  }

  public BooleanBigArray getXBooleanValues() {
    return xBooleanValues;
  }

  public void setXBooleanValues(BooleanBigArray xBooleanValues) {
    this.xBooleanValues = xBooleanValues;
  }

  public LongBigArray getYLongValues() {
    return yLongValues;
  }

  public void setYLongValues(LongBigArray yLongValues) {
    this.yLongValues = yLongValues;
  }

  public IntBigArray getYIntValues() {
    return yIntValues;
  }

  public void setYIntValues(IntBigArray yIntValues) {
    this.yIntValues = yIntValues;
  }

  public FloatBigArray getYFloatValues() {
    return yFloatValues;
  }

  public void setYFloatValues(FloatBigArray yFloatValues) {
    this.yFloatValues = yFloatValues;
  }

  public DoubleBigArray getYDoubleValues() {
    return yDoubleValues;
  }

  public void setYDoubleValues(DoubleBigArray yDoubleValues) {
    this.yDoubleValues = yDoubleValues;
  }

  public BinaryBigArray getYBinaryValues() {
    return yBinaryValues;
  }

  public void setYBinaryValues(BinaryBigArray yBinaryValues) {
    this.yBinaryValues = yBinaryValues;
  }

  public BooleanBigArray getYBooleanValues() {
    return yBooleanValues;
  }

  public void setYBooleanValues(BooleanBigArray yBooleanValues) {
    this.yBooleanValues = yBooleanValues;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE
        + xValueService.sizeOf(xValueAccessor)
        + yValueService.sizeOf(yValueAccessor)
        + inits.sizeOf()
        + xNulls.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    inits.ensureCapacity(groupCount);
    xNulls.ensureCapacity(groupCount);
    xValueService.ensureCapacity(xValueAccessor, groupCount);
    yValueService.ensureCapacity(yValueAccessor, groupCount);
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    inits.reset();
    xNulls.reset();
    xValueService.reset(xValueAccessor);
    yValueService.reset(yValueAccessor);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    inputReader.add(groupIds, arguments, mask, this);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of MAX_BY/MIN_BY should be BinaryColumn");

    for (int i = 0; i < groupIds.length; i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      updateFromBytesIntermediateInput(groupIds[i], bytes);
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of MAX_BY/MIN_BY should be BinaryColumn");

    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }
    columnBuilder.writeBinary(new Binary(serialize(groupId)));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (!inits.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }
    writeX(groupId, columnBuilder);
  }

  @Override
  public void updateInt(int groupId, int yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yIntValues.get(groupId))) {
      inits.set(groupId, true);
      yIntValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  @Override
  public void updateLong(int groupId, long yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yLongValues.get(groupId))) {
      inits.set(groupId, true);
      yLongValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  @Override
  public void updateFloat(int groupId, float yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yFloatValues.get(groupId))) {
      inits.set(groupId, true);
      yFloatValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  @Override
  public void updateDouble(int groupId, double yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yDoubleValues.get(groupId))) {
      inits.set(groupId, true);
      yDoubleValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  @Override
  public void updateBinary(int groupId, Binary yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yBinaryValues.get(groupId))) {
      inits.set(groupId, true);
      yBinaryValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  @Override
  public void updateBoolean(int groupId, boolean yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yBooleanValues.get(groupId))) {
      inits.set(groupId, true);
      yBooleanValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void writeX(int groupId, ColumnBuilder columnBuilder) {
    if (xNulls.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }
    xValueService.write(xValueAccessor, groupId, columnBuilder);
  }

  private void updateX(int groupId, Column xColumn, int xIndex) {
    if (xColumn.isNull(xIndex)) {
      xNulls.set(groupId, true);
    } else {
      xNulls.set(groupId, false);
      xValueSetter.set(xValueAccessor, groupId, xColumn, xIndex);
    }
  }

  private byte[] serialize(int groupId) {
    boolean xNull = xNulls.get(groupId);
    int yLength = yValueSerializer.calcTypeSize(yValueAccessor, groupId);
    int length = yLength + 1 + (xNull ? 0 : xValueSerializer.calcTypeSize(xValueAccessor, groupId));
    byte[] bytes = new byte[length];

    yValueSerializer.serialize(yValueAccessor, groupId, bytes, 0);
    boolToBytes(xNull, bytes, yLength);
    if (!xNull) {
      xValueSerializer.serialize(xValueAccessor, groupId, bytes, yLength + 1);
    }

    return bytes;
  }

  private void updateFromBytesIntermediateInput(int groupId, byte[] bytes) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(xDataType));
    ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
    intermediateInputReader.update(groupId, bytes, xType, columnBuilder, this);
  }

  private final class ValueAccessor implements TypeServices.GroupedValueAccessor {
    private final boolean x;

    private ValueAccessor(boolean x) {
      this.x = x;
    }

    @Override
    public RuntimeException unsupportedException() {
      TSDataType dataType = x ? xDataType : yDataType;
      return new UnSupportedDataTypeException(
          String.format(CalcMessages.UNSUPPORTED_DATA_TYPE_IN_MAX_BY_MIN_BY_AGGREGATION, dataType));
    }

    @Override
    public void initializeIntValues() {
      if (x) {
        xIntValues = new IntBigArray();
      } else {
        yIntValues = new IntBigArray();
      }
    }

    @Override
    public void initializeLongValues() {
      if (x) {
        xLongValues = new LongBigArray();
      } else {
        yLongValues = new LongBigArray();
      }
    }

    @Override
    public void initializeFloatValues() {
      if (x) {
        xFloatValues = new FloatBigArray();
      } else {
        yFloatValues = new FloatBigArray();
      }
    }

    @Override
    public void initializeDoubleValues() {
      if (x) {
        xDoubleValues = new DoubleBigArray();
      } else {
        yDoubleValues = new DoubleBigArray();
      }
    }

    @Override
    public void initializeBinaryValues() {
      if (x) {
        xBinaryValues = new BinaryBigArray();
      } else {
        yBinaryValues = new BinaryBigArray();
      }
    }

    @Override
    public void initializeBooleanValues() {
      if (x) {
        xBooleanValues = new BooleanBigArray();
      } else {
        yBooleanValues = new BooleanBigArray();
      }
    }

    @Override
    public IntBigArray getIntValues() {
      return x ? xIntValues : yIntValues;
    }

    @Override
    public LongBigArray getLongValues() {
      return x ? xLongValues : yLongValues;
    }

    @Override
    public FloatBigArray getFloatValues() {
      return x ? xFloatValues : yFloatValues;
    }

    @Override
    public DoubleBigArray getDoubleValues() {
      return x ? xDoubleValues : yDoubleValues;
    }

    @Override
    public BinaryBigArray getBinaryValues() {
      return x ? xBinaryValues : yBinaryValues;
    }

    @Override
    public BooleanBigArray getBooleanValues() {
      return x ? xBooleanValues : yBooleanValues;
    }
  }

  /**
   * @param yValue Input y.
   * @param yExtremeValue Current extreme value of y.
   * @return True if yValue is the new extreme value.
   */
  protected abstract boolean check(int yValue, int yExtremeValue);

  protected abstract boolean check(long yValue, long yExtremeValue);

  protected abstract boolean check(float yValue, float yExtremeValue);

  protected abstract boolean check(double yValue, double yExtremeValue);

  protected abstract boolean check(Binary yValue, Binary yExtremeValue);

  protected abstract boolean check(boolean yValue, boolean yExtremeValue);
}
