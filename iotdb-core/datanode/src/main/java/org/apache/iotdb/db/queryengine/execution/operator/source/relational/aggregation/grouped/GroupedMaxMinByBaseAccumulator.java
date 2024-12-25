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
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.boolToBytes;
import static org.apache.tsfile.utils.BytesUtils.doubleToBytes;
import static org.apache.tsfile.utils.BytesUtils.floatToBytes;
import static org.apache.tsfile.utils.BytesUtils.intToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

/** max(x,y) returns the value of x associated with the maximum value of y over all input values. */
public abstract class GroupedMaxMinByBaseAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedAccumulator.class);

  private final TSDataType xDataType;

  private final TSDataType yDataType;

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

    switch (xDataType) {
      case INT32:
      case DATE:
        xIntValues = new IntBigArray();
        break;
      case INT64:
      case TIMESTAMP:
        xLongValues = new LongBigArray();
        break;
      case FLOAT:
        xFloatValues = new FloatBigArray();
        break;
      case DOUBLE:
        xDoubleValues = new DoubleBigArray();
        break;
      case TEXT:
      case BLOB:
      case STRING:
        xBinaryValues = new BinaryBigArray();
        break;
      case BOOLEAN:
        xBooleanValues = new BooleanBigArray();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", xDataType));
    }

    switch (yDataType) {
      case INT32:
      case DATE:
        yIntValues = new IntBigArray();
        break;
      case INT64:
      case TIMESTAMP:
        yLongValues = new LongBigArray();
        break;
      case FLOAT:
        yFloatValues = new FloatBigArray();
        break;
      case DOUBLE:
        yDoubleValues = new DoubleBigArray();
        break;
      case TEXT:
      case BLOB:
      case STRING:
        yBinaryValues = new BinaryBigArray();
        break;
      case BOOLEAN:
        yBooleanValues = new BooleanBigArray();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", yDataType));
    }
  }

  @Override
  public long getEstimatedSize() {
    long valuesSize = 0;
    switch (xDataType) {
      case INT32:
      case DATE:
        valuesSize += xIntValues.sizeOf();
        break;
      case INT64:
      case TIMESTAMP:
        valuesSize += xLongValues.sizeOf();
        break;
      case FLOAT:
        valuesSize += xFloatValues.sizeOf();
        break;
      case DOUBLE:
        valuesSize += xDoubleValues.sizeOf();
        break;
      case TEXT:
      case STRING:
      case BLOB:
        valuesSize += xBinaryValues.sizeOf();
        break;
      case BOOLEAN:
        valuesSize += xBooleanValues.sizeOf();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", xDataType));
    }

    switch (yDataType) {
      case INT32:
      case DATE:
        valuesSize += yIntValues.sizeOf();
        break;
      case INT64:
      case TIMESTAMP:
        valuesSize += yLongValues.sizeOf();
        break;
      case FLOAT:
        valuesSize += yFloatValues.sizeOf();
        break;
      case DOUBLE:
        valuesSize += yDoubleValues.sizeOf();
        break;
      case TEXT:
      case STRING:
      case BLOB:
        valuesSize += yBinaryValues.sizeOf();
        break;
      case BOOLEAN:
        valuesSize += yBooleanValues.sizeOf();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", xDataType));
    }

    return INSTANCE_SIZE + valuesSize + inits.sizeOf() + xNulls.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    inits.ensureCapacity(groupCount);
    xNulls.ensureCapacity(groupCount);
    switch (xDataType) {
      case INT32:
      case DATE:
        xIntValues.ensureCapacity(groupCount);
        break;
      case INT64:
      case TIMESTAMP:
        xLongValues.ensureCapacity(groupCount);
        break;
      case FLOAT:
        xFloatValues.ensureCapacity(groupCount);
        break;
      case DOUBLE:
        xDoubleValues.ensureCapacity(groupCount);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        xBinaryValues.ensureCapacity(groupCount);
        break;
      case BOOLEAN:
        xBooleanValues.ensureCapacity(groupCount);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", xDataType));
    }
    switch (yDataType) {
      case INT32:
      case DATE:
        yIntValues.ensureCapacity(groupCount);
        break;
      case INT64:
      case TIMESTAMP:
        yLongValues.ensureCapacity(groupCount);
        break;
      case FLOAT:
        yFloatValues.ensureCapacity(groupCount);
        break;
      case DOUBLE:
        yDoubleValues.ensureCapacity(groupCount);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        yBinaryValues.ensureCapacity(groupCount);
        break;
      case BOOLEAN:
        yBooleanValues.ensureCapacity(groupCount);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in : %s", xDataType));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    inits.reset();
    xNulls.reset();
    switch (xDataType) {
      case INT32:
      case DATE:
        xIntValues.reset();
        break;
      case INT64:
      case TIMESTAMP:
        xLongValues.reset();
        break;
      case FLOAT:
        xFloatValues.reset();
        break;
      case DOUBLE:
        xDoubleValues.reset();
        break;
      case TEXT:
      case BLOB:
      case STRING:
        xBinaryValues.reset();
        break;
      case BOOLEAN:
        xBooleanValues.reset();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", xDataType));
    }

    switch (yDataType) {
      case INT32:
      case DATE:
        yIntValues.reset();
        break;
      case INT64:
      case TIMESTAMP:
        yLongValues.reset();
        break;
      case FLOAT:
        yFloatValues.reset();
        break;
      case DOUBLE:
        yDoubleValues.reset();
        break;
      case TEXT:
      case BLOB:
      case STRING:
        yBinaryValues.reset();
        break;
      case BOOLEAN:
        yBooleanValues.reset();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", yDataType));
    }
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    switch (yDataType) {
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments);
        return;
      case TEXT:
      case BLOB:
      case STRING:
        addBinaryInput(groupIds, arguments);
        return;
      case BOOLEAN:
        addBooleanInput(groupIds, arguments);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", yDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of MaxBy or MinBy should be BinaryColumn");

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
        "intermediate input and output of Mode should be BinaryColumn");

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

  private void addIntInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateIntResult(groupIds[i], arguments[1].getInt(i), arguments[0], i);
      }
    }
  }

  private void updateIntResult(int groupId, int yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yIntValues.get(groupId))) {
      inits.set(groupId, true);
      yIntValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void addLongInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateLongResult(groupIds[i], arguments[1].getLong(i), arguments[0], i);
      }
    }
  }

  private void updateLongResult(int groupId, long yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yLongValues.get(groupId))) {
      inits.set(groupId, true);
      yLongValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void addFloatInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateFloatResult(groupIds[i], arguments[1].getFloat(i), arguments[0], i);
      }
    }
  }

  private void updateFloatResult(int groupId, float yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yFloatValues.get(groupId))) {
      inits.set(groupId, true);
      yFloatValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void addDoubleInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateDoubleResult(groupIds[i], arguments[1].getDouble(i), arguments[0], i);
      }
    }
  }

  private void updateDoubleResult(int groupId, double yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yDoubleValues.get(groupId))) {
      inits.set(groupId, true);
      yDoubleValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void addBinaryInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateBinaryResult(groupIds[i], arguments[1].getBinary(i), arguments[0], i);
      }
    }
  }

  private void updateBinaryResult(int groupId, Binary yValue, Column xColumn, int xIndex) {
    if (!inits.get(groupId) || check(yValue, yBinaryValues.get(groupId))) {
      inits.set(groupId, true);
      yBinaryValues.set(groupId, yValue);
      updateX(groupId, xColumn, xIndex);
    }
  }

  private void addBooleanInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[1].isNull(i)) {
        updateBooleanResult(groupIds[i], arguments[1].getBoolean(i), arguments[0], i);
      }
    }
  }

  private void updateBooleanResult(int groupId, boolean yValue, Column xColumn, int xIndex) {
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
    switch (xDataType) {
      case INT32:
      case DATE:
        columnBuilder.writeInt(xIntValues.get(groupId));
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(xLongValues.get(groupId));
        break;
      case FLOAT:
        columnBuilder.writeFloat(xFloatValues.get(groupId));
        break;
      case DOUBLE:
        columnBuilder.writeDouble(xDoubleValues.get(groupId));
        break;
      case TEXT:
      case STRING:
      case BLOB:
        columnBuilder.writeBinary(xBinaryValues.get(groupId));
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(xBooleanValues.get(groupId));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", xDataType));
    }
  }

  private void updateX(int groupId, Column xColumn, int xIndex) {
    if (xColumn.isNull(xIndex)) {
      xNulls.set(groupId, true);
    } else {
      xNulls.set(groupId, false);
      switch (xDataType) {
        case INT32:
        case DATE:
          xIntValues.set(groupId, xColumn.getInt(xIndex));
          break;
        case INT64:
        case TIMESTAMP:
          xLongValues.set(groupId, xColumn.getLong(xIndex));
          break;
        case FLOAT:
          xFloatValues.set(groupId, xColumn.getFloat(xIndex));
          break;
        case DOUBLE:
          xDoubleValues.set(groupId, xColumn.getDouble(xIndex));
          break;
        case TEXT:
        case STRING:
        case BLOB:
          xBinaryValues.set(groupId, xColumn.getBinary(xIndex));
          break;
        case BOOLEAN:
          xBooleanValues.set(groupId, xColumn.getBoolean(xIndex));
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type : %s", xDataType));
      }
    }
  }

  private byte[] serialize(int groupId) {
    boolean xNull = xNulls.get(groupId);
    int yLength = calculateValueLength(groupId, yDataType, false);
    int length = yLength + 1 + (xNull ? 0 : calculateValueLength(groupId, xDataType, true));
    byte[] bytes = new byte[length];

    writeIntermediate(groupId, false, yDataType, bytes, 0);
    boolToBytes(xNull, bytes, yLength);
    if (!xNull) {
      writeIntermediate(groupId, true, xDataType, bytes, yLength + 1);
    }

    return bytes;
  }

  private void writeIntermediate(
      int groupId, boolean isX, TSDataType dataType, byte[] bytes, int offset) {
    switch (dataType) {
      case INT32:
      case DATE:
        if (isX) {
          intToBytes(xIntValues.get(groupId), bytes, offset);
        } else {
          intToBytes(yIntValues.get(groupId), bytes, offset);
        }
        break;
      case INT64:
      case TIMESTAMP:
        if (isX) {
          longToBytes(xLongValues.get(groupId), bytes, offset);
        } else {
          longToBytes(yLongValues.get(groupId), bytes, offset);
        }
        break;
      case FLOAT:
        if (isX) {
          floatToBytes(xFloatValues.get(groupId), bytes, offset);
        } else {
          floatToBytes(yFloatValues.get(groupId), bytes, offset);
        }
        break;
      case DOUBLE:
        if (isX) {
          doubleToBytes(xDoubleValues.get(groupId), bytes, offset);
        } else {
          doubleToBytes(yDoubleValues.get(groupId), bytes, offset);
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        byte[] values =
            isX ? xBinaryValues.get(groupId).getValues() : yBinaryValues.get(groupId).getValues();
        intToBytes(values.length, bytes, offset);
        System.arraycopy(values, 0, bytes, offset + Integer.BYTES, values.length);
        break;
      case BOOLEAN:
        if (isX) {
          boolToBytes(yBooleanValues.get(groupId), bytes, offset);
        } else {
          boolToBytes(yBooleanValues.get(groupId), bytes, offset);
        }
        break;

      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", dataType));
    }
  }

  private int calculateValueLength(int groupId, TSDataType dataType, boolean isX) {
    switch (dataType) {
      case INT32:
      case DATE:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
        return Long.BYTES;
      case FLOAT:
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case TEXT:
      case BLOB:
      case STRING:
        return Integer.BYTES
            + (isX
                ? xBinaryValues.get(groupId).getValues().length
                : yBinaryValues.get(groupId).getValues().length);
      case BOOLEAN:
        return 1;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type: %s", dataType));
    }
  }

  private void updateFromBytesIntermediateInput(int groupId, byte[] bytes) {
    // long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
    int offset = 0;
    // Use Column to store x value
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(xDataType));
    ColumnBuilder columnBuilder = builder.getValueColumnBuilders()[0];
    switch (yDataType) {
      case INT32:
      case DATE:
        int intMaxVal = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateIntResult(groupId, intMaxVal, columnBuilder.build(), 0);
        break;
      case INT64:
      case TIMESTAMP:
        long longMaxVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateLongResult(groupId, longMaxVal, columnBuilder.build(), 0);
        break;
      case FLOAT:
        float floatMaxVal = BytesUtils.bytesToFloat(bytes, offset);
        offset += Float.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateFloatResult(groupId, floatMaxVal, columnBuilder.build(), 0);
        break;
      case DOUBLE:
        double doubleMaxVal = BytesUtils.bytesToDouble(bytes, offset);
        offset += Long.BYTES;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateDoubleResult(groupId, doubleMaxVal, columnBuilder.build(), 0);
        break;
      case STRING:
      case TEXT:
      case BLOB:
        int length = BytesUtils.bytesToInt(bytes, offset);
        offset += Integer.BYTES;
        Binary binaryMaxVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
        offset += length;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateBinaryResult(groupId, binaryMaxVal, columnBuilder.build(), 0);
        break;
      case BOOLEAN:
        boolean booleanVal = BytesUtils.bytesToBool(bytes, offset);
        offset += 1;
        readXFromBytesIntermediateInput(bytes, offset, columnBuilder);
        updateBooleanResult(groupId, booleanVal, columnBuilder.build(), 0);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", yDataType));
    }
  }

  private void readXFromBytesIntermediateInput(
      byte[] bytes, int offset, ColumnBuilder columnBuilder) {
    boolean isXNull = BytesUtils.bytesToBool(bytes, offset);
    offset += 1;
    if (isXNull) {
      columnBuilder.appendNull();
    } else {
      switch (xDataType) {
        case INT32:
        case DATE:
          columnBuilder.writeInt(BytesUtils.bytesToInt(bytes, offset));
          break;
        case INT64:
        case TIMESTAMP:
          columnBuilder.writeLong(BytesUtils.bytesToLongFromOffset(bytes, 8, offset));
          break;
        case FLOAT:
          columnBuilder.writeFloat(BytesUtils.bytesToFloat(bytes, offset));
          break;
        case DOUBLE:
          columnBuilder.writeDouble(BytesUtils.bytesToDouble(bytes, offset));
          break;
        case TEXT:
        case STRING:
        case BLOB:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          columnBuilder.writeBinary(new Binary(BytesUtils.subBytes(bytes, offset, length)));
          break;
        case BOOLEAN:
          columnBuilder.writeBoolean(BytesUtils.bytesToBool(bytes, offset));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type : %s", xDataType));
      }
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
