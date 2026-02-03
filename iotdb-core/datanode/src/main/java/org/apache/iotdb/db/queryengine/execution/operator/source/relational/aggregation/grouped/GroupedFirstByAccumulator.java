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

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BinaryBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BooleanBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.DoubleBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.FloatBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.IntBigArray;
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
import static org.apache.tsfile.utils.BytesUtils.boolToBytes;
import static org.apache.tsfile.utils.BytesUtils.doubleToBytes;
import static org.apache.tsfile.utils.BytesUtils.floatToBytes;
import static org.apache.tsfile.utils.BytesUtils.intToBytes;
import static org.apache.tsfile.utils.BytesUtils.longToBytes;

public class GroupedFirstByAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedFirstByAccumulator.class);

  private final TSDataType xDataType;
  private final TSDataType yDataType;

  private final LongBigArray yFirstTimes = new LongBigArray(Long.MAX_VALUE);

  private final BooleanBigArray inits = new BooleanBigArray();
  private final BooleanBigArray initNullTimeValues = new BooleanBigArray();

  private LongBigArray xLongValues;
  private IntBigArray xIntValues;
  private FloatBigArray xFloatValues;
  private DoubleBigArray xDoubleValues;
  private BinaryBigArray xBinaryValues;
  private BooleanBigArray xBooleanValues;

  private final BooleanBigArray xNulls = new BooleanBigArray(true);

  public GroupedFirstByAccumulator(TSDataType xDataType, TSDataType yDataType) {
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
      case OBJECT:
      case STRING:
        xBinaryValues = new BinaryBigArray();
        break;
      case BOOLEAN:
        xBooleanValues = new BooleanBigArray();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
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
      case OBJECT:
        valuesSize += xBinaryValues.sizeOf();
        break;
      case BOOLEAN:
        valuesSize += xBooleanValues.sizeOf();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }

    return INSTANCE_SIZE
        + valuesSize
        + yFirstTimes.sizeOf()
        + inits.sizeOf()
        + initNullTimeValues.sizeOf()
        + xNulls.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    yFirstTimes.ensureCapacity(groupCount);
    inits.ensureCapacity(groupCount);
    initNullTimeValues.ensureCapacity(groupCount);
    xNulls.ensureCapacity(groupCount);
    switch (xDataType) {
      case INT32:
      case DATE:
        xIntValues.ensureCapacity(groupCount);
        return;
      case INT64:
      case TIMESTAMP:
        xLongValues.ensureCapacity(groupCount);
        return;
      case FLOAT:
        xFloatValues.ensureCapacity(groupCount);
        return;
      case DOUBLE:
        xDoubleValues.ensureCapacity(groupCount);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        xBinaryValues.ensureCapacity(groupCount);
        return;
      case BOOLEAN:
        xBooleanValues.ensureCapacity(groupCount);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    yFirstTimes.reset();
    inits.reset();
    initNullTimeValues.reset();
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
      case OBJECT:
      case STRING:
        xBinaryValues.reset();
        break;
      case BOOLEAN:
        xBooleanValues.reset();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    checkArgument(arguments.length == 3, "Length of input Column[] for LastBy/FirstBy should be 3");

    // arguments[0] is x column, arguments[1] is y column, arguments[2] is time column
    switch (xDataType) {
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        addBinaryInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], arguments[1], arguments[2], mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of FIRST_BY should be BinaryColumn");

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

      switch (xDataType) {
        case INT32:
        case DATE:
          int intVal = isXValueNull ? 0 : BytesUtils.bytesToInt(bytes, offset);
          if (!isOrderTimeNull) {
            updateIntFirstValue(groupId, isXValueNull, intVal, curTime);
          } else {
            updateIntNullTimeValue(groupId, isXValueNull, intVal);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long longVal =
              isXValueNull ? 0 : BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          if (!isOrderTimeNull) {
            updateLongFirstValue(groupId, isXValueNull, longVal, curTime);
          } else {
            updateLongNullTimeValue(groupId, isXValueNull, longVal);
          }
          break;
        case FLOAT:
          float floatVal = isXValueNull ? 0 : BytesUtils.bytesToFloat(bytes, offset);
          if (!isOrderTimeNull) {
            updateFloatFirstValue(groupId, isXValueNull, floatVal, curTime);
          } else {
            updateFloatNullTimeValue(groupId, isXValueNull, floatVal);
          }
          break;
        case DOUBLE:
          double doubleVal = isXValueNull ? 0 : BytesUtils.bytesToDouble(bytes, offset);
          if (!isOrderTimeNull) {
            updateDoubleFirstValue(groupId, isXValueNull, doubleVal, curTime);
          } else {
            updateDoubleNullTimeValue(groupId, isXValueNull, doubleVal);
          }
          break;
        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          Binary binaryVal = null;
          if (!isXValueNull) {
            int length = BytesUtils.bytesToInt(bytes, offset);
            offset += Integer.BYTES;
            binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          }
          if (!isOrderTimeNull) {
            updateBinaryFirstValue(groupId, isXValueNull, binaryVal, curTime);
          } else {
            updateBinaryNullTimeValue(groupId, isXValueNull, binaryVal);
          }
          break;
        case BOOLEAN:
          boolean boolVal = false;
          if (!isXValueNull) {
            boolVal = BytesUtils.bytesToBool(bytes, offset);
          }
          if (!isOrderTimeNull) {
            updateBooleanFirstValue(groupId, isXValueNull, boolVal, curTime);
          } else {
            updateBooleanNullTimeValue(groupId, isXValueNull, boolVal);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of FIRST_BY should be BinaryColumn");

    if (inits.get(groupId) || initNullTimeValues.get(groupId)) {
      columnBuilder.writeBinary(new Binary(serializeTimeWithValue(groupId)));
      return;
    }
    columnBuilder.appendNull();
  }

  /** Serializes group state: Time (8B) | xNull (1B) | OrderTimeNull (1B) | [Value] */
  private byte[] serializeTimeWithValue(int groupId) {
    boolean xNull = xNulls.get(groupId);
    int length = Long.BYTES + 2 + (xNull ? 0 : calculateValueLength(groupId));
    byte[] bytes = new byte[length];
    boolean isOrderTimeNull = !inits.get(groupId);

    longToBytes(yFirstTimes.get(groupId), bytes, 0);
    boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
    boolToBytes(xNull, bytes, Long.BYTES + 1);

    if (!xNull) {
      int valueOffset = Long.BYTES + 2;
      switch (xDataType) {
        case INT32:
        case DATE:
          intToBytes(xIntValues.get(groupId), bytes, valueOffset);
          return bytes;
        case INT64:
        case TIMESTAMP:
          longToBytes(xLongValues.get(groupId), bytes, valueOffset);
          return bytes;
        case FLOAT:
          floatToBytes(xFloatValues.get(groupId), bytes, valueOffset);
          return bytes;
        case DOUBLE:
          doubleToBytes(xDoubleValues.get(groupId), bytes, valueOffset);
          return bytes;
        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          byte[] values = xBinaryValues.get(groupId).getValues();
          intToBytes(values.length, bytes, valueOffset);
          System.arraycopy(values, 0, bytes, length - values.length, values.length);
          return bytes;
        case BOOLEAN:
          boolToBytes(xBooleanValues.get(groupId), bytes, valueOffset);
          return bytes;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
      }
    }
    return bytes;
  }

  private int calculateValueLength(int groupId) {
    switch (xDataType) {
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
      case OBJECT:
      case STRING:
        return Integer.BYTES + xBinaryValues.get(groupId).getValues().length;
      case BOOLEAN:
        return 1;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
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
      case BLOB:
      case OBJECT:
      case STRING:
        columnBuilder.writeBinary(xBinaryValues.get(groupId));
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(xBooleanValues.get(groupId));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in FIRST_BY Aggregation: %s", xDataType));
    }
  }

  private boolean checkAndUpdateFirstTime(int groupId, boolean isXValueNull, long curTime) {
    if (!inits.get(groupId) || curTime < yFirstTimes.get(groupId)) {
      inits.set(groupId, true);
      yFirstTimes.set(groupId, curTime);

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
        updateIntFirstValue(
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

  protected void updateIntFirstValue(int groupId, boolean isXValueNull, int xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
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

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateLongFirstValue(
            groupId,
            xColumn.isNull(position),
            xColumn.getLong(position),
            timeColumn.getLong(position));
      } else {
        updateLongNullTimeValue(groupId, xColumn.isNull(position), xColumn.getLong(position));
      }
    }
  }

  protected void updateLongFirstValue(
      int groupId, boolean isXValueNull, long xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
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

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateFloatFirstValue(
            groupId,
            xColumn.isNull(position),
            xColumn.getFloat(position),
            timeColumn.getLong(position));
      } else {
        updateFloatNullTimeValue(groupId, xColumn.isNull(position), xColumn.getFloat(position));
      }
    }
  }

  protected void updateFloatFirstValue(
      int groupId, boolean isXValueNull, float xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
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

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateDoubleFirstValue(
            groupId,
            xColumn.isNull(position),
            xColumn.getDouble(position),
            timeColumn.getLong(position));
      } else {
        updateDoubleNullTimeValue(groupId, xColumn.isNull(position), xColumn.getDouble(position));
      }
    }
  }

  protected void updateDoubleFirstValue(
      int groupId, boolean isXValueNull, double xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
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

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateBinaryFirstValue(
            groupId,
            xColumn.isNull(position),
            xColumn.getBinary(position),
            timeColumn.getLong(position));
      } else {
        updateBinaryNullTimeValue(groupId, xColumn.isNull(position), xColumn.getBinary(position));
      }
    }
  }

  protected void updateBinaryFirstValue(
      int groupId, boolean isXValueNull, Binary xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
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

      int groupId = groupIds[position];
      if (!timeColumn.isNull(position)) {
        updateBooleanFirstValue(
            groupId,
            xColumn.isNull(position),
            xColumn.getBoolean(position),
            timeColumn.getLong(position));
      } else {
        updateBooleanNullTimeValue(groupId, xColumn.isNull(position), xColumn.getBoolean(position));
      }
    }
  }

  protected void updateBooleanFirstValue(
      int groupId, boolean isXValueNull, boolean xValue, long curTime) {
    if (checkAndUpdateFirstTime(groupId, isXValueNull, curTime)) {
      xBooleanValues.set(groupId, xValue);
    }
  }

  protected void updateBooleanNullTimeValue(int groupId, boolean isXValueNull, boolean xValue) {
    if (checkAndUpdateNullTime(groupId, isXValueNull)) {
      xBooleanValues.set(groupId, xValue);
    }
  }
}
