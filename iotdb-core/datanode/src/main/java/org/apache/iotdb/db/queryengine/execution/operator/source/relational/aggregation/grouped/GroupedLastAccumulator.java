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

public class GroupedLastAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedLastAccumulator.class);
  private final TSDataType seriesDataType;
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
    switch (seriesDataType) {
      case INT32:
      case DATE:
        intValues = new IntBigArray();
        return;
      case INT64:
      case TIMESTAMP:
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
      case OBJECT:
        binaryValues = new BinaryBigArray();
        return;
      case BOOLEAN:
        booleanValues = new BooleanBigArray();
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
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
      case OBJECT:
        valuesSize += binaryValues.sizeOf();
        break;
      case BOOLEAN:
        valuesSize += booleanValues.sizeOf();
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }

    return INSTANCE_SIZE
        + inits.sizeOf()
        + maxTimes.sizeOf()
        + initNullTimeValues.sizeOf()
        + valuesSize;
  }

  @Override
  public void setGroupCount(long groupCount) {
    maxTimes.ensureCapacity(groupCount);
    inits.ensureCapacity(groupCount);
    initNullTimeValues.ensureCapacity(groupCount);
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
      case OBJECT:
        binaryValues.ensureCapacity(groupCount);
        return;
      case BOOLEAN:
        booleanValues.ensureCapacity(groupCount);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    // arguments[0] is value column, arguments[1] is time column
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], arguments[1], mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], arguments[1], mask);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], arguments[1], mask);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], arguments[1], mask);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        addBinaryInput(groupIds, arguments[0], arguments[1], mask);
        return;
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], arguments[1], mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
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

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          if (!isOrderTimeNull) {
            updateIntValue(groupId, intVal, time);
          } else {
            updateIntNullTimeValue(groupId, intVal);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          if (!isOrderTimeNull) {
            updateLongValue(groupId, longVal, time);
          } else {
            updateLongNullTimeValue(groupId, longVal);
          }
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          if (!isOrderTimeNull) {
            updateFloatValue(groupId, floatVal, time);
          } else {
            updateFloatNullTimeValue(groupId, floatVal);
          }
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          if (!isOrderTimeNull) {
            updateDoubleValue(groupId, doubleVal, time);
          } else {
            updateDoubleNullTimeValue(groupId, doubleVal);
          }
          break;
        case TEXT:
        case BLOB:
        case OBJECT:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          if (!isOrderTimeNull) {
            updateBinaryValue(groupId, binaryVal, time);
          } else {
            updateBinaryNullTimeValue(groupId, binaryVal);
          }
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          if (!isOrderTimeNull) {
            updateBooleanValue(groupId, boolVal, time);
          } else {
            updateBooleanNullTimeValue(groupId, boolVal);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
      }
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
      case OBJECT:
      case STRING:
        columnBuilder.writeBinary(binaryValues.get(groupId));
        break;
      case BOOLEAN:
        columnBuilder.writeBoolean(booleanValues.get(groupId));
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    maxTimes.reset();
    inits.reset();
    initNullTimeValues.reset();
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
      case OBJECT:
        binaryValues.reset();
        return;
      case BOOLEAN:
        booleanValues.reset();
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
  }

  private byte[] serializeTimeWithValue(int groupId) {
    byte[] bytes;
    int length = Long.BYTES;
    boolean isOrderTimeNull = !inits.get(groupId);
    length += 1;

    switch (seriesDataType) {
      case INT32:
      case DATE:
        length += Integer.BYTES;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        intToBytes(intValues.get(groupId), bytes, Long.BYTES + 1);
        return bytes;
      case INT64:
      case TIMESTAMP:
        length += Long.BYTES;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        longToBytes(longValues.get(groupId), bytes, Long.BYTES + 1);
        return bytes;
      case FLOAT:
        length += Float.BYTES;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        floatToBytes(floatValues.get(groupId), bytes, Long.BYTES + 1);
        return bytes;
      case DOUBLE:
        length += Double.BYTES;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        doubleToBytes(doubleValues.get(groupId), bytes, Long.BYTES + 1);
        return bytes;
      case TEXT:
      case BLOB:
      case OBJECT:
      case STRING:
        byte[] values = binaryValues.get(groupId).getValues();
        length += Integer.BYTES + values.length;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        BytesUtils.intToBytes(values.length, bytes, Long.BYTES + 1);
        System.arraycopy(values, 0, bytes, length - values.length, values.length);
        return bytes;
      case BOOLEAN:
        length++;
        bytes = new byte[length];
        longToBytes(maxTimes.get(groupId), bytes, 0);
        boolToBytes(isOrderTimeNull, bytes, Long.BYTES);
        boolToBytes(booleanValues.get(groupId), bytes, Long.BYTES + 1);
        return bytes;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LAST Aggregation: %s", seriesDataType));
    }
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
}
