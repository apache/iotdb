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

public class GroupedFirstAccumulator implements GroupedAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedFirstAccumulator.class);
  private final TSDataType seriesDataType;
  private final LongBigArray minTimes = new LongBigArray(Long.MAX_VALUE);

  private LongBigArray longValues;
  private IntBigArray intValues;
  private FloatBigArray floatValues;
  private DoubleBigArray doubleValues;
  private BinaryBigArray binaryValues;
  private BooleanBigArray booleanValues;

  public GroupedFirstAccumulator(TSDataType seriesDataType) {
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
    // arguments[0] is value column, arguments[1] is time column
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0], arguments[1]);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0], arguments[1]);
        return;
      case FLOAT:
        addFloatInput(groupIds, arguments[0], arguments[1]);
        return;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0], arguments[1]);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(groupIds, arguments[0], arguments[1]);
        return;
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0], arguments[1]);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type : %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of First should be BinaryColumn");

    for (int i = 0; i < groupIds.length; i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      long time = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 0);
      int offset = Long.BYTES;

      switch (seriesDataType) {
        case INT32:
        case DATE:
          int intVal = BytesUtils.bytesToInt(bytes, offset);
          updateIntValue(groupIds[i], intVal, time);
          break;
        case INT64:
        case TIMESTAMP:
          long longVal = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          updateLongValue(groupIds[i], longVal, time);
          break;
        case FLOAT:
          float floatVal = BytesUtils.bytesToFloat(bytes, offset);
          updateFloatValue(groupIds[i], floatVal, time);
          break;
        case DOUBLE:
          double doubleVal = BytesUtils.bytesToDouble(bytes, offset);
          updateDoubleValue(groupIds[i], doubleVal, time);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          updateBinaryValue(groupIds[i], binaryVal, time);
          break;
        case BOOLEAN:
          boolean boolVal = BytesUtils.bytesToBool(bytes, offset);
          updateBooleanValue(groupIds[i], boolVal, time);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Unsupported data type: %s", seriesDataType));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of First should be BinaryColumn");
    if (minTimes.get(groupId) == Long.MIN_VALUE) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeBinary(new Binary(serializeTimeWithValue(groupId)));
    }
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    if (minTimes.get(groupId) == Long.MIN_VALUE) {
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
    minTimes.reset();
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

  private byte[] serializeTimeWithValue(int groupId) {
    byte[] bytes;
    int length = Long.BYTES;
    switch (seriesDataType) {
      case INT32:
      case DATE:
        length += Integer.BYTES;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        intToBytes(intValues.get(groupId), bytes, Long.BYTES);
        return bytes;
      case INT64:
      case TIMESTAMP:
        length += Long.BYTES;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        longToBytes(longValues.get(groupId), bytes, Long.BYTES);
        return bytes;
      case FLOAT:
        length += Float.BYTES;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        floatToBytes(floatValues.get(groupId), bytes, Long.BYTES);
        return bytes;
      case DOUBLE:
        length += Double.BYTES;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        doubleToBytes(doubleValues.get(groupId), bytes, Long.BYTES);
        return bytes;
      case TEXT:
      case BLOB:
      case STRING:
        byte[] values = binaryValues.get(groupId).getValues();
        length += Integer.BYTES + values.length;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        System.arraycopy(values, 0, bytes, length - values.length, values.length);
        return bytes;
      case BOOLEAN:
        length++;
        bytes = new byte[length];
        longToBytes(minTimes.get(groupId), bytes, 0);
        boolToBytes(booleanValues.get(groupId), bytes, Long.BYTES);
        return bytes;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type: %s", seriesDataType));
    }
  }

  private void addIntInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateIntValue(groupIds[i], valueColumn.getInt(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateIntValue(int groupId, int value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      intValues.set(groupId, value);
    }
  }

  private void addLongInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateLongValue(groupIds[i], valueColumn.getLong(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateLongValue(int groupId, long value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      longValues.set(groupId, value);
    }
  }

  private void addFloatInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateFloatValue(groupIds[i], valueColumn.getFloat(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateFloatValue(int groupId, float value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      floatValues.set(groupId, value);
    }
  }

  private void addDoubleInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateDoubleValue(groupIds[i], valueColumn.getDouble(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateDoubleValue(int groupId, double value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      doubleValues.set(groupId, value);
    }
  }

  private void addBinaryInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateBinaryValue(groupIds[i], valueColumn.getBinary(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateBinaryValue(int groupId, Binary value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      binaryValues.set(groupId, value);
    }
  }

  private void addBooleanInput(int[] groupIds, Column valueColumn, Column timeColumn) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!valueColumn.isNull(i)) {
        updateBooleanValue(groupIds[i], valueColumn.getBoolean(i), timeColumn.getLong(i));
      }
    }
  }

  protected void updateBooleanValue(int groupId, boolean value, long curTime) {
    long minTime = minTimes.get(groupId);
    if (curTime < minTime) {
      minTimes.set(groupId, curTime);
      booleanValues.set(groupId, value);
    }
  }
}
