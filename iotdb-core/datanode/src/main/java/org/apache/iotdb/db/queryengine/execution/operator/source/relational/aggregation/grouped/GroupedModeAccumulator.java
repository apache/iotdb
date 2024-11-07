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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.MapBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.UNSUPPORTED_TYPE_MESSAGE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeBinaryValue;
import static org.apache.tsfile.utils.BytesUtils.bytesToBool;
import static org.apache.tsfile.utils.BytesUtils.bytesToLongFromOffset;
import static org.apache.tsfile.utils.TsPrimitiveType.getByType;

public class GroupedModeAccumulator implements GroupedAccumulator {

  private final int MAP_SIZE_THRESHOLD =
      IoTDBDescriptor.getInstance().getConfig().getModeMapSizeThreshold();
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedModeAccumulator.class);
  private final TSDataType seriesDataType;

  private final MapBigArray countMaps = new MapBigArray();

  private final LongBigArray nullCounts = new LongBigArray();

  public GroupedModeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + countMaps.sizeOf() + nullCounts.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    countMaps.ensureCapacity(groupCount);
    nullCounts.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    switch (seriesDataType) {
      case BOOLEAN:
        addBooleanInput(groupIds, arguments[0]);
        break;
      case INT32:
      case DATE:
        addIntInput(groupIds, arguments[0]);
        break;
      case FLOAT:
        addFloatInput(groupIds, arguments[0]);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments[0]);
        break;
      case DOUBLE:
        addDoubleInput(groupIds, arguments[0]);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(groupIds, arguments[0]);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of Mode should be BinaryColumn");

    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (argument.isNull(i)) {
        continue;
      }

      byte[] bytes = argument.getBinary(i).getValues();
      deserializeAndMergeCountMap(groupIds[i], bytes);
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output of Mode should be BinaryColumn");

    columnBuilder.writeBinary(new Binary(serializeCountMap(groupId)));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupId);
    if (countMap.isEmpty()) {
      columnBuilder.appendNull();
      return;
    }
    // must be present
    Map.Entry<TsPrimitiveType, Long> maxEntry =
        countMap.entrySet().stream().max(Map.Entry.comparingByValue()).get();
    if (maxEntry.getValue() < nullCounts.get(groupId)) {
      columnBuilder.appendNull();
      return;
    }

    switch (seriesDataType) {
      case BOOLEAN:
        columnBuilder.writeBoolean(maxEntry.getKey().getBoolean());
        break;
      case INT32:
      case DATE:
        columnBuilder.writeInt(maxEntry.getKey().getInt());
        break;
      case FLOAT:
        columnBuilder.writeFloat(maxEntry.getKey().getFloat());
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong(maxEntry.getKey().getLong());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(maxEntry.getKey().getDouble());
        break;
      case TEXT:
      case STRING:
      case BLOB:
        columnBuilder.writeBinary(maxEntry.getKey().getBinary());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    countMaps.reset();
    nullCounts.reset();
  }

  // haveNull | nullCount (optional) | countMap
  private byte[] serializeCountMap(int groupId) {
    byte[] bytes;
    int offset = 1 + (nullCounts.get(groupId) == 0 ? 0 : Long.BYTES);
    HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupId);

    switch (seriesDataType) {
      case BOOLEAN:
        bytes = new byte[offset + Integer.BYTES + (1 + Long.BYTES) * countMap.size()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          BytesUtils.boolToBytes(entry.getKey().getBoolean(), bytes, offset);
          offset += 1;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      case INT32:
      case DATE:
        bytes = new byte[offset + Integer.BYTES + (Integer.BYTES + Long.BYTES) * countMap.size()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += Integer.BYTES;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          BytesUtils.intToBytes(entry.getKey().getInt(), bytes, offset);
          offset += Integer.BYTES;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      case FLOAT:
        bytes = new byte[offset + Integer.BYTES + (Float.BYTES + Long.BYTES) * countMap.size()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += Integer.BYTES;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          BytesUtils.floatToBytes(entry.getKey().getFloat(), bytes, offset);
          offset += Float.BYTES;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      case INT64:
      case TIMESTAMP:
        bytes = new byte[offset + Integer.BYTES + (Long.BYTES + Long.BYTES) * countMap.size()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += Integer.BYTES;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          BytesUtils.longToBytes(entry.getKey().getLong(), bytes, offset);
          offset += Long.BYTES;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      case DOUBLE:
        bytes = new byte[offset + Integer.BYTES + (Double.BYTES + Long.BYTES) * countMap.size()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += Integer.BYTES;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          BytesUtils.doubleToBytes(entry.getKey().getDouble(), bytes, offset);
          offset += Double.BYTES;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        bytes =
            new byte
                [offset
                    + Integer.BYTES
                    + (Integer.BYTES + Long.BYTES) * countMap.size()
                    + countMap.keySet().stream()
                        .mapToInt(key -> key.getBinary().getValues().length)
                        .sum()];
        BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
        if (nullCounts.get(groupId) != 0) {
          BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
        }
        BytesUtils.intToBytes(countMap.size(), bytes, offset);
        offset += Integer.BYTES;
        for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
          Binary binary = entry.getKey().getBinary();
          serializeBinaryValue(binary, bytes, offset);
          offset += (Integer.BYTES + binary.getLength());
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += Long.BYTES;
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }

    return bytes;
  }

  private void deserializeAndMergeCountMap(int groupId, byte[] bytes) {
    int offset = 0;
    if (bytesToBool(bytes, 0)) {
      nullCounts.add(groupId, bytesToLongFromOffset(bytes, Long.BYTES, 1));
      offset += Long.BYTES;
    }
    offset++;
    int size = BytesUtils.bytesToInt(bytes, offset);
    offset += Integer.BYTES;

    HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupId);

    switch (seriesDataType) {
      case BOOLEAN:
        for (int i = 0; i < size; i++) {
          TsPrimitiveType key = new TsPrimitiveType.TsBoolean(bytesToBool(bytes, offset));
          offset += 1;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case INT32:
      case DATE:
        for (int i = 0; i < size; i++) {
          TsPrimitiveType key = new TsPrimitiveType.TsInt(BytesUtils.bytesToInt(bytes, offset));
          offset += Integer.BYTES;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case FLOAT:
        for (int i = 0; i < size; i++) {
          TsPrimitiveType key = new TsPrimitiveType.TsFloat(BytesUtils.bytesToFloat(bytes, offset));
          offset += Float.BYTES;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case INT64:
      case TIMESTAMP:
        for (int i = 0; i < size; i++) {
          TsPrimitiveType key =
              new TsPrimitiveType.TsLong(
                  BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset));
          offset += Long.BYTES;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < size; i++) {
          TsPrimitiveType key =
              new TsPrimitiveType.TsDouble(BytesUtils.bytesToDouble(bytes, offset));
          offset += Double.BYTES;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        for (int i = 0; i < size; i++) {
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += Integer.BYTES;
          TsPrimitiveType key =
              new TsPrimitiveType.TsBinary(new Binary(BytesUtils.subBytes(bytes, offset, length)));
          offset += length;
          long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
          offset += Long.BYTES;
          countMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }
  }

  private void addBooleanInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getBoolean(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());

      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void addIntInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getInt(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());
      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void addFloatInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getFloat(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());
      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void addLongInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getLong(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());
      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void addDoubleInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getDouble(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());
      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void addBinaryInput(int[] groupIds, Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
        countMap.compute(
            getByType(seriesDataType, column.getBinary(i)), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(countMap.size());
      } else {
        nullCounts.increment(groupIds[i]);
      }
    }
  }

  private void checkMapSize(int size) {
    if (size > MAP_SIZE_THRESHOLD) {
      throw new RuntimeException(
          String.format(
              "distinct values has exceeded the threshold %s when calculate Mode in one group",
              MAP_SIZE_THRESHOLD));
    }
  }
}
