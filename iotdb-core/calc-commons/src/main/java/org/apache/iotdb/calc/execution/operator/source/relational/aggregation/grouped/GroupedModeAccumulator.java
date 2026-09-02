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
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.array.MapBigArray;
import org.apache.iotdb.commons.conf.CommonDescriptor;

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
import org.apache.tsfile.utils.TsPrimitiveType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.utils.BytesUtils.bytesToBool;
import static org.apache.tsfile.utils.BytesUtils.bytesToLongFromOffset;

public class GroupedModeAccumulator implements GroupedAccumulator {

  private final int MAP_SIZE_THRESHOLD =
      CommonDescriptor.getInstance().getConfig().getModeMapSizeThreshold();
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedModeAccumulator.class);
  private final TSDataType seriesDataType;
  private final Type type;

  private final MapBigArray countMaps = new MapBigArray();

  private final LongBigArray nullCounts = new LongBigArray();

  public GroupedModeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.type = Type.fromTsDataType(seriesDataType);
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
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    addInput(groupIds, arguments[0], mask);
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    checkArgument(
        argument instanceof BinaryColumn
            || (argument instanceof RunLengthEncodedColumn
                && ((RunLengthEncodedColumn) argument).getValue() instanceof BinaryColumn),
        "intermediate input and output of MODE should be BinaryColumn");

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
        "intermediate input and output of MODE should be BinaryColumn");

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

    type.write(columnBuilder, maxEntry.getKey());
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    countMaps.reset();
    nullCounts.reset();
  }

  private void addInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        addValue(groupIds[i], column, i);
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        int position = selectedPositions[i];
        addValue(groupIds[position], column, position);
      }
    }
  }

  private void addValue(int groupId, Column column, int position) {
    HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupId);
    if (column.isNull(position)) {
      nullCounts.increment(groupId);
    } else {
      countMap.compute(
          column.getTsPrimitiveType(position), (key, count) -> count == null ? 1L : count + 1);
      checkMapSize(countMap.size());
    }
  }

  // haveNull | nullCount (optional) | countMap
  private byte[] serializeCountMap(int groupId) {
    int offset = 1 + (nullCounts.get(groupId) == 0 ? 0 : Long.BYTES);
    HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupId);
    int valueSize = countMap.keySet().stream().mapToInt(type::calcTypeSize).sum();
    byte[] bytes = new byte[offset + Integer.BYTES + valueSize + Long.BYTES * countMap.size()];
    BytesUtils.boolToBytes(nullCounts.get(groupId) != 0, bytes, 0);
    if (nullCounts.get(groupId) != 0) {
      BytesUtils.longToBytes(nullCounts.get(groupId), bytes, 1);
    }
    BytesUtils.intToBytes(countMap.size(), bytes, offset);
    offset += Integer.BYTES;
    for (Map.Entry<TsPrimitiveType, Long> entry : countMap.entrySet()) {
      TsPrimitiveType key = entry.getKey();
      int keySize = type.calcTypeSize(key);
      type.toBytes(key, bytes, offset);
      offset += keySize;
      BytesUtils.longToBytes(entry.getValue(), bytes, offset);
      offset += Long.BYTES;
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
    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, bytes.length - offset);
    for (int i = 0; i < size; i++) {
      TsPrimitiveType key = type.deserialize(buffer);
      long count = buffer.getLong();
      countMap.compute(key, (k, v) -> v == null ? count : v + count);
    }
  }

  private void addBooleanInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getBoolean(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getBoolean(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void addIntInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getInt(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getInt(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void addFloatInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getFloat(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getFloat(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void addLongInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getLong(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getLong(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void addDoubleInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getDouble(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getDouble(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void addBinaryInput(int[] groupIds, Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[i]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getBinary(i)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[i]);
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          HashMap<TsPrimitiveType, Long> countMap = countMaps.get(groupIds[position]);
          countMap.compute(
              Type.fromTsDataType(seriesDataType).getTsPrimitiveType(column.getBinary(position)),
              (k, v) -> v == null ? 1 : v + 1);
          checkMapSize(countMap.size());

        } else {
          nullCounts.increment(groupIds[position]);
        }
      }
    }
  }

  private void checkMapSize(int size) {
    if (size > MAP_SIZE_THRESHOLD) {
      throw new RuntimeException(
          String.format(
              "distinct values has exceeded the threshold %s when calculate MODE in one group",
              MAP_SIZE_THRESHOLD));
    }
  }
}
