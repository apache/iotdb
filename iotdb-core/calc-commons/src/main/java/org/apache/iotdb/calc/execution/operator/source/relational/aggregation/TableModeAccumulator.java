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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.TypeServices;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class TableModeAccumulator implements TableAccumulator {

  private final int MAP_SIZE_THRESHOLD =
      CommonDescriptor.getInstance().getConfig().getModeMapSizeThreshold();
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableModeAccumulator.class);
  private final TSDataType seriesDataType;
  private final TypeServices.ModeValueService valueService;
  private final Map<Object, Long> countMap = new HashMap<>();

  private long nullCount;

  public TableModeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.valueService = TypeServices.MODE_VALUE_SERVICE.call(Type.fromTsDataType(seriesDataType));
    if (seriesDataType == TSDataType.OBJECT) {
      throw new UnsupportedOperationException(
          String.format(CalcMessages.UNSUPPORTED_DATA_TYPE_IN_MODE_AGGREGATION, seriesDataType));
    }
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new TableModeAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    Column column = arguments[0];
    int positionCount = mask.getSelectedPositionCount();
    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        addValue(column, i);
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      for (int i = 0; i < positionCount; i++) {
        addValue(column, selectedPositions[i]);
      }
    }
  }

  @Override
  public void removeInput(Column[] arguments) {
    Column column = arguments[0];
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (column.isNull(i)) {
        nullCount--;
      } else {
        Object key = valueService.getValue(column, i);
        countMap.computeIfPresent(key, (k, count) -> count - 1);
      }
    }
  }

  @Override
  public void addIntermediate(Column argument) {
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
      deserializeAndMergeCountMap(bytes);
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    checkArgument(
        columnBuilder instanceof BinaryColumnBuilder,
        "intermediate input and output should be BinaryColumn");

    columnBuilder.writeBinary(new Binary(serializeCountMap()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    if (countMap.isEmpty()) {
      columnBuilder.appendNull();
      return;
    }
    Map.Entry<Object, Long> maxEntry =
        countMap.entrySet().stream().max(Map.Entry.comparingByValue()).get();
    if (maxEntry.getValue() < nullCount) {
      columnBuilder.appendNull();
    } else {
      valueService.write(columnBuilder, maxEntry.getKey());
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void reset() {
    countMap.clear();
    nullCount = 0;
  }

  @Override
  public boolean removable() {
    return true;
  }

  // haveNull | nullCount (optional) | countMap
  private byte[] serializeCountMap() {
    int offset = 1 + (nullCount == 0 ? 0 : Long.BYTES);
    int valueSize =
        seriesDataType.isBinary()
            ? countMap.keySet().stream().mapToInt(valueService::calcTypeSize).sum()
            : countMap.size() * seriesDataType.getDataTypeSize();
    byte[] bytes = new byte[offset + Integer.BYTES + valueSize + Long.BYTES * countMap.size()];
    BytesUtils.boolToBytes(nullCount != 0, bytes, 0);
    if (nullCount != 0) {
      BytesUtils.longToBytes(nullCount, bytes, 1);
    }
    BytesUtils.intToBytes(countMap.size(), bytes, offset);
    offset += Integer.BYTES;
    for (Map.Entry<Object, Long> entry : countMap.entrySet()) {
      Object key = entry.getKey();
      offset += valueService.serialize(key, bytes, offset);
      BytesUtils.longToBytes(entry.getValue(), bytes, offset);
      offset += Long.BYTES;
    }
    return bytes;
  }

  private void deserializeAndMergeCountMap(byte[] bytes) {
    int offset = 0;
    if (BytesUtils.bytesToBool(bytes, 0)) {
      nullCount += BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, 1);
      offset += Long.BYTES;
    }
    offset++;
    int size = BytesUtils.bytesToInt(bytes, offset);
    offset += Integer.BYTES;
    for (int i = 0; i < size; i++) {
      Object key = valueService.deserialize(bytes, offset);
      offset += valueService.calcTypeSize(key);
      long count = BytesUtils.bytesToLongFromOffset(bytes, Long.BYTES, offset);
      offset += Long.BYTES;
      countMap.compute(key, (k, v) -> v == null ? count : v + count);
    }
  }

  private void addValue(Column column, int position) {
    if (column.isNull(position)) {
      nullCount++;
      return;
    }
    countMap.compute(
        valueService.getValue(column, position), (key, count) -> count == null ? 1L : count + 1);
    checkMapSize(countMap.size());
  }

  private void checkMapSize(int size) {
    if (size > MAP_SIZE_THRESHOLD) {
      throw new RuntimeException(
          String.format(
              "distinct values has exceeded the threshold %s when calculate Mode",
              MAP_SIZE_THRESHOLD));
    }
  }
}
