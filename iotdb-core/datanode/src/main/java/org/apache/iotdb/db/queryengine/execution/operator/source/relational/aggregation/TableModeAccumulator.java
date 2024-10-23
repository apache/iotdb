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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.UNSUPPORTED_TYPE_MESSAGE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeBinaryValue;

public class TableModeAccumulator implements TableAccumulator {

  private final int MAP_SIZE_THRESHOLD =
      IoTDBDescriptor.getInstance().getConfig().getModeMapSizeThreshold();
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableModeAccumulator.class);
  private final TSDataType seriesDataType;

  private Map<Boolean, Long> booleanCountMap;
  private Map<Integer, Long> intCountMap;
  private Map<Float, Long> floatCountMap;
  private Map<Long, Long> longCountMap;
  private Map<Double, Long> doubleCountMap;
  private Map<Binary, Long> binaryCountMap;

  public TableModeAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    switch (seriesDataType) {
      case BOOLEAN:
        booleanCountMap = new HashMap<>();
        break;
      case INT32:
      case DATE:
        intCountMap = new HashMap<>();
        break;
      case FLOAT:
        floatCountMap = new HashMap<>();
        break;
      case INT64:
      case TIMESTAMP:
        longCountMap = new HashMap<>();
        break;
      case DOUBLE:
        doubleCountMap = new HashMap<>();
        break;
      case TEXT:
      case STRING:
      case BLOB:
        binaryCountMap = new HashMap<>();
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
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
  public void addInput(Column[] arguments) {
    switch (seriesDataType) {
      case BOOLEAN:
        addBooleanInput(arguments[0]);
        break;
      case INT32:
      case DATE:
        addIntInput(arguments[0]);
        break;
      case FLOAT:
        addFloatInput(arguments[0]);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0]);
        break;
      case DOUBLE:
        addDoubleInput(arguments[0]);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments[0]);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
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
    columnBuilder.writeBinary(new Binary(serializeCountMap()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    switch (seriesDataType) {
      case BOOLEAN:
        if (booleanCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Boolean> maxKey =
              booleanCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeBoolean);
        }
        break;
      case INT32:
      case DATE:
        if (intCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Integer> maxKey =
              intCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeInt);
        }
        break;
      case FLOAT:
        if (floatCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Float> maxKey =
              floatCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeFloat);
        }
        break;
      case INT64:
      case TIMESTAMP:
        if (longCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Long> maxKey =
              longCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeLong);
        }
        break;
      case DOUBLE:
        if (doubleCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Double> maxKey =
              doubleCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeDouble);
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        if (binaryCountMap.isEmpty()) {
          columnBuilder.appendNull();
        } else {
          Optional<Binary> maxKey =
              binaryCountMap.entrySet().stream()
                  .max(Map.Entry.comparingByValue())
                  .map(Map.Entry::getKey);
          maxKey.ifPresent(columnBuilder::writeBinary);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
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
    if (booleanCountMap != null) {
      booleanCountMap.clear();
    }
    if (intCountMap != null) {
      intCountMap.clear();
    }
    if (floatCountMap != null) {
      floatCountMap.clear();
    }
    if (longCountMap != null) {
      longCountMap.clear();
    }
    if (doubleCountMap != null) {
      doubleCountMap.clear();
    }
    if (binaryCountMap != null) {
      binaryCountMap.clear();
    }
  }

  private byte[] serializeCountMap() {
    byte[] bytes;
    int offset = 0;

    switch (seriesDataType) {
      case BOOLEAN:
        bytes = new byte[4 + (1 + 8) * booleanCountMap.size()];
        BytesUtils.intToBytes(booleanCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Boolean, Long> entry : booleanCountMap.entrySet()) {
          BytesUtils.boolToBytes(entry.getKey(), bytes, offset);
          offset += 1;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      case INT32:
      case DATE:
        bytes = new byte[4 + (4 + 8) * intCountMap.size()];
        BytesUtils.intToBytes(intCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Integer, Long> entry : intCountMap.entrySet()) {
          BytesUtils.intToBytes(entry.getKey(), bytes, offset);
          offset += 4;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      case FLOAT:
        bytes = new byte[4 + (4 + 8) * floatCountMap.size()];
        BytesUtils.intToBytes(floatCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Float, Long> entry : floatCountMap.entrySet()) {
          BytesUtils.floatToBytes(entry.getKey(), bytes, offset);
          offset += 4;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      case INT64:
      case TIMESTAMP:
        bytes = new byte[4 + (8 + 8) * longCountMap.size()];
        BytesUtils.intToBytes(longCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Long, Long> entry : longCountMap.entrySet()) {
          BytesUtils.longToBytes(entry.getKey(), bytes, offset);
          offset += 8;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      case DOUBLE:
        bytes = new byte[4 + (8 + 8) * doubleCountMap.size()];
        BytesUtils.intToBytes(doubleCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Double, Long> entry : doubleCountMap.entrySet()) {
          BytesUtils.doubleToBytes(entry.getKey(), bytes, offset);
          offset += 8;
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        bytes =
            new byte
                [4
                    + (8 + 4) * binaryCountMap.size()
                    + binaryCountMap.keySet().stream()
                        .mapToInt(key -> key.getValues().length)
                        .sum()];
        BytesUtils.intToBytes(binaryCountMap.size(), bytes, offset);
        offset += 4;
        for (Map.Entry<Binary, Long> entry : binaryCountMap.entrySet()) {
          Binary binary = entry.getKey();
          serializeBinaryValue(binary, bytes, offset);
          offset += (4 + binary.getLength());
          BytesUtils.longToBytes(entry.getValue(), bytes, offset);
          offset += 8;
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }

    return bytes;
  }

  private void deserializeAndMergeCountMap(byte[] bytes) {
    int offset = 0;
    int size = BytesUtils.bytesToInt(bytes, offset);
    offset += 4;

    switch (seriesDataType) {
      case BOOLEAN:
        for (int i = 0; i < size; i++) {
          boolean key = BytesUtils.bytesToBool(bytes, offset);
          offset += 1;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          booleanCountMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case INT32:
      case DATE:
        for (int i = 0; i < size; i++) {
          int key = BytesUtils.bytesToInt(bytes, offset);
          offset += 4;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          intCountMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case FLOAT:
        for (int i = 0; i < size; i++) {
          float key = BytesUtils.bytesToFloat(bytes, offset);
          offset += 4;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          floatCountMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case INT64:
      case TIMESTAMP:
        for (int i = 0; i < size; i++) {
          long key = BytesUtils.bytesToLong(bytes, offset);
          offset += 8;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          longCountMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < size; i++) {
          double key = BytesUtils.bytesToDouble(bytes, offset);
          offset += 8;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          doubleCountMap.compute(key, (k, v) -> v == null ? count : v + count);
        }
        break;
      case TEXT:
      case STRING:
      case BLOB:
        for (int i = 0; i < size; i++) {
          int length = BytesUtils.bytesToInt(bytes, offset);
          offset += 4;
          Binary binaryVal = new Binary(BytesUtils.subBytes(bytes, offset, length));
          offset += length;
          long count = BytesUtils.bytesToLongFromOffset(bytes, 8, offset);
          offset += 8;
          binaryCountMap.compute(binaryVal, (k, v) -> v == null ? count : v + count);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(UNSUPPORTED_TYPE_MESSAGE, seriesDataType));
    }
  }

  private void addBooleanInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        booleanCountMap.compute(column.getBoolean(i), (k, v) -> v == null ? 1 : v + 1);
        if (booleanCountMap.size() > MAP_SIZE_THRESHOLD) {
          checkMapSize(booleanCountMap.size());
        }
      }
    }
  }

  private void addIntInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        intCountMap.compute(column.getInt(i), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(intCountMap.size());
      }
    }
  }

  private void addFloatInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        floatCountMap.compute(column.getFloat(i), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(floatCountMap.size());
      }
    }
  }

  private void addLongInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        longCountMap.compute(column.getLong(i), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(longCountMap.size());
      }
    }
  }

  private void addDoubleInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        doubleCountMap.compute(column.getDouble(i), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(doubleCountMap.size());
      }
    }
  }

  private void addBinaryInput(Column column) {
    for (int i = 0; i < column.getPositionCount(); i++) {
      if (!column.isNull(i)) {
        binaryCountMap.compute(column.getBinary(i), (k, v) -> v == null ? 1 : v + 1);
        checkMapSize(binaryCountMap.size());
      }
    }
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
