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

package org.apache.iotdb.isession.subscription;

import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class SubscriptionSessionDataSet implements ISessionDataSet {

  private final EnrichedTablets enrichedTablets;

  public String getTopicName() {
    return enrichedTablets.getTopicName();
  }

  public String getSubscriptionCommitId() {
    return enrichedTablets.getSubscriptionCommitId();
  }

  private List<Tablet> getTablets() {
    return enrichedTablets.getTablets();
  }

  public SubscriptionSessionDataSet(EnrichedTablets enrichedTablets) {
    this.enrichedTablets = enrichedTablets;
    generateIterator();
  }

  /////////////////////////////// override ///////////////////////////////

  @Override
  public List<String> getColumnNames() {
    List<String> columnNameList = new ArrayList<>();
    columnNameList.add("Time");
    for (Tablet tablet : getTablets()) {
      String deviceId = tablet.deviceId;
      List<MeasurementSchema> schemas = tablet.getSchemas();
      columnNameList.addAll(
          schemas.stream()
              .map((schema) -> deviceId + "." + schema.getMeasurementId())
              .collect(Collectors.toList()));
    }
    return columnNameList;
  }

  @Override
  public List<String> getColumnTypes() {
    List<String> columnTypeList = new ArrayList<>();
    columnTypeList.add(TSDataType.INT64.toString());
    for (Tablet tablet : getTablets()) {
      List<MeasurementSchema> schemas = tablet.getSchemas();
      columnTypeList.addAll(
          schemas.stream()
              .map((schema) -> schema.getType().toString())
              .collect(Collectors.toList()));
    }
    return columnTypeList;
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public RowRecord next() {
    Map.Entry<Long, Map<Integer, Integer>> entry = this.iterator.next();
    final int totalColumnSize = getAllColumnSize();

    final List<Field> fields = new ArrayList<>();
    final long timestamp = entry.getKey();
    final Map<Integer, Integer> indexToRowIndex = entry.getValue();

    for (int currentIndex = 0; currentIndex < totalColumnSize; ++currentIndex) {
      final Field field;
      if (!indexToRowIndex.containsKey(currentIndex)) {
        field = new Field(null);
      } else {
        Pair<Integer, Integer> tabletAndColumnIndex = generateIndexMap().get(currentIndex);
        if (getTablets()
            .get(tabletAndColumnIndex.getLeft())
            .bitMaps[tabletAndColumnIndex.getRight()]
            .isMarked(indexToRowIndex.get(currentIndex))) {
          field = new Field(null);
        } else {
          TSDataType dataType = getAllDataTypes().get(currentIndex);
          field =
              generateFieldFromTabletValue(
                  dataType,
                  getTablets()
                      .get(tabletAndColumnIndex.getLeft())
                      .values[tabletAndColumnIndex.getRight()],
                  indexToRowIndex.get(currentIndex));
        }
      }
      fields.add(field);
    }
    return new RowRecord(timestamp, fields);
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  /////////////////////////////// utility ///////////////////////////////

  private List<MeasurementSchema> allSchemas;
  private List<TSDataType> allDataTypes;
  // current column index -> (tablet index, origin column index)
  private Map<Integer, Pair<Integer, Integer>> indexMap;
  private Iterator<Map.Entry<Long, Map<Integer, Integer>>> iterator;

  private int getAllColumnSize() {
    return getAllSchemas().size();
  }

  private List<MeasurementSchema> getAllSchemas() {
    if (Objects.nonNull(allSchemas)) {
      return allSchemas;
    }
    return allSchemas =
        getTablets().stream()
            .map(Tablet::getSchemas)
            .flatMap(List::stream)
            .collect(Collectors.toList());
  }

  private List<TSDataType> getAllDataTypes() {
    if (Objects.nonNull(allDataTypes)) {
      return allDataTypes;
    }
    return allDataTypes =
        getAllSchemas().stream().map(MeasurementSchema::getType).collect(Collectors.toList());
  }

  private void generateIterator() {
    // timestamp -> (current column index -> row index)
    TreeMap<Long, Map<Integer, Integer>> timestampIndexMap = new TreeMap<>();
    int currentIndex = 0;
    for (Tablet tablet : getTablets()) {
      final int columnSize = tablet.getSchemas().size();
      final int rowSize = tablet.timestamps.length;
      for (int rowIndex = 0; rowIndex < rowSize; ++rowIndex) {
        Long timestamp = tablet.timestamps[rowIndex];
        final Map<Integer, Integer> indexToTimestampIndex =
            timestampIndexMap.computeIfAbsent(timestamp, (t) -> new HashMap<>());
        for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
          indexToTimestampIndex.put(currentIndex + columnIndex, rowIndex);
        }
      }
      currentIndex += tablet.getSchemas().size();
    }
    this.iterator = timestampIndexMap.entrySet().iterator();
  }

  private Map<Integer, Pair<Integer, Integer>> generateIndexMap() {
    if (Objects.nonNull(indexMap)) {
      return indexMap;
    }
    indexMap = new HashMap<>();
    int currentIndex = 0;
    final int tabletSize = getTablets().size();
    for (int tabletIndex = 0; tabletIndex < tabletSize; ++tabletIndex) {
      final int columnSize = getTablets().get(tabletIndex).getSchemas().size();
      for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
        indexMap.put(currentIndex, new Pair<>(tabletIndex, columnIndex));
        currentIndex += 1;
      }
    }
    return indexMap;
  }

  private static Field generateFieldFromTabletValue(TSDataType dataType, Object value, int index) {
    final Field field = new Field(dataType);
    switch (dataType) {
      case BOOLEAN:
        boolean booleanValue = ((boolean[]) value)[index];
        field.setBoolV(booleanValue);
        break;
      case INT32:
        int intValue = ((int[]) value)[index];
        field.setIntV(intValue);
        break;
      case INT64:
        long longValue = ((long[]) value)[index];
        field.setLongV(longValue);
        break;
      case FLOAT:
        float floatValue = ((float[]) value)[index];
        field.setFloatV(floatValue);
        break;
      case DOUBLE:
        double doubleValue = ((double[]) value)[index];
        field.setDoubleV(doubleValue);
        break;
      case TEXT:
        Binary binaryValue = new Binary((((Binary[]) value)[index]).getValues());
        field.setBinaryV(binaryValue);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    return field;
  }
}
