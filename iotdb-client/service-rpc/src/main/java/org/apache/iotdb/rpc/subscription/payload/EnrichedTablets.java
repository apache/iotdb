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

package org.apache.iotdb.rpc.subscription.payload;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class EnrichedTablets {

  private transient String topicName;
  private transient String subscriptionCommitId;
  private transient List<Tablet> tablets;

  public String getTopicName() {
    return topicName;
  }

  public List<Tablet> getTablets() {
    return tablets;
  }

  public String getSubscriptionCommitId() {
    return subscriptionCommitId;
  }

  public EnrichedTablets() {
    this.tablets = new ArrayList<>();
  }

  public EnrichedTablets(String topicName, List<Tablet> tablets, String subscriptionCommitId) {
    this.topicName = topicName;
    this.tablets = tablets;
    this.subscriptionCommitId = subscriptionCommitId;
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(topicName, stream);
    ReadWriteIOUtils.write(subscriptionCommitId, stream);
    ReadWriteIOUtils.write(tablets.size(), stream);
    for (Tablet tablet : tablets) {
      tablet.serialize(stream);
    }
  }

  public static EnrichedTablets deserialize(ByteBuffer buffer) {
    final EnrichedTablets enrichedTablets = new EnrichedTablets();
    enrichedTablets.topicName = ReadWriteIOUtils.readString(buffer);
    enrichedTablets.subscriptionCommitId = ReadWriteIOUtils.readString(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; ++i) {
      enrichedTablets.tablets.add(Tablet.deserialize(buffer));
    }
    return enrichedTablets;
  }

  /////////////////////////////// convert to EnrichedRowRecord ///////////////////////////////

  private List<MeasurementSchema> allSchemas;
  private List<TSDataType> allDataTypes;
  private Map<Integer, Pair<Integer, Integer>> indexMap;
  private TreeMap<Long, Map<Integer, Integer>> timestampIndexMap;

  private int getAllColumnSize() {
    return getAllSchemas().size();
  }

  private List<MeasurementSchema> getAllSchemas() {
    if (Objects.nonNull(allSchemas)) {
      return allSchemas;
    }
    return allSchemas =
        tablets.stream().map(Tablet::getSchemas).flatMap(List::stream).collect(Collectors.toList());
  }

  private List<TSDataType> getAllDataTypes() {
    if (Objects.nonNull(allDataTypes)) {
      return allDataTypes;
    }
    return allDataTypes =
        getAllSchemas().stream().map(MeasurementSchema::getType).collect(Collectors.toList());
  }

  private Map<Integer, Pair<Integer, Integer>> generateIndexMap() {
    if (Objects.nonNull(indexMap)) {
      return indexMap;
    }
    indexMap = new HashMap<>();
    int currentIndex = 0;
    final int tabletSize = tablets.size();
    for (int tabletIndex = 0; tabletIndex < tabletSize; ++tabletIndex) {
      final int columnSize = tablets.get(tabletIndex).getSchemas().size();
      for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
        indexMap.put(currentIndex, new Pair<>(tabletIndex, columnIndex));
        currentIndex += 1;
      }
    }
    return indexMap;
  }

  private TreeMap<Long, Map<Integer, Integer>> generateTimestampIndexMap() {
    if (Objects.nonNull(timestampIndexMap)) {
      return timestampIndexMap;
    }
    timestampIndexMap = new TreeMap<>();
    int currentIndex = 0;
    for (Tablet tablet : tablets) {
      final int columnSize = tablet.getSchemas().size();
      final int timestampSize = tablet.timestamps.length;
      for (int timestampIndex = 0; timestampIndex < timestampSize; ++timestampIndex) {
        Long timestamp = tablet.timestamps[timestampIndex];
        final Map<Integer, Integer> indexToTimestampIndex =
            timestampIndexMap.computeIfAbsent(timestamp, (t) -> new HashMap<>());
        for (int columnIndex = 0; columnIndex < columnSize; ++columnIndex) {
          indexToTimestampIndex.put(currentIndex + columnIndex, timestampIndex);
        }
      }
      currentIndex += tablet.getSchemas().size();
    }
    return timestampIndexMap;
  }

  public List<String> generateColumnNameList() {
    List<String> columnNameList = new ArrayList<>();
    columnNameList.add("Time");
    for (Tablet tablet : tablets) {
      String deviceId = tablet.deviceId;
      List<MeasurementSchema> schemas = tablet.getSchemas();
      columnNameList.addAll(
          schemas.stream()
              .map((schema) -> deviceId + "." + schema.getMeasurementId())
              .collect(Collectors.toList()));
    }
    return columnNameList;
  }

  public List<String> generateColumnTypeList() {
    List<String> columnTypeList = new ArrayList<>();
    columnTypeList.add(TSDataType.INT64.toString());
    for (Tablet tablet : tablets) {
      List<MeasurementSchema> schemas = tablet.getSchemas();
      columnTypeList.addAll(
          schemas.stream()
              .map((schema) -> schema.getType().toString())
              .collect(Collectors.toList()));
    }
    return columnTypeList;
  }

  public List<RowRecord> generateRecords() {
    final List<RowRecord> records = new ArrayList<>();

    final int totalColumnSize = getAllColumnSize();
    TreeMap<Long, Map<Integer, Integer>> timestampIndexMap = generateTimestampIndexMap();
    for (Map.Entry<Long, Map<Integer, Integer>> entry : timestampIndexMap.entrySet()) {
      final List<Field> fields = new ArrayList<>();
      final long timestamp = entry.getKey();
      final Map<Integer, Integer> timestampIndex = entry.getValue();

      for (int currentIndex = 0; currentIndex < totalColumnSize; ++currentIndex) {
        final Field field;
        if (!timestampIndex.containsKey(currentIndex)) {
          field = new Field(null);
        } else {
          Pair<Integer, Integer> tabletAndColumnIndex = generateIndexMap().get(currentIndex);
          if (tablets
              .get(tabletAndColumnIndex.getLeft())
              .bitMaps[tabletAndColumnIndex.getRight()]
              .isMarked(timestampIndex.get(currentIndex))) {
            field = new Field(null);
          } else {
            TSDataType dataType = getAllDataTypes().get(currentIndex);
            field =
                generateFieldFromTabletValue(
                    dataType,
                    tablets.get(tabletAndColumnIndex.getLeft())
                        .values[tabletAndColumnIndex.getRight()],
                    timestampIndex.get(currentIndex));
          }
        }
        fields.add(field);
      }
      records.add(new RowRecord(timestamp, fields));
    }
    return records;
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
        Binary binary = new Binary((((Binary[]) value)[index]).getValues());
        field.setBinaryV(binary);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    return field;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    EnrichedTablets that = (EnrichedTablets) obj;
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.tablets, that.tablets)
        && Objects.equals(this.subscriptionCommitId, that.subscriptionCommitId);
  }

  @Override
  public int hashCode() {
    // Considering that the Tablet class has not implemented the hashCode method, the tablets member
    // should not be included when calculating the hashCode of EnrichedTablets.
    return Objects.hash(topicName, subscriptionCommitId);
  }
}
