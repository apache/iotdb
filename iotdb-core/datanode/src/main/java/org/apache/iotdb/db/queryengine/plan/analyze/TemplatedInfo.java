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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;

/**
 * If in align by device query ,all queried devices are set in one template, we can store the common
 * variables in TemplatedInfo to avoid repeated creation.
 */
public class TemplatedInfo {
  private List<String> measurementList;
  private List<IMeasurementSchema> schemaList;
  private List<TSDataType> dataTypes;
  private Set<String> allSensors;
  private Ordering scanOrder;
  private boolean queryAllSensors;
  private List<String> selectMeasurements;
  private List<Integer> deviceToMeasurementIndexes;
  private final long offsetValue;
  private long limitValue;
  // these variables below are use in value filter condition
  private final Expression predicate;
  private ZoneId zoneId;
  private boolean keepNull;
  // not serialize
  private Map<String, IMeasurementSchema> schemaMap;
  // not serialize
  private Map<String, List<InputLocation>> layoutMap;

  public TemplatedInfo(
      List<String> measurementList,
      List<IMeasurementSchema> schemaList,
      List<TSDataType> dataTypes,
      Set<String> allSensors,
      Ordering scanOrder,
      boolean queryAllSensors,
      List<String> selectMeasurements,
      List<Integer> deviceToMeasurementIndexes,
      long offsetValue,
      long limitValue,
      Expression predicate,
      ZoneId zoneId,
      Map<String, IMeasurementSchema> schemaMap,
      Map<String, List<InputLocation>> layoutMap) {
    this.measurementList = measurementList;
    this.schemaList = schemaList;
    this.dataTypes = dataTypes;
    this.allSensors = allSensors;
    this.scanOrder = scanOrder;
    this.queryAllSensors = queryAllSensors;
    this.selectMeasurements = selectMeasurements;
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
    this.offsetValue = offsetValue;
    this.limitValue = limitValue;
    this.predicate = predicate;
    if (predicate != null) {
      this.zoneId = zoneId;
      this.schemaMap = schemaMap;
      this.layoutMap = layoutMap;
    }
  }

  public void setMeasurementList(List<String> measurementList) {
    this.measurementList = measurementList;
  }

  public List<String> getMeasurementList() {
    return this.measurementList;
  }

  public void setSchemaList(List<IMeasurementSchema> schemaList) {
    this.schemaList = schemaList;
  }

  public List<IMeasurementSchema> getSchemaList() {
    return this.schemaList;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<TSDataType> getDataTypes() {
    return this.dataTypes;
  }

  public void setAllSensors(Set<String> allSensors) {
    this.allSensors = allSensors;
  }

  public Set<String> getAllSensors() {
    return this.allSensors;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public Ordering getScanOrder() {
    return this.scanOrder;
  }

  public void setQueryAllSensors(boolean queryAllSensors) {
    this.queryAllSensors = queryAllSensors;
  }

  public boolean isQueryAllSensors() {
    return this.queryAllSensors;
  }

  public void setSelectMeasurements(List<String> selectMeasurements) {
    this.selectMeasurements = selectMeasurements;
  }

  public List<String> getSelectMeasurements() {
    return this.selectMeasurements;
  }

  public void setDeviceToMeasurementIndexes(List<Integer> deviceToMeasurementIndexes) {
    this.deviceToMeasurementIndexes = deviceToMeasurementIndexes;
  }

  public long getOffsetValue() {
    return this.offsetValue;
  }

  public void setLimitValue(long limitValue) {
    this.limitValue = limitValue;
  }

  public long getLimitValue() {
    return this.limitValue;
  }

  public List<Integer> getDeviceToMeasurementIndexes() {
    return this.deviceToMeasurementIndexes;
  }

  public Expression getPredicate() {
    return this.predicate;
  }

  public ZoneId getZoneId() {
    return this.zoneId;
  }

  public boolean isKeepNull() {
    return this.keepNull;
  }

  public Map<String, IMeasurementSchema> getSchemaMap() {
    return this.schemaMap;
  }

  public Map<String, List<InputLocation>> getLayoutMap() {
    return this.layoutMap;
  }

  public static Map<String, List<InputLocation>> makeLayout(List<String> measurementList) {
    Map<String, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    outputMappings
        .computeIfAbsent(TIMESTAMP_EXPRESSION_STRING, key -> new ArrayList<>())
        .add(new InputLocation(tsBlockIndex, -1));
    int valueColumnIndex = 0;
    for (String columnName : measurementList) {
      outputMappings
          .computeIfAbsent(columnName, key -> new ArrayList<>())
          .add(new InputLocation(tsBlockIndex, valueColumnIndex));
      valueColumnIndex++;
    }
    return outputMappings;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(measurementList.size(), byteBuffer);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }
    for (IMeasurementSchema schema : schemaList) {
      schema.serializeTo(byteBuffer);
    }
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType, byteBuffer);
    }
    ReadWriteIOUtils.write(allSensors.size(), byteBuffer);
    for (String dataType : allSensors) {
      ReadWriteIOUtils.write(dataType, byteBuffer);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(queryAllSensors, byteBuffer);

    ReadWriteIOUtils.write(selectMeasurements.size(), byteBuffer);
    for (String selectMeasurement : selectMeasurements) {
      ReadWriteIOUtils.write(selectMeasurement, byteBuffer);
    }

    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), byteBuffer);
    for (int index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }

    ReadWriteIOUtils.write(offsetValue, byteBuffer);
    ReadWriteIOUtils.write(limitValue, byteBuffer);

    if (predicate != null) {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(predicate, byteBuffer);
      ReadWriteIOUtils.write(zoneId.getId(), byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(measurementList.size(), stream);
    for (String measurement : measurementList) {
      ReadWriteIOUtils.write(measurement, stream);
    }
    for (IMeasurementSchema schema : schemaList) {
      schema.serializeTo(stream);
    }
    for (TSDataType dataType : dataTypes) {
      ReadWriteIOUtils.write(dataType, stream);
    }
    ReadWriteIOUtils.write(allSensors.size(), stream);
    for (String dataType : allSensors) {
      ReadWriteIOUtils.write(dataType, stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    ReadWriteIOUtils.write(queryAllSensors, stream);

    ReadWriteIOUtils.write(selectMeasurements.size(), stream);
    for (String selectMeasurement : selectMeasurements) {
      ReadWriteIOUtils.write(selectMeasurement, stream);
    }

    ReadWriteIOUtils.write(deviceToMeasurementIndexes.size(), stream);
    for (int index : deviceToMeasurementIndexes) {
      ReadWriteIOUtils.write(index, stream);
    }

    ReadWriteIOUtils.write(offsetValue, stream);
    ReadWriteIOUtils.write(limitValue, stream);

    if (predicate != null) {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(predicate, stream);
      ReadWriteIOUtils.write(zoneId.getId(), stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
  }

  public static TemplatedInfo deserialize(ByteBuffer byteBuffer) {

    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> measurementList = new ArrayList<>();
    int cnt = measurementSize;
    while (cnt-- > 0) {
      measurementList.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    cnt = measurementSize;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    while (cnt-- > 0) {
      // MeasurementSchema is ok?
      measurementSchemaList.add(MeasurementSchema.deserializeFrom(byteBuffer));
    }

    cnt = measurementSize;
    List<TSDataType> dataTypeList = new ArrayList<>();
    while (cnt-- > 0) {
      dataTypeList.add(ReadWriteIOUtils.readDataType(byteBuffer));
    }

    int allSensorSize = ReadWriteIOUtils.readInt(byteBuffer);
    Set<String> allSensorSet = new HashSet<>();
    while (allSensorSize-- > 0) {
      allSensorSet.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    boolean queryAllSensors = ReadWriteIOUtils.readBool(byteBuffer);

    int listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> selectMeasurements = new ArrayList<>(listSize);
    while (listSize-- > 0) {
      selectMeasurements.add(ReadWriteIOUtils.readString(byteBuffer));
    }

    listSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> deviceToMeasurementIndexes = new ArrayList<>(listSize);
    while (listSize-- > 0) {
      deviceToMeasurementIndexes.add(ReadWriteIOUtils.readInt(byteBuffer));
    }

    long offsetValue = ReadWriteIOUtils.readLong(byteBuffer);

    long limitValue = ReadWriteIOUtils.readLong(byteBuffer);

    Expression predicate = null;
    ZoneId zone = null;
    byte hasFilter = ReadWriteIOUtils.readByte(byteBuffer);
    Map<String, IMeasurementSchema> currentSchemaMap = null;
    Map<String, List<InputLocation>> layoutMap = null;
    if (hasFilter == 1) {
      predicate = Expression.deserialize(byteBuffer);
      zone = ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(byteBuffer)));
      currentSchemaMap = new HashMap<>();
      for (IMeasurementSchema measurementSchema : measurementSchemaList) {
        currentSchemaMap.put(measurementSchema.getMeasurementId(), measurementSchema);
      }
      layoutMap = makeLayout(measurementList);
    }

    return new TemplatedInfo(
        measurementList,
        measurementSchemaList,
        dataTypeList,
        allSensorSet,
        scanOrder,
        queryAllSensors,
        selectMeasurements,
        deviceToMeasurementIndexes,
        offsetValue,
        limitValue,
        predicate,
        zone,
        currentSchemaMap,
        layoutMap);
  }
}
