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

import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * If in align by device query ,all queried devices are set in one template, we can store the common
 * variables in TemplatedInfo to avoid repeated creation.
 */
public class TemplatedInfo {
  private List<String> measurementList;
  private List<IMeasurementSchema> schemaList;
  private List<TSDataType> dataTypes;
  private Set<String> allSensors;
  private Filter timeFilter;
  private Ordering scanOrder;
  private boolean queryAllSensors;
  private long offsetValue;
  private long limitValue;

  public TemplatedInfo(
      List<String> measurementList,
      List<IMeasurementSchema> schemaList,
      List<TSDataType> dataTypes,
      Set<String> allSensors,
      Filter timeFilter,
      Ordering scanOrder,
      boolean queryAllSensors,
      long offsetValue,
      long limitValue) {
    this.measurementList = measurementList;
    this.schemaList = schemaList;
    this.dataTypes = dataTypes;
    this.allSensors = allSensors;
    this.timeFilter = timeFilter;
    this.scanOrder = scanOrder;
    this.queryAllSensors = queryAllSensors;
    this.offsetValue = offsetValue;
    this.limitValue = limitValue;
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

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public Filter getTimeFilter() {
    return this.timeFilter;
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

  public long getOffsetValue() {
    return this.offsetValue;
  }

  public void setLimitValue(long limitValue) {
    this.limitValue = limitValue;
  }

  public long getLimitValue() {
    return this.limitValue;
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
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(queryAllSensors, byteBuffer);
    ReadWriteIOUtils.write(offsetValue, byteBuffer);
    ReadWriteIOUtils.write(limitValue, byteBuffer);
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
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      timeFilter.serialize(stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    ReadWriteIOUtils.write(queryAllSensors, stream);
    ReadWriteIOUtils.write(offsetValue, stream);
    ReadWriteIOUtils.write(limitValue, stream);
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

    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter timeFilter = null;
    if (isNull == 1) {
      timeFilter = FilterFactory.deserialize(byteBuffer);
    }

    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    boolean queryAllSensors = ReadWriteIOUtils.readBool(byteBuffer);

    long offsetValue = ReadWriteIOUtils.readLong(byteBuffer);

    long limitValue = ReadWriteIOUtils.readLong(byteBuffer);

    return new TemplatedInfo(
        measurementList,
        measurementSchemaList,
        dataTypeList,
        allSensorSet,
        timeFilter,
        scanOrder,
        queryAllSensors,
        offsetValue,
        limitValue);
  }
}
