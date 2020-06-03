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
package org.apache.iotdb.db.qp.physical.crud;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertPlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(InsertPlan.class);

  private long time;
  private String deviceId;
  private String[] measurements;
  private Object[] values;
  private TSDataType[] types;
  private MeasurementSchema[] schemas;

  // if inferType is false, use the type of values directly
  // if inferType is true, values is String[], and infer types from them
  private boolean inferType = false;

  // record the failed measurements
  private List<String> failedMeasurements;

  public InsertPlan() {
    super(false, OperatorType.INSERT);
    canbeSplit = false;
  }

  @TestOnly
  public InsertPlan(String deviceId, long insertTime, String[] measurements, TSDataType[] types,
      String[] insertValues) {
    super(false, OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurements;

    this.types = types;
    this.values = new Object[measurements.length];
    for (int i = 0; i < measurements.length; i++) {
      try {
        values[i] = CommonUtils.parseValueForTest(types[i], insertValues[i]);
      } catch (QueryProcessException e) {
        e.printStackTrace();
      }
    }
    canbeSplit = false;
  }

  @TestOnly
  public InsertPlan(String deviceId, long insertTime, String measurement, TSDataType type, String insertValue) {
    super(false, OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = new String[]{measurement};
    this.types = new TSDataType[]{type};
    this.values = new Object[1];
    try {
      values[0] = CommonUtils.parseValueForTest(types[0], insertValue);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }
    canbeSplit = false;
  }

  public InsertPlan(TSRecord tsRecord) {
    super(false, OperatorType.INSERT);
    this.deviceId = tsRecord.deviceId;
    this.time = tsRecord.time;
    this.measurements = new String[tsRecord.dataPointList.size()];
    this.schemas = new MeasurementSchema[tsRecord.dataPointList.size()];
    this.types = new TSDataType[tsRecord.dataPointList.size()];
    this.values = new Object[tsRecord.dataPointList.size()];
    for (int i = 0; i < tsRecord.dataPointList.size(); i++) {
      measurements[i] = tsRecord.dataPointList.get(i).getMeasurementId();
      schemas[i] = new MeasurementSchema(measurements[i], tsRecord.dataPointList.get(i).getType(),
          TSEncoding.PLAIN);
      types[i] = tsRecord.dataPointList.get(i).getType();
      values[i] = tsRecord.dataPointList.get(i).getValue();
    }
    canbeSplit = false;
  }

  public InsertPlan(String deviceId, long insertTime, String[] measurementList, TSDataType[] types,
      Object[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurementList;
    this.types = types;
    this.values = insertValues;
    canbeSplit = false;
  }

  public InsertPlan(String deviceId, long insertTime, String[] measurementList,
      String[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurementList;
    // build types and values
    this.types = new TSDataType[measurements.length];
    this.values = new Object[measurements.length];
    System.arraycopy(insertValues, 0, values, 0, measurements.length);
    inferType = true;
    canbeSplit = false;
  }


  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public boolean isInferType() {
    return inferType;
  }

  public void setInferType(boolean inferType) {
    this.inferType = inferType;
  }

  public MeasurementSchema[] getSchemas() {
    return schemas;
  }

  /**
   * if inferType is true,
   * transfer String[] values to specific data types (Integer, Long, Float, Double, Binary)
   */
  public void setSchemasAndTransferType(MeasurementSchema[] schemas) throws QueryProcessException {
    this.schemas = schemas;
    if (inferType) {
      for (int i = 0; i < schemas.length; i++) {
        if (schemas[i] == null) {
          continue;
        }
        types[i] = schemas[i].getType();
        try {
          values[i] = CommonUtils.parseValue(types[i], values[i].toString());
        } catch (Exception e) {
          logger.warn("{}.{} data type is not consistent, input {}, registered {}", deviceId,
              measurements[i], values[i], types[i]);
          if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
            markMeasurementInsertionFailed(i);
          } else {
            throw e;
          }
        }
      }
    }
  }

  /**
   * @param index failed measurement index
   */
  public void markMeasurementInsertionFailed(int index) {
    if (failedMeasurements == null) {
      failedMeasurements = new ArrayList<>();
    }
    failedMeasurements.add(measurements[index]);
    measurements[index] = null;
    types[index] = null;
    values[index] = null;
  }

  @Override
  public List<Path> getPaths() {
    List<Path> ret = new ArrayList<>();

    for (String m : measurements) {
      ret.add(new Path(deviceId, m));
    }
    return ret;
  }

  public String getDeviceId() {
    return this.deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String[] getMeasurements() {
    return this.measurements;
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public Object[] getValues() {
    return this.values;
  }

  public void setValues(Object[] values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertPlan that = (InsertPlan) o;
    return time == that.time && Objects.equals(deviceId, that.deviceId)
        && Arrays.equals(measurements, that.measurements)
        && Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceId, time);
  }

  @Override
  public void serializeTo(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.INSERT.ordinal();
    stream.writeByte((byte) type);
    stream.writeLong(time);

    putString(stream, deviceId);

    stream.writeInt(measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));

    for (String m : measurements) {
      if (m != null) {
        putString(stream, m);
      }
    }

    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        schemas[i].serializeTo(stream);
      }
    }

    try {
      putValues(stream);
    } catch (QueryProcessException e) {
      throw new IOException(e);
    }
  }

  private void putValues(DataOutputStream outputStream) throws QueryProcessException, IOException {
    for (int i = 0; i < values.length; i++) {
      if (types[i] == null) {
        continue;
      }
      ReadWriteIOUtils.write(types[i], outputStream);
      switch (types[i]) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values[i], outputStream);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values[i], outputStream);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) values[i], outputStream);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values[i], outputStream);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values[i], outputStream);
          break;
        case TEXT:
          ReadWriteIOUtils.write((Binary) values[i], outputStream);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + types[i]);
      }
    }
  }

  private void putValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < values.length; i++) {
      if (types[i] == null) {
        continue;
      }
      ReadWriteIOUtils.write(types[i], buffer);
      switch (types[i]) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values[i], buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values[i], buffer);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) values[i], buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values[i], buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values[i], buffer);
          break;
        case TEXT:
          ReadWriteIOUtils.write((Binary) values[i], buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + types[i]);
      }
    }
  }

  public List<String> getFailedMeasurements() {
    return failedMeasurements;
  }

  public TSDataType[] getTypes() {
    return types;
  }

  public void setTypes(TSDataType[] types) {
    this.types = types;
  }

  public void setValues(ByteBuffer buffer) throws QueryProcessException {
    for (int i = 0; i < measurements.length; i++) {
      types[i] = ReadWriteIOUtils.readDataType(buffer);
      switch (types[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + types[i]);
      }
    }
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.INSERT.ordinal();
    buffer.put((byte) type);
    buffer.putLong(time);

    putString(buffer, deviceId);

    buffer.putInt(measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));

    for (String measurement : measurements) {
      if (measurement != null) {
        putString(buffer, measurement);
      }
    }

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    this.time = buffer.getLong();
    this.deviceId = readString(buffer);

    int measurementSize = buffer.getInt();

    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    this.types = new TSDataType[measurementSize];
    this.values = new Object[measurementSize];
    try {
      setValues(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String toString() {
    return "deviceId: " + deviceId + ", time: " + time;
  }

  public TimeValuePair composeTimeValuePair(int measurementIndex) {
    if (measurementIndex >= values.length) {
      return null;
    }
    Object value = values[measurementIndex];
    return new TimeValuePair(time,
        TsPrimitiveType.getByType(schemas[measurementIndex].getType(), value));
  }
}
