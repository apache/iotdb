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
import java.util.List;
import java.util.Objects;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.TSRecord;

public class InsertPlan extends PhysicalPlan {

  private String deviceId;
  private String[] measurements;
  private TSDataType[] dataTypes;
  private String[] values;
  private long time;

  public InsertPlan() {
    super(false, OperatorType.INSERT);
    canbeSplit = false;
  }

  @TestOnly
  public InsertPlan(String deviceId, long insertTime, String measurement, String insertValue) {
    super(false, OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = new String[] {measurement};
    this.values = new String[] {insertValue};
    canbeSplit = false;
  }

  public InsertPlan(TSRecord tsRecord) {
    super(false, OperatorType.INSERT);
    this.deviceId = tsRecord.deviceId;
    this.time = tsRecord.time;
    this.measurements = new String[tsRecord.dataPointList.size()];
    this.dataTypes = new TSDataType[tsRecord.dataPointList.size()];
    this.values = new String[tsRecord.dataPointList.size()];
    for (int i = 0; i < tsRecord.dataPointList.size(); i++) {
      measurements[i] = tsRecord.dataPointList.get(i).getMeasurementId();
      dataTypes[i] = tsRecord.dataPointList.get(i).getType();
      values[i] = tsRecord.dataPointList.get(i).getValue().toString();
    }
    canbeSplit = false;
  }

  public InsertPlan(String deviceId, long insertTime, String[] measurementList,
      String[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurementList;
    this.values = insertValues;
    canbeSplit = false;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
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

  public String[] getValues() {
    return this.values;
  }

  public void setValues(String[] values) {
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

    stream.writeInt(measurements.length);
    for (String m : measurements) {
      putString(stream, m);
    }

    stream.writeInt(values.length);
    for (String m : values) {
      putString(stream, m);
    }
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.INSERT.ordinal();
    buffer.put((byte) type);
    buffer.putLong(time);

    putString(buffer, deviceId);

    buffer.putInt(measurements.length);
    for (String m : measurements) {
      putString(buffer, m);
    }

    buffer.putInt(values.length);
    for (String m : values) {
      putString(buffer, m);
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

    int valueSize = buffer.getInt();
    this.values = new String[valueSize];
    for (int i = 0; i < valueSize; i++) {
      values[i] = readString(buffer);
    }
  }

  @Override
  public String toString() {
    return "deviceId: " + deviceId + ", time: " + time;
  }

  public TimeValuePair composeTimeValuePair(int measurementIndex) throws QueryProcessException {
    if (measurementIndex >= values.length) {
      return null;
    }
    Object value = CommonUtils.parseValue(dataTypes[measurementIndex], values[measurementIndex]);
    return new TimeValuePair(time, TsPrimitiveType.getByType(dataTypes[measurementIndex], value));
  }
}
