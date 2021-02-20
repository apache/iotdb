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
package org.apache.iotdb.hadoop.tsfile.record;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HDFSTSRecord implements Writable {

  /** timestamp of this TSRecord. */
  private long time;
  /** deviceId of this TSRecord. */
  private String deviceId;
  /** all value of this TSRecord. */
  private List<DataPoint> dataPointList = new ArrayList<>();

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public List<DataPoint> getDataPointList() {
    return dataPointList;
  }

  public void setDataPointList(List<DataPoint> dataPointList) {
    this.dataPointList = dataPointList;
  }

  public HDFSTSRecord() {}

  /**
   * constructor of HDFSTSRecord.
   *
   * @param timestamp timestamp of this TSRecord
   * @param deviceId deviceId of this TSRecord
   */
  public HDFSTSRecord(long timestamp, String deviceId) {
    this.time = timestamp;
    this.deviceId = deviceId;
  }

  public TSRecord convertToTSRecord() {
    TSRecord tsRecord = new TSRecord(time, deviceId);
    tsRecord.dataPointList = this.dataPointList;
    return tsRecord;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(time);
    out.writeInt(deviceId.getBytes(StandardCharsets.UTF_8).length);
    out.write(deviceId.getBytes(StandardCharsets.UTF_8));
    out.writeInt(dataPointList.size());
    for (DataPoint dataPoint : dataPointList) {
      out.write(dataPoint.getType().serialize());
      out.writeInt(dataPoint.getMeasurementId().getBytes(StandardCharsets.UTF_8).length);
      out.write(dataPoint.getMeasurementId().getBytes(StandardCharsets.UTF_8));
      switch (dataPoint.getType()) {
        case BOOLEAN:
          out.writeBoolean((boolean) dataPoint.getValue());
          break;
        case INT32:
          out.writeInt((int) dataPoint.getValue());
          break;
        case INT64:
          out.writeLong((long) dataPoint.getValue());
          break;
        case FLOAT:
          out.writeFloat((float) dataPoint.getValue());
          break;
        case DOUBLE:
          out.writeDouble((double) dataPoint.getValue());
          break;
        case TEXT:
          out.writeInt(((Binary) dataPoint.getValue()).getLength());
          out.write(((Binary) dataPoint.getValue()).getValues());
          break;
        default:
          throw new UnSupportedDataTypeException("The type isn't supported");
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    time = in.readLong();
    int lenOfDeviceId = in.readInt();
    byte[] deviceBytes = new byte[lenOfDeviceId];
    in.readFully(deviceBytes);
    int len = in.readInt();
    List<DataPoint> dataPoints = new ArrayList<>(len);

    for (int i = 0; i < len; i++) {
      TSDataType dataType = TSDataType.deserialize(in.readByte());
      int lenOfMeasurementId = in.readInt();
      byte[] c = new byte[lenOfMeasurementId];
      in.readFully(c);
      String measurementId = new String(c, StandardCharsets.UTF_8);
      switch (dataType) {
        case BOOLEAN:
          dataPoints.add(new BooleanDataPoint(measurementId, in.readBoolean()));
          break;
        case INT32:
          dataPoints.add(new IntDataPoint(measurementId, in.readInt()));
          break;
        case INT64:
          dataPoints.add(new LongDataPoint(measurementId, in.readLong()));
          break;
        case FLOAT:
          dataPoints.add(new FloatDataPoint(measurementId, in.readFloat()));
          break;
        case DOUBLE:
          dataPoints.add(new DoubleDataPoint(measurementId, in.readDouble()));
          break;
        case TEXT:
          int stringLen = in.readInt();
          byte[] b = new byte[stringLen];
          in.readFully(b);
          Binary binary = new Binary(b);
          dataPoints.add(new StringDataPoint(measurementId, binary));
          break;
        default:
          throw new UnSupportedDataTypeException("The type isn't supported");
      }
    }
    dataPointList = dataPoints;
  }

  /**
   * output this HDFSTSRecord in String format.For example: {device id: d1 time: 123456 ,data:[
   * {measurement id: s1 type:INT32 value: 1 } {measurement id: s2 type: FLOAT value: 11.11 }
   * {measurement id: s3 type: BOOLEAN value: true }]}
   *
   * @return the String format of this TSRecord
   */
  @Override
  public String toString() {
    StringContainer sc = new StringContainer(" ");
    sc.addTail("{device id:", deviceId, "time:", time, ",data:[");
    for (DataPoint tuple : dataPointList) {
      sc.addTail(tuple);
    }
    sc.addTail("]}");
    return sc.toString();
  }

  /**
   * add one data point to this HDFSTSRecord.
   *
   * @param tuple data point to be added
   */
  public HDFSTSRecord addTuple(DataPoint tuple) {
    this.dataPointList.add(tuple);
    return this;
  }
}
