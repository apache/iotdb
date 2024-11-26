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
package org.apache.tsfile.write.record;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.StringContainer;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.DateDataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * TSRecord is a kind of format that TsFile receives.TSRecord contains timestamp, deviceId and a
 * list of data points.
 */
public class TSRecord {

  /** timestamp of this TSRecord. */
  public long time;

  /** deviceId of this TSRecord. */
  public IDeviceID deviceId;

  /** all value of this TSRecord. */
  public List<DataPoint> dataPointList = new ArrayList<>();

  /**
   * constructor of TSRecord.
   *
   * @param deviceId deviceId of this TSRecord
   * @param timestamp timestamp of this TSRecord
   */
  @TsFileApi
  public TSRecord(String deviceId, long timestamp) {
    this.time = timestamp;
    this.deviceId = Factory.DEFAULT_FACTORY.create(deviceId);
  }

  @TsFileApi
  public TSRecord(IDeviceID deviceId, long timestamp) {
    this.time = timestamp;
    this.deviceId = deviceId;
  }

  public void setTime(long timestamp) {
    this.time = timestamp;
  }

  /**
   * add one data point to this TSRecord.
   *
   * @param tuple data point to be added
   */
  public TSRecord addTuple(DataPoint tuple) {
    this.dataPointList.add(tuple);
    return this;
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, int val) {
    return addTuple(new IntDataPoint(measurementName, val));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, long val) {
    return addTuple(new LongDataPoint(measurementName, val));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, float val) {
    return addTuple(new FloatDataPoint(measurementName, val));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, double val) {
    return addTuple(new DoubleDataPoint(measurementName, val));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, boolean val) {
    return addTuple(new BooleanDataPoint(measurementName, val));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, String val) {
    return addTuple(
        new StringDataPoint(measurementName, new Binary(val, TSFileConfig.STRING_CHARSET)));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, byte[] val) {
    return addTuple(new StringDataPoint(measurementName, new Binary(val)));
  }

  @TsFileApi
  public TSRecord addPoint(String measurementName, LocalDate val) {
    return addTuple(new DateDataPoint(measurementName, val));
  }

  /**
   * output this TSRecord in String format.For example: {device id: d1 time: 123456 ,data:[
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
}
