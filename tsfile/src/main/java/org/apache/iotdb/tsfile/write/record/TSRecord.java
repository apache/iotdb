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
package org.apache.iotdb.tsfile.write.record;

import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

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
  public String deviceId;
  /** all value of this TSRecord. */
  public List<DataPoint> dataPointList = new ArrayList<>();

  /**
   * constructor of TSRecord.
   *
   * @param timestamp timestamp of this TSRecord
   * @param deviceId deviceId of this TSRecord
   */
  public TSRecord(long timestamp, String deviceId) {
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
