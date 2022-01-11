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

package org.apache.iotdb.tool;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class CsvModel {

  private int count;

  private String deviceId;

  private List<Long> times;

  private List<List<TSDataType>> typesList;

  private List<List<Object>> valuesList;

  private List<List<String>> measurementsList;

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public List<Long> getTimes() {
    if (times == null) {
      times = new ArrayList<>();
    }
    return times;
  }

  public void setTimes(List<Long> times) {
    this.times = times;
  }

  public List<List<TSDataType>> getTypesList() {
    if (typesList == null) {
      typesList = new ArrayList<>();
    }
    return typesList;
  }

  public void setTypesList(List<List<TSDataType>> typesList) {
    this.typesList = typesList;
  }

  public List<List<Object>> getValuesList() {
    if (valuesList == null) {
      valuesList = new ArrayList<>();
    }
    return valuesList;
  }

  public void setValuesList(List<List<Object>> valuesList) {
    this.valuesList = valuesList;
  }

  public List<List<String>> getMeasurementsList() {
    if (measurementsList == null) {
      measurementsList = new ArrayList<>();
    }
    return measurementsList;
  }

  public void setMeasurementsList(List<List<String>> measurementsList) {
    this.measurementsList = measurementsList;
  }
}
