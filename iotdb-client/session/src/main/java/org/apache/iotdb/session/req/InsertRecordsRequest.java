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
package org.apache.iotdb.session.req;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class InsertRecordsRequest {
  private List<String> deviceIds;
  private List<List<String>> measurementsIdsList;
  private List<Long> timestamps;
  private List<List<TSDataType>> dataTypesList;
  private List<List<Object>> valuesList;

  public InsertRecordsRequest(
      List<String> deviceIds,
      List<List<String>> measurementsIdsList,
      List<Long> timestamps,
      List<List<TSDataType>> dataTypesList,
      List<List<Object>> valuesList) {
    this.deviceIds = deviceIds;
    this.measurementsIdsList = measurementsIdsList;
    this.timestamps = timestamps;
    this.dataTypesList = dataTypesList;
    this.valuesList = valuesList;
  }

  public InsertRecordsRequest() {}

  public List<String> getDeviceIds() {
    return deviceIds;
  }

  public List<List<String>> getMeasurementsIdsList() {
    return measurementsIdsList;
  }

  public List<Long> getTimestamps() {
    return timestamps;
  }

  public List<List<TSDataType>> getDataTypesList() {
    return dataTypesList;
  }

  public List<List<Object>> getValuesList() {
    return valuesList;
  }

  public void setDeviceIds(List<String> deviceIds) {
    this.deviceIds = deviceIds;
  }

  public void setMeasurementsIdsList(List<List<String>> measurementsIdsList) {
    this.measurementsIdsList = measurementsIdsList;
  }

  public void setTimestamps(List<Long> timestamps) {
    this.timestamps = timestamps;
  }

  public void setDataTypesList(List<List<TSDataType>> dataTypesList) {
    this.dataTypesList = dataTypesList;
  }

  public void setValuesList(List<List<Object>> valuesList) {
    this.valuesList = valuesList;
  }
}
