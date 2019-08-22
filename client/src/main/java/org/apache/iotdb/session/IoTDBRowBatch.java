/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.jdbc.Utils;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.service.rpc.thrift.TSDataValueList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * a row batch of one device
 *
 * timestamps, s1, s2, s3 (measurements)
 *  1,    1,  1,  2
 *  2,    2,  2,  2
 */
public class IoTDBRowBatch {

  private String deviceId;
  private List<String> measurements;
  private List<TSDataValueList> columns;
  private List<Long> timestamps;
  private List<IoTDBDataType> dataTypes;

  /**
   * @param deviceId deviceId of this RowBatch
   * @param measurements measurements of this device
   */
  public IoTDBRowBatch(String deviceId, List<String> measurements, List<IoTDBDataType> dataTypes) {
    this.deviceId = deviceId;
    this.measurements = measurements;
    initColumns(dataTypes);
    this.dataTypes = dataTypes;
    timestamps = new ArrayList<>();
  }

  private void initColumns(List<IoTDBDataType> dataTypes) {
    columns = new ArrayList<>();
    for (int i = 0; i < measurements.size(); i++) {
      TSDataValueList dataValueList = new TSDataValueList();
      switch (dataTypes.get(i)) {
        case BOOLEAN:
          dataValueList.setBool_vals(new ArrayList<>());
          break;
        case INT32:
          dataValueList.setInt_vals(new ArrayList<>());
          break;
        case INT64:
          dataValueList.setDouble_vals(new ArrayList<>());
          break;
        case FLOAT:
          dataValueList.setFloat_vals(new ArrayList<>());
          break;
        case DOUBLE:
          dataValueList.setDouble_vals(new ArrayList<>());
          break;
        case TEXT:
          dataValueList.setBinary_vals(new ArrayList<>());
          break;
      }
      columns.add(dataValueList);
    }
  }

  /**
   * @param timestamp timestamp of this row
   * @param row multiple values of one device, will be written to each columns
   */
  public void addRow(long timestamp, List<Object> row) {
    this.timestamps.add(timestamp);
    for (int i = 0; i < row.size(); i++) {
      switch (dataTypes.get(i)) {
        case BOOLEAN:
          columns.get(i).addToBool_vals((Boolean) row.get(i));
          break;
        case INT32:
          columns.get(i).addToInt_vals((Integer) row.get(i));
          break;
        case INT64:
          columns.get(i).addToLong_vals((Long) row.get(i));
          break;
        case FLOAT:
          columns.get(i).addToFloat_vals((Float) row.get(i));
          break;
        case DOUBLE:
          columns.get(i).addToDouble_vals((Double) row.get(i));
          break;
        case TEXT:
          columns.get(i).addToBinary_vals((ByteBuffer.wrap((byte[])row.get(i))));
      }
    }
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<TSDataValueList> getColumns() {
    return columns;
  }

  public void setColumns(List<TSDataValueList> columns) {
    this.columns = columns;
  }

  public List<Long> getTimestamps() {
    return timestamps;
  }

  public void setTimestamps(List<Long> timestamps) {
    this.timestamps = timestamps;
  }

  public List<IoTDBDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<IoTDBDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }
}
