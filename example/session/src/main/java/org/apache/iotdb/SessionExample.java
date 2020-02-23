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
package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.thrift.TException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SessionExample {

  private static Session session;

  public static void main(String[] args)
          throws IoTDBSessionException, TException, IoTDBRPCException, SQLException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");
    if (session.checkTimeseriesExists("root.sg1.d1.s1")) {
      session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (session.checkTimeseriesExists("root.sg1.d1.s2")) {
      session.createTimeseries("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (session.checkTimeseriesExists("root.sg1.d1.s3")) {
      session.createTimeseries("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    insert();
    insertInBatch();
    insertRowBatch();
    nonQuery();
    query();
    deleteData();
    deleteTimeseries();
    session.close();
  }

  private static void insert() throws IoTDBSessionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insert(deviceId, time, measurements, values);
    }
  }

  private static void insertInBatch() throws IoTDBSessionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
  }

  /**
   * insert a batch data of one device, each batch contains multiple timestamps with values of sensors
   *
   * a RowBatch example:
   *
   *      device1
   * time s1, s2, s3
   * 1,   1,  1,  1
   * 2,   2,  2,  2
   * 3,   3,  3,  3
   *
   * Users need to control the count of RowBatch and write a batch when it reaches the maxBatchSize
   *
   */
  private static void insertRowBatch() throws IoTDBSessionException {
    // The schema of sensors of one device
    Schema schema = new Schema();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 100);

    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (long time = 0; time < 100; time++) {
      int row = rowBatch.batchSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
        session.insertBatch(rowBatch);
        rowBatch.reset();
      }
    }

    if (rowBatch.batchSize != 0) {
      session.insertBatch(rowBatch);
      rowBatch.reset();
    }
  }

  private static void deleteData() throws IoTDBSessionException {
    String path = "root.sg1.d1.s1";
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries() throws IoTDBSessionException {
    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d1.s2");
    paths.add("root.sg1.d1.s3");
    session.deleteTimeseries(paths);
  }

  private static void query() throws TException, IoTDBRPCException, SQLException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    dataSet.setBatchSize(1024); // default is 512
    while (dataSet.hasNext()){
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void nonQuery() throws TException, IoTDBRPCException, SQLException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1);");
  }
}