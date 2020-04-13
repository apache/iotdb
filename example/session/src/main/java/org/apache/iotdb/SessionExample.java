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

import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionExample {

  private static Session session;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(true);

    try {
      session.setStorageGroup("root.sg1");
    } catch (StatementExecutionException e) {
      if (!e.getMessage().contains("StorageGroupAlreadySetException")) {
        throw e;
      }
//       ignore duplicated set
    }

    if (!session.checkTimeseriesExists("root.sg1.d1.s1")) {
      session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists("root.sg1.d1.s2")) {
      session.createTimeseries("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists("root.sg1.d1.s3")) {
      session.createTimeseries("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
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

  private static void insert() throws IoTDBConnectionException, StatementExecutionException {
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

  private static void insertInObject()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 100; time++) {
      session.insert(deviceId, time, measurements, 1L, 1L, 1L);
    }
  }

  private static void insertInBatch() throws IoTDBConnectionException, BatchExecutionException {
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
   * insert a batch data of one device, each batch contains multiple timestamps with values of
   * sensors
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
   */
  private static void insertRowBatch() throws IoTDBConnectionException, BatchExecutionException {
    // The schema of sensors of one device
    Schema schema = new Schema();
    schema.registerTimeseries(new Path("root.sg1.d1", "s1"),
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path("root.sg1.d1", "s2"),
        new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path("root.sg1.d1", "s3"),
        new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

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

  private static void insertMultipleDeviceRowBatch()
      throws IoTDBConnectionException, BatchExecutionException {
    // The schema of sensors of one device
    Schema schema1 = new Schema();
    schema1.registerTimeseries(new Path("root.sg1.d1.s1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema1.registerTimeseries(new Path("root.sg1.d1.s2"), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema1.registerTimeseries(new Path("root.sg1.d1.s3"), new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch1 = schema1.createRowBatch("root.sg1.d1", 100);

    Schema schema2 = new Schema();
    schema2.registerTimeseries(new Path("root.sg1.d2.s1"), new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema2.registerTimeseries(new Path("root.sg1.d2.s2"), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema2.registerTimeseries(new Path("root.sg1.d2.s3"), new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch2 = schema2.createRowBatch("root.sg1.d2", 100);
    
    Schema schema3 = new Schema();
    Map<String, MeasurementSchema> template = new HashMap<>();
    template.put("s1", new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    template.put("s2", new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE));
    schema3.registerDeviceTemplate("template3", template);
    schema3.extendTemplate("template3", new MeasurementSchema("s3", TSDataType.FLOAT, TSEncoding.RLE));
    schema3.registerDevice("root.sg1.d3", "template3");
    
    RowBatch rowBatch3 = schema3.createRowBatch("root.sg1.d3", 100);
    
    Map<String, RowBatch> rowBatchMap = new HashMap<>();
    rowBatchMap.put("root.sg1.d1", rowBatch1);
    rowBatchMap.put("root.sg1.d2", rowBatch2);
    rowBatchMap.put("root.sg1.d3", rowBatch3);

    long[] timestamps1 = rowBatch1.timestamps;
    Object[] values1 = rowBatch1.values;
    long[] timestamps2 = rowBatch2.timestamps;
    Object[] values2 = rowBatch2.values;
    long[] timestamps3 = rowBatch3.timestamps;
    Object[] values3 = rowBatch3.values;

    for (long time = 0; time < 100; time++) {
      int row1 = rowBatch1.batchSize++;
      int row2 = rowBatch2.batchSize++;
      int row3 = rowBatch3.batchSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (rowBatch1.batchSize == rowBatch1.getMaxBatchSize()) {
        session.insertMultipleDeviceBatch(rowBatchMap);

        rowBatch1.reset();
        rowBatch2.reset();
        rowBatch3.reset();
      }
    }

    if (rowBatch1.batchSize != 0) {
      session.insertMultipleDeviceBatch(rowBatchMap);
      rowBatch1.reset();
      rowBatch2.reset();
      rowBatch3.reset();
    }
  }

  private static void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = "root.sg1.d1.s1";
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d1.s2");
    paths.add("root.sg1.d1.s3");
    session.deleteTimeseries(paths);
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet;
    dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    System.out.println(dataSet.getColumnNames());
    dataSet.setBatchSize(1024); // default is 512
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void nonQuery() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1);");
  }
}