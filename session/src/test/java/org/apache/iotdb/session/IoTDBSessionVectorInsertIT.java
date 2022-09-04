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

package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** use session interface to IT for vector timeseries insert and select Black-box Testing */
public class IoTDBSessionVectorInsertIT {
  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_1.d1.vector";
  private static final String ROOT_SG1_D1 = "root.sg_1.d1";
  private static final String ROOT_SG1_D2 = "root.sg_1.d2";

  private Session session;

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertAlignedTablet() {
    try {
      insertTabletWithAlignedTimeseriesMethod();
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select * from root.sg_1.d1.vector");
      assertEquals(dataSet.getColumnNames().size(), 3);
      assertEquals(dataSet.getColumnNames().get(0), "Time");
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s1");
      assertEquals(dataSet.getColumnNames().get(2), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 1);
        assertEquals(rowRecord.getFields().get(2).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAlignedRecord() {
    try {
      insertAlignedRecord(ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAlignedStringRecord() {
    try {
      insertAlignedStringRecord(ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(ROOT_SG1_D1_VECTOR1 + ".s2", dataSet.getColumnNames().get(1));
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAlignedStringRecords() {
    try {
      insertAlignedStringRecords(ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAlignedRecords() {
    try {
      insertAlignedRecords(ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAlignedRecordsOfOneDevice() {
    try {
      insertAlignedRecordsOfOneDevice(ROOT_SG1_D1_VECTOR1);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void nonAlignedSingleSelectTest() {
    try {
      insertNonAlignedRecord(ROOT_SG1_D1);
      insertTabletWithAlignedTimeseriesMethod();
      insertNonAlignedRecord(ROOT_SG1_D2);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select s2 from root.sg_1.d1.vector");
      assertEquals(2, dataSet.getColumnNames().size());
      assertEquals("Time", dataSet.getColumnNames().get(0));
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void nonAlignedVectorSelectTest() {
    try {
      insertNonAlignedRecord(ROOT_SG1_D1);
      insertTabletWithAlignedTimeseriesMethod();
      insertNonAlignedRecord(ROOT_SG1_D2);
      session.executeNonQueryStatement("flush");
      SessionDataSet dataSet = selectTest("select * from root.sg_1.d1.vector");
      assertEquals(dataSet.getColumnNames().size(), 3);
      assertEquals(dataSet.getColumnNames().get(0), "Time");
      assertEquals(dataSet.getColumnNames().get(1), ROOT_SG1_D1_VECTOR1 + ".s1");
      assertEquals(dataSet.getColumnNames().get(2), ROOT_SG1_D1_VECTOR1 + ".s2");
      int time = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals(rowRecord.getFields().get(0).getIntV(), time);
        assertEquals(rowRecord.getFields().get(1).getIntV(), (time + 1) * 10 + 1);
        assertEquals(rowRecord.getFields().get(2).getIntV(), (time + 1) * 10 + 2);
      }

      dataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  private SessionDataSet selectTest(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    return dataSet;
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private void insertTabletWithAlignedTimeseriesMethod()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT32));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);
    tablet.setAligned(true);
    long timestamp = 0;

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, row * 10 + 1L);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (int) (row * 10 + 2));

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertAlignedTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertAlignedTablet(tablet);
      tablet.reset();
    }
  }

  private void insertNonAlignedRecord(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s2");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  private void insertAlignedRecord(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s2");
    measurements.add("s4");
    measurements.add("s5");
    measurements.add("s6");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      session.insertAlignedRecord(deviceId, time, measurements, types, values);
    }
  }

  private void insertAlignedStringRecord(String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> subMeasurements = new ArrayList<>();
    subMeasurements.add("s2");
    subMeasurements.add("s4");
    subMeasurements.add("s5");
    subMeasurements.add("s6");

    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add(String.valueOf(time * 10 + 3L));
      values.add(String.valueOf(time * 10 + 4L));
      values.add(String.valueOf(time * 10 + 5L));
      values.add(String.valueOf(time * 10 + 6L));
      session.insertAlignedRecord(prefixPath, time, subMeasurements, values);
    }
  }

  private void insertAlignedStringRecords(String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> prefixPaths = new ArrayList<>();
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      prefixPaths.add(prefixPath);
      times.add(time);
      List<String> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      values.add(String.valueOf(time * 10 + 3L));
      values.add(String.valueOf(time * 10 + 4L));
      values.add(String.valueOf(time * 10 + 5L));
      values.add(String.valueOf(time * 10 + 6L));
      subMeasurementsList.add(measurements);
      valueList.add(values);
    }
    session.insertAlignedRecords(prefixPaths, times, subMeasurementsList, valueList);
  }

  private void insertAlignedRecords(String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> prefixPaths = new ArrayList<>();
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      prefixPaths.add(prefixPath);
      times.add(time);
      List<Object> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      subMeasurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecords(prefixPaths, times, subMeasurementsList, typeList, valueList);
  }

  private void insertAlignedRecordsOfOneDevice(String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    List<List<String>> subMeasurementsList = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();
    for (long time = 0; time < 100; time++) {
      times.add(time);
      List<Object> values = new ArrayList<>();
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      measurements.add("s2");
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      values.add(time * 10 + 3L);
      values.add(time * 10 + 4L);
      values.add(time * 10 + 5L);
      values.add(time * 10 + 6L);
      subMeasurementsList.add(measurements);
      typeList.add(types);
      valueList.add(values);
    }
    session.insertAlignedRecordsOfOneDevice(
        prefixPath, times, subMeasurementsList, typeList, valueList);
  }

  @Test
  public void testInsertPartialAlignedTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> multiMeasurementComponents = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      multiMeasurementComponents.add("s" + i);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.INT64);
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      encodings.add(TSEncoding.PLAIN);
      compressors.add(CompressionType.SNAPPY);
    }
    session.createAlignedTimeseries(
        "root.sg.d", multiMeasurementComponents, dataTypes, encodings, compressors, null);
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.DOUBLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.TEXT));

    Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 15; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s1", rowIndex, 1L);
      tablet.addValue("s2", rowIndex, 1D);
      tablet.addValue("s3", rowIndex, new Binary("1"));
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        try {
          session.insertAlignedTablet(tablet, true);
        } catch (StatementExecutionException e) {
          Assert.assertEquals(
              "313: failed to insert measurements [s3] caused by DataType mismatch, Insert measurement s3 type TEXT, metadata tree type INT64",
              e.getMessage());
        }
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      try {
        session.insertAlignedTablet(tablet);
      } catch (StatementExecutionException e) {
        Assert.assertEquals(
            "313: failed to insert measurements [s3] caused by DataType mismatch, Insert measurement s3 type TEXT, metadata tree type INT64",
            e.getMessage());
      }
      tablet.reset();
    }

    SessionDataSet dataSet = session.executeQueryStatement("select s1, s2, s3 from root.sg.d");
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      Assert.assertEquals(1, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(1.0, rowRecord.getFields().get(1).getDoubleV(), 0.01);
      Assert.assertEquals(null, rowRecord.getFields().get(2).getObjectValue(TSDataType.TEXT));
    }
  }
}
