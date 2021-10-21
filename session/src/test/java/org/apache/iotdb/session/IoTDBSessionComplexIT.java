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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.example.Counter;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBSessionComplexIT {

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsertByStr() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();
    insertByStr();

    insertViaSQL();
    queryByDevice("root.sg1.d1");

    session.close();
  }

  private void insertByStr() throws IoTDBConnectionException, StatementExecutionException {
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
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  @Test
  public void testInsertByObject() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      session.insertRecord(deviceId, time, measurements, types, 1L, 2L, 3L);
    }

    insertViaSQL();
    queryByDevice("root.sg1.d1");

    session.close();
  }

  @Test
  public void testAlignByDevice() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTablet("root.sg1.d1");

    SessionDataSet sessionDataSet =
        session.executeQueryStatement("select '11', s1, '11' from root.sg1.d1 align by device");
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      StringBuilder sb = new StringBuilder();
      List<Field> fields = sessionDataSet.next().getFields();
      for (Field f : fields) {
        sb.append(f.getStringValue()).append(",");
      }
      Assert.assertEquals("root.sg1.d1,'11',0,'11',", sb.toString());
    }
    Assert.assertEquals(100, count);
    sessionDataSet.closeOperationHandle();

    session.close();
  }

  @Test
  public void testBatchInsertSeqAndUnseq()
      throws SQLException, ClassNotFoundException, IoTDBConnectionException,
          StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTablet("root.sg1.d1");

    session.executeNonQueryStatement("FLUSH");
    session.executeNonQueryStatement("FLUSH root.sg1");
    session.executeNonQueryStatement("MERGE");
    session.executeNonQueryStatement("FULL MERGE");

    queryForBatch();

    session.close();
  }

  @Test
  public void testBatchInsert()
      throws StatementExecutionException, SQLException, ClassNotFoundException,
          IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTablet("root.sg1.d1");

    queryForBatch();

    session.close();
  }

  @Test
  public void testTestMethod() throws StatementExecutionException, IoTDBConnectionException {

    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");
    String deviceId = "root.sg1.d1";

    createTimeseries();

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100);

    session.testInsertTablet(tablet);

    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.testInsertRecord(deviceId, time, measurements, values);
    }

    measurements = new ArrayList<>();
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
        session.testInsertRecords(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.testInsertRecords(deviceIds, timestamps, measurementsList, valuesList);

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root.sg1.**");
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
    }
    Assert.assertEquals(6, count);
    session.close();
  }

  @Test
  public void testRawDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertRecords();

    rawDataQuery();
  }

  @Test
  public void testLastDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertRecords();

    lastDataQuery();
  }

  @Test
  public void test()
      throws ClassNotFoundException, SQLException, IoTDBConnectionException,
          StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
    List<String> standard =
        Arrays.asList(
            "Time",
            "root.sg1.d1.s1",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d2.s1",
            "root.sg1.d2.s2",
            "root.sg1.d2.s3");
    List<String> standardAfterDelete =
        Arrays.asList(
            "Time",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d2.s1",
            "root.sg1.d2.s2",
            "root.sg1.d2.s3");

    session.setStorageGroup("root.sg1");

    createTimeseries();

    String deviceId1 = "root.sg1.d1";
    List<String> measurements1 = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements1.add("s1");
    measurements1.add("s2");
    measurements1.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId1, time, measurements1, types, values);
    }

    insertViaSQL();

    queryByDevice("root.sg1.d1");

    deleteData();

    insertByStr();

    insertViaSQL();

    queryByDevice("root.sg1.d1");

    deleteData();

    queryAll(standard);

    deleteTimeseries();

    queryAll(standardAfterDelete);

    insertRecords();

    queryByDevice("root.sg1.d2");

    deleteData();

    String deviceId2 = "root.sg1.d2";
    List<String> measurements2 = new ArrayList<>();
    measurements2.add("s1");
    measurements2.add("s2");
    measurements2.add("s3");
    List<String> deviceIds2 = new ArrayList<>();
    List<List<String>> measurementsList2 = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");

      deviceIds2.add(deviceId2);
      measurementsList2.add(measurements2);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds2, timestamps, measurementsList2, valuesList);
        deviceIds2.clear();
        measurementsList2.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds2, timestamps, measurementsList2, valuesList);

    queryByDevice("root.sg1.d2");

    session.createTimeseries(
        "root.sg1.d1.1_2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.\"1.2.3\"", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.\"1.2.4\"", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.1_2"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.\"1.2.3\""));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.\"1.2.4\""));

    session.setStorageGroup("root.1");
    session.createTimeseries(
        "root.1.2.3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.setStorageGroup("root.sg2");
    session.createTimeseries(
        "root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    deleteStorageGroupTest();

    session.setStorageGroup("root.sg3");
    insertTablet("root.sg3.d1");

    session.createTimeseries(
        "root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    insertTablet("root.sg4.d1");

    insertTablet("root.sg5.d1");

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.** group by device");
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
    }
    Assert.assertEquals(300, count);

    session.close();
  }

  @Test
  public void TestSessionInterfacesWithDisabledWAL()
      throws StatementExecutionException, IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    session.setStorageGroup("root.sg1");
    String deviceId = "root.sg1.d1";

    boolean isEnableWAL = IoTDBDescriptor.getInstance().getConfig().isEnableWal();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    createTimeseries();

    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    for (long time = 0; time < 100; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 200);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    for (int time = 1; time <= 100; time++) {
      timestamps[time - 1] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[time - 1] = i;
      }
      tablet.rowSize++;
    }

    for (int time = 101; time <= 200; time++) {
      int rowIndex = time - 1;
      tablet.addTimestamp(rowIndex, time);
      long value = 0;
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
        value++;
      }
      tablet.rowSize++;
    }

    session.insertTablet(tablet);

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    int count = 0;
    while (dataSet.hasNext()) {
      count++;
    }
    Assert.assertEquals(201, count);

    IoTDBDescriptor.getInstance().getConfig().setEnableWal(isEnableWAL);
    session.close();
  }

  private void createTimeseries() throws StatementExecutionException, IoTDBConnectionException {
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
  }

  private void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d2";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  private void rawDataQuery() throws StatementExecutionException, IoTDBConnectionException {
    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d2.*");
    paths.add("root.sg1.d2.s1");
    paths.add("root.sg1.d2.s2");

    SessionDataSet sessionDataSet = session.executeRawDataQuery(paths, 450L, 600L);
    sessionDataSet.setFetchSize(1024);

    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      StringBuilder sb = new StringBuilder();
      List<Field> fields = sessionDataSet.next().getFields();
      for (Field f : fields) {
        sb.append(f.getStringValue()).append(",");
      }
      Assert.assertEquals("3,1,2,1,2,", sb.toString());
    }
    Assert.assertEquals(50, count);
    sessionDataSet.closeOperationHandle();
  }

  private void lastDataQuery() throws StatementExecutionException, IoTDBConnectionException {
    List<String> paths = new ArrayList<>();

    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d2.s1");

    SessionDataSet sessionDataSet = session.executeLastDataQuery(paths);
    sessionDataSet.setFetchSize(1024);

    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      List<Field> fields = sessionDataSet.next().getFields();
      Assert.assertEquals("[root.sg1.d2.s1,1,INT64]", fields.toString().replace(" ", ""));
    }
    Assert.assertEquals(1, count);
    sessionDataSet.closeOperationHandle();
  }

  private void insertTablet(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);

    for (long time = 0; time < 100; time++) {
      int rowIndex = tablet.rowSize++;
      long value = 0;
      tablet.addTimestamp(rowIndex, time);
      for (int s = 0; s < 3; s++) {
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
        value++;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path1 = "root.sg1.d1.s1";
    String path2 = "root.sg1.d1.s2";
    String path3 = "root.sg1.d1.s3";
    String path4 = "root.sg1.d2.s1";
    String path5 = "root.sg1.d2.s2";
    String path6 = "root.sg1.d2.s3";
    long deleteTime = 500;

    List<String> paths = new ArrayList<>();
    paths.add(path1);
    paths.add(path2);
    paths.add(path3);
    paths.add(path4);
    paths.add(path5);
    paths.add(path6);
    try {
      session.deleteData(paths, deleteTime);
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode()) {
        throw e;
      }
    }
  }

  private void deleteTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    session.deleteTimeseries("root.sg1.d1.s1");
  }

  private void queryAll(List<String> standard) throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      for (int i = 0; i < colCount; i++) {
        Assert.assertTrue(standard.contains(metaData.getColumnLabel(i + 1)));
      }
      while (resultSet.next()) {
        fail();
      }
    }
  }

  private void deleteStorageGroupTest()
      throws ClassNotFoundException, SQLException, IoTDBConnectionException,
          StatementExecutionException {
    try {
      session.deleteStorageGroup("root.sg1.d1.s1");
    } catch (StatementExecutionException e) {
      assertTrue(e.getMessage().contains("Path [root.sg1.d1.s1] does not exist"));
    }
    session.deleteStorageGroup("root.sg1");
    File folder = new File("data/system/storage_groups/root.sg1/");
    assertFalse(folder.exists());
    session.setStorageGroup("root.sg1.d1");
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    Class.forName(Config.JDBC_DRIVER_NAME);
    List<String> standards =
        Arrays.asList("Time", "root.1.2.3", "root.sg2.d1.s1", "root.sg1.d1.s1");
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      for (int i = 0; i < colCount; i++) {
        Assert.assertTrue(standards.contains(metaData.getColumnLabel(i + 1)));
      }
      while (resultSet.next()) {
        fail();
      }
      List<String> storageGroups = new ArrayList<>();
      storageGroups.add("root.sg1.d1");
      storageGroups.add("root.sg2");
      session.deleteStorageGroups(storageGroups);
    }
  }

  private void queryByDevice(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from " + deviceId);
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    long expectedSum = 1 + 2 + 3;
    while (sessionDataSet.hasNext()) {
      count++;
      long actualSum = 0;
      for (Field f : sessionDataSet.next().getFields()) {
        actualSum += f.getLongV();
      }
      Assert.assertEquals(expectedSum, actualSum);
    }

    switch (deviceId) {
      case "root.sg1.d1":
        Assert.assertEquals(101, count);
        break;
      case "root.sg1.d2":
        Assert.assertEquals(500, count);
        break;
    }

    sessionDataSet.closeOperationHandle();
  }

  private void insertViaSQL() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "insert into root.sg1.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");
  }

  private void checkSetSG(Session session, String sg, boolean correctStatus)
      throws IoTDBConnectionException {
    boolean status = true;
    try {
      session.setStorageGroup(sg);
    } catch (StatementExecutionException e) {
      status = false;
    }
    assertEquals(correctStatus, status);
  }

  private void queryForBatch() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    List<String> standards =
        Arrays.asList(
            "Time",
            "root.sg1.d1.s1",
            "root.sg1.d1.s2",
            "root.sg1.d1.s3",
            "root.sg1.d2.s1",
            "root.sg1.d2.s2",
            "root.sg1.d2.s3");
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      for (int i = 0; i < colCount; i++) {
        Assert.assertTrue(standards.contains(metaData.getColumnLabel(i + 1)));
      }

      int count = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          count++;
        }
      }
      Assert.assertEquals(700, count);
    }
  }

  @Test
  public void testInsertTabletWithTriggers()
      throws StatementExecutionException, IoTDBConnectionException, TriggerManagementException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    session.setStorageGroup("root.sg1");
    createTimeseries();

    session.executeNonQueryStatement(
        "create trigger d1s1 after insert on root.sg1.d1.s1 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");
    session.executeNonQueryStatement(
        "create trigger d1s2 before insert on root.sg1.d1.s2 as 'org.apache.iotdb.db.engine.trigger.example.Counter'");

    assertEquals(
        Counter.BASE,
        ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s1"))
            .getCounter());
    assertEquals(
        Counter.BASE,
        ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s2"))
            .getCounter());
    try {
      int counter =
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s3"))
              .getCounter();
      fail(String.valueOf(counter - Counter.BASE));
    } catch (TriggerManagementException e) {
      assertEquals("Trigger d1s3 does not exist.", e.getMessage());
    }

    insertTablet("root.sg1.d1");

    assertEquals(
        Counter.BASE + 200,
        ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s1"))
            .getCounter());
    assertEquals(
        Counter.BASE + 200,
        ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s2"))
            .getCounter());
    try {
      int counter =
          ((Counter) TriggerRegistrationService.getInstance().getTriggerInstance("d1s3"))
              .getCounter();
      fail(String.valueOf(counter - Counter.BASE));
    } catch (TriggerManagementException e) {
      assertEquals("Trigger d1s3 does not exist.", e.getMessage());
    }

    session.close();
  }

  @Test
  public void testSessionCluster() throws IoTDBConnectionException, StatementExecutionException {
    ArrayList<String> nodeList = new ArrayList<>();
    nodeList.add("127.0.0.1:6669");
    nodeList.add("127.0.0.1:6667");
    nodeList.add("127.0.0.1:6668");
    session = new Session(nodeList, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();
    insertByStr();

    insertViaSQL();
    queryByDevice("root.sg1.d1");

    session.close();
  }

  @Test
  public void testErrorSessionCluster() throws IoTDBConnectionException {
    ArrayList<String> nodeList = new ArrayList<>();
    // test Format error
    nodeList.add("127.0.0.16669");
    nodeList.add("127.0.0.1:6667");
    session = new Session(nodeList, "root", "root");
    try {
      session.open();
    } catch (Exception e) {
      Assert.assertEquals("NodeUrl Incorrect format", e.getMessage());
    }
    session.close();
  }
}
