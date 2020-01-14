/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IoTDBSessionIT {

  private Session session;

  @Before
  public void setUp() throws Exception {
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
  public void testTestMethod() throws IoTDBSessionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");
    String deviceId = "root.sg1.d1";

    // test insert batch
    Schema schema = new Schema();
    schema.registerTimeseries(new Path(deviceId, "s1"), new TimeseriesSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path(deviceId, "s2"), new TimeseriesSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path(deviceId, "s3"), new TimeseriesSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 100);

    session.testInsertBatch(rowBatch);

    // test insert row
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.testInsert(deviceId, time, measurements, values);
    }

    // test insert row in batch
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
        session.testInsertInBatch(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.testInsertInBatch(deviceIds, timestamps, measurementsList, valuesList);
  }

  @Test
  public void test()
      throws ClassNotFoundException, SQLException, IoTDBSessionException, TException, IoTDBRPCException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();
    insert();

    // sql test
    insert_via_sql();
    query3();

//    insertRowBatchTest1();
    deleteData();

    query();

    deleteTimeseries();

    query2();

    insertInBatch();

    query4();

    // Add another storage group to test the deletion of storage group
    session.setStorageGroup("root.sg2");
    session.createTimeseries("root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    deleteStorageGroupTest();

    // Test batch insertions when creating schema automatically is enabled
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    conf.setAutoCreateSchemaEnabled(true);

    // set storage group but do not create timeseries
    session.setStorageGroup("root.sg3");
    insertRowBatchTest1("root.sg3.d1");

    // create timeseries but do not set storage group
    session.createTimeseries("root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    insertRowBatchTest1("root.sg4.d1");

    // do not set storage group and create timeseries
    insertRowBatchTest1("root.sg5.d1");

    conf.setAutoCreateSchemaEnabled(false);

    session.close();
  }

  private void createTimeseries() throws IoTDBSessionException {
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
  }

  private void insertInBatch() throws IoTDBSessionException {
    String deviceId = "root.sg1.d2";
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

  private void insert() throws IoTDBSessionException {
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

  private void insertRowBatchTest1(String deviceId) throws IoTDBSessionException {
    Schema schema = new Schema();
    schema.registerTimeseries(new Path(deviceId, "s1"), new TimeseriesSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path(deviceId, "s2"), new TimeseriesSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schema.registerTimeseries(new Path(deviceId, "s3"), new TimeseriesSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    RowBatch rowBatch = schema.createRowBatch(deviceId, 100);

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

  private void deleteData() throws IoTDBSessionException {
    String path1 = "root.sg1.d1.s1";
    String path2 = "root.sg1.d1.s2";
    String path3 = "root.sg1.d1.s3";
    long deleteTime = 100;

    List<String> paths = new ArrayList<>();
    paths.add(path1);
    paths.add(path2);
    paths.add(path3);
    session.deleteData(paths, deleteTime);
  }

  private void deleteTimeseries() throws IoTDBSessionException {
    session.deleteTimeseries("root.sg1.d1.s1");
  }

  private void query() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s1\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n"
            + "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select * from root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1) + "\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    }
  }

  private void query2() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n"
            + "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select * from root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1) + "\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    }
  }

  public void deleteStorageGroupTest()
      throws ClassNotFoundException, SQLException, IoTDBSessionException {
    try {
      session.deleteStorageGroup("root.sg1.d1.s1");
    } catch (IoTDBSessionException e) {
      assertEquals("The path root.sg1.d1.s1 is not a deletable storage group", e.getMessage());
    }
    session.deleteStorageGroup("root.sg1");
    File folder = new File("data/system/storage_groups/root.sg1/");
    assertEquals(folder.exists(), false);
    session.setStorageGroup("root.sg1.d1");
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    // using the query result as the QueryTest to verify the deletion and the new insertion
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard = "Time\n" + "root.sg1.d1.s1\n" + "root.sg2.d1.s1\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select * from root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1) + "\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
      List<String> storageGroups = new ArrayList<>();
      storageGroups.add("root.sg1.d1");
      storageGroups.add("root.sg2");
      session.deleteStorageGroups(storageGroups);
    }
  }

  private void query4() throws TException, IoTDBRPCException, SQLException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1.d2");
    sessionDataSet.setBatchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      long index = 1;
      count++;
      for (Field f : sessionDataSet.next().getFields()) {
        Assert.assertEquals(f.getLongV(), index);
        index++;
      }
    }
    Assert.assertEquals(500, count);
    sessionDataSet.closeOperationHandle();
  }


  private void query3() throws TException, IoTDBRPCException, SQLException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1.d1");
    sessionDataSet.setBatchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      long index = 1;
      count++;
      for (Field f : sessionDataSet.next().getFields()) {
        Assert.assertEquals(f.getLongV(), index);
        index++;
      }
    }
    Assert.assertEquals(101, count);
    sessionDataSet.closeOperationHandle();
  }


  private void insert_via_sql() throws TException, IoTDBRPCException {
    session.executeNonQueryStatement(
        "insert into root.sg1.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");
  }

  @Test
  public void checkPathTest()
      throws ClassNotFoundException, SQLException, IoTDBSessionException, TException, IoTDBRPCException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    //test set sg
    checkSetSG(session, "root.vehicle", true);
    checkSetSG(session, "root.123456", true);
    checkSetSG(session, "root._1234", true);
    checkSetSG(session, "root._vehicle", true);
    checkSetSG(session, "root.\tvehicle", false);
    checkSetSG(session, "root.\nvehicle", false);
    checkSetSG(session, "root..vehicle", false);
    checkSetSG(session, "root.1234a4", false);
    checkSetSG(session, "root.+12345", true);
    checkSetSG(session, "root.-12345", true);
    checkSetSG(session, "root.%12345", false);
    checkSetSG(session, "root.a{12345}", false);

    //test create timeseries
    checkCreateTimeseries(session, "root.vehicle.d0.s0", true);
    checkCreateTimeseries(session, "root.vehicle.1110.s0", true);
    checkCreateTimeseries(session, "root.vehicle.d0.1220", true);
    checkCreateTimeseries(session, "root.vehicle._1234.s0", true);
    checkCreateTimeseries(session, "root.vehicle.+1245.-1256", true);
    checkCreateTimeseries(session, "root.vehicle./d0.s0", false);
    checkCreateTimeseries(session, "root.vehicle.d\t0.s0", false);
    checkCreateTimeseries(session, "root.vehicle.!d\t0.s0", false);
    checkCreateTimeseries(session, "root.vehicle.d{dfewrew0}.s0", false);

    session.close();
  }

  private void checkSetSG(Session session, String sg, boolean correctStatus) {
    boolean status = true;
    try {
      session.setStorageGroup(sg);
    } catch (IoTDBSessionException e) {
      status = false;
    }
    assertEquals(status, correctStatus);
  }

  private void checkCreateTimeseries(Session session, String timeseris, boolean correctStatus) {
    boolean status = true;
    try {
      session.createTimeseries(timeseris, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    } catch (IoTDBSessionException e) {
      status = false;
    }
    assertEquals(status, correctStatus);
  }
}
