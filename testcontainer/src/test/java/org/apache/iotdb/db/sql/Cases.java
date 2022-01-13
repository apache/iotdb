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
package org.apache.iotdb.db.sql;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowFunctionsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class Cases {

  protected Statement writeStatement;
  protected Connection writeConnection;
  protected Statement[] readStatements;
  protected Connection[] readConnections;
  protected Session session;
  private final Planner processor = new Planner();

  /** initialize the writeStatement,writeConnection, readStatements and the readConnections. */
  public abstract void init() throws Exception;

  public void clean() throws Exception {
    writeStatement.close();
    writeConnection.close();
    for (Statement statement : readStatements) {
      statement.close();
    }
    for (Connection connection : readConnections) {
      connection.close();
    }
    session.close();
  }

  protected Connection openConnection(String hostname, int port) throws SQLException {
    for (int i = 0; i < 20; i++) {
      try {
        return DriverManager.getConnection("jdbc:iotdb://" + hostname + ":" + port, "root", "root");
      } catch (SQLException e) {
        if (!(e.getCause().getMessage().contains("closed by peer"))) {
          throw e;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    throw new SQLException("Cannot connect to " + hostname + ":" + port);
  }

  // if we seperate the test into multiply test() methods, then the docker container have to be
  // built
  // several times. So, if the test cases are not conflict, we can put them into one method.
  // but if you want to avoid other cases' impact, use a seperate test() method.
  @Test
  public void multiCasesTest() throws SQLException {
    System.out.println("Testing multiCasesTest");
    String[] timeSeriesArray = {"root.sg1.aa.bb", "root.sg1.aa.bb.cc", "root.sg1.aa"};

    for (String timeSeries : timeSeriesArray) {
      try {
        writeStatement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                timeSeries));
      } catch (Exception e) {
        if (timeSeries.equals("root.sg1.aa.bb")) {
          e.printStackTrace();
          fail();
        }
      }
    }
    ResultSet resultSet = null;
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("show timeseries");
      Set<String> result = new HashSet<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      Assert.assertEquals(1, result.size());
      Assert.assertTrue(result.contains("root.sg1.aa.bb"));
      Assert.assertFalse(result.contains("root.sg1.aa.bb.cc"));
      Assert.assertFalse(result.contains("root.sg1.aa"));
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1331
    writeStatement.execute(
        "create timeseries root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    String[] initDataArray = {
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(200,20.71)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(220,50.71)"
    };
    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select avg(temperature) from root.ln.wf01.wt01");
      if (resultSet.next()) {
        Assert.assertEquals(35.71, resultSet.getDouble(1), 0.01);
      } else {
        fail("expect 1 result, but get an empty resultSet.");
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1348
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(250, 10.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(300, 20.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(350, 25.0)"
        };

    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select last * from root.ln.wf01.wt01;");
      Assert.assertTrue(resultSet.next());
      double last = Double.parseDouble(resultSet.getString("value"));
      Assert.assertEquals(25.0, last, 0.1);
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1457
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf011.wt0110(timestamp, temperature) values(250, 10.0)",
          "INSERT INTO root.ln.wf011.wt0111(timestamp, temperature) values(300, 20.0)",
          "INSERT INTO root.ln.wf011.wt0112(timestamp, temperature) values(350, 25.0)"
        };

    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    try {
      session.executeNonQueryStatement(" delete from root.ln.wf011.**");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      Assert.assertFalse(e instanceof BatchExecutionException);
    }

    // test dictionary encoding
    writeStatement.execute(
        "create timeseries root.ln.wf01.wt02.city WITH DATATYPE=TEXT, ENCODING=DICTIONARY");
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(250, 'Nanjing')",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(300, 'Nanjing')",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(350, 'Singapore')",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(400, 'Shanghai')"
        };
    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }

    String[] results = new String[] {"Nanjing", "Nanjing", "Singapore", "Shanghai"};
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select * from root.ln.wf01.wt02");
      int i = 0;
      while (resultSet.next()) {
        Assert.assertEquals(results[i++], resultSet.getString("root.ln.wf01.wt02.city"));
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1600
    try {
      // Target data of device "root.ln.wf01.d_1600"
      // Time s1   s2   s3
      // 1000 1.0  2.0  null
      // 2000 null 3.0  4.0
      // 3000 5.0  6.0  7.0
      String testDevice = "root.ln.wf01.d_1600";
      session.createTimeseries(
          testDevice + ".s1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
      session.createTimeseries(
          testDevice + ".s2", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
      session.createTimeseries(
          testDevice + ".s3", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
      List<Long> insertTimes = Arrays.asList(1000L, 2000L, 3000L);
      List<List<String>> insertedMeasurements =
          Arrays.asList(
              Arrays.asList("s1", "s2"),
              Arrays.asList("s2", "s3"),
              Arrays.asList("s1", "s2", "s3"));
      List<List<TSDataType>> insertedDataTypes =
          Arrays.asList(
              Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
              Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE),
              Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE));
      List<List<Object>> insertedValues =
          Arrays.asList(
              Arrays.asList(1.0D, 2.0D),
              Arrays.asList(3.0D, 4.0D),
              Arrays.asList(5.0D, 6.0D, 7.0D));
      session.insertRecordsOfOneDevice(
          testDevice, insertTimes, insertedMeasurements, insertedDataTypes, insertedValues);
      final double E = 0.00001;
      for (Statement readStatement : readStatements) {
        resultSet = readStatement.executeQuery("select s1, s2, s3 from " + testDevice);
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1000L, resultSet.getLong("Time"));
        Assert.assertEquals(1.0D, resultSet.getDouble(testDevice + ".s1"), E);
        Assert.assertEquals(2.0D, resultSet.getDouble(testDevice + ".s2"), E);
        Assert.assertNull(resultSet.getObject(testDevice + ".s3"));

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2000L, resultSet.getLong("Time"));
        Assert.assertNull(resultSet.getObject(testDevice + ".s1"));
        Assert.assertEquals(3.0D, resultSet.getDouble(testDevice + ".s2"), E);
        Assert.assertEquals(4.0D, resultSet.getDouble(testDevice + ".s3"), E);

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(3000L, resultSet.getLong("Time"));
        Assert.assertEquals(5.0D, resultSet.getDouble(testDevice + ".s1"), E);
        Assert.assertEquals(6.0D, resultSet.getDouble(testDevice + ".s2"), E);
        Assert.assertEquals(7.0D, resultSet.getDouble(testDevice + ".s3"), E);

        Assert.assertFalse(resultSet.next());
        resultSet.close();
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail();
    }
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1266
  @Test
  public void showTimeseriesRowsTest() throws SQLException {
    System.out.println("Testing showTimeseriesRowsTest");
    int n = 3000;
    String timeSeriesPrefix = "root.ln.wf01.wt";
    String timeSeriesSuffix = ".temperature WITH DATATYPE=DOUBLE, ENCODING=RLE";
    String timeSeries;
    for (int i = 0; i < n; i++) {
      timeSeries = timeSeriesPrefix + String.valueOf(i) + timeSeriesSuffix;
      writeStatement.execute(String.format("create timeseries %s ", timeSeries));
    }

    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(n, cnt);
      resultSet.close();
    }

    // try to get devices on each node;
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("COUNT DEVICES");
      while (resultSet.next()) {
        assertEquals(n, resultSet.getInt(1));
      }
    }
  }

  @Test
  public void clusterLastQueryTest() throws IoTDBConnectionException, StatementExecutionException {
    System.out.println("Testing clusterLastQueryTest");
    session.setStorageGroup("root.sg1");
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    insertRecords();

    List<String> paths = new ArrayList<>();

    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d2.s1");

    SessionDataSet sessionDataSet = session.executeLastDataQuery(paths);
    sessionDataSet.setFetchSize(1024);

    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      List<Field> fields = sessionDataSet.next().getFields();
      Assert.assertEquals("[root.sg1.d1.s1,1,INT64]", fields.toString().replace(" ", ""));
    }
    Assert.assertEquals(1, count);
    sessionDataSet.closeOperationHandle();
  }

  private void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
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

  @Test
  public void testCreateFunctionPlan() {
    System.out.println("Testing testCreateFunctionPlan");
    try {
      PhysicalPlan plan =
          processor.parseSQLToPhysicalPlan(
              "create function udf as 'org.apache.iotdb.db.query.udf.example.Adder'");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        Assert.fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      Assert.assertEquals("udf", createFunctionPlan.getUdfName());
      Assert.assertEquals(
          "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testDropFunctionPlan() { // drop function
    System.out.println("Testing testDropFunctionPlan");
    try {
      DropFunctionPlan dropFunctionPlan =
          (DropFunctionPlan) processor.parseSQLToPhysicalPlan("drop function udf");
      Assert.assertEquals("udf", dropFunctionPlan.getUdfName());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testShowFunction() throws QueryProcessException {
    System.out.println("Testing testShowFunction");
    String sql = "SHOW FUNCTIONS";

    ShowFunctionsPlan plan = (ShowFunctionsPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
    Assert.assertEquals(ShowPlan.ShowContentType.FUNCTIONS, plan.getShowContentType());
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1407
  @Test
  public void showTimeseriesTagsTest() throws SQLException {
    System.out.println("Testing showTimeseriesTagsTest");
    String createTimeSeries1 =
        "create timeseries root.ln.wf01.wt1 WITH DATATYPE=DOUBLE, ENCODING=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)";
    String createTimeSeries2 =
        "create timeseries root.ln.wf01.wt2 WITH DATATYPE=DOUBLE, ENCODING=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)";
    writeStatement.execute(createTimeSeries1);
    writeStatement.execute(createTimeSeries2);
    // try to read data on each node. select .*
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from parent series
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from one series
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt1 where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from root
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES root.** where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. SHOW TIMESERIES root.ln.wf01.* where tag1=v3"
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag1=v3");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(0, cnt);
      resultSet.close();
    }

    // try to read data on each node. SHOW TIMESERIES root.ln.wf01.* where tag3=v1"
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = null;
      try {
        resultSet = readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag3=v1");
        fail("Exception expected for \"The key tag3 is not a tag\"");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("The key tag3 is not a tag"));
      } finally {
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void clusterUDTFQueryTest() throws SQLException {
    System.out.println("Testing clusterUDTFQueryTest");
    // Prepare data.
    writeStatement.execute(
        "CREATE timeseries root.sg.d.s WITH datatype=DOUBLE, encoding=RLE, compression=SNAPPY");
    for (int i = 10; i < 20; i++) {
      writeStatement.execute(
          String.format("INSERT INTO root.sg.d(timestamp,s) VALUES(%s,%s)", i, i));
    }
    for (int i = 0; i < 10; i++) {
      writeStatement.execute(
          String.format("INSERT INTO root.sg.d(timestamp,s) VALUES(%s,%s)", i, i));
    }

    ResultSet resultSet = null;

    // Try to execute udf query on each node.
    // Without time filter
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("SELECT sin(s) FROM root.sg.d");

      double i = 0;
      while (resultSet.next()) {
        Assert.assertEquals(Math.sin(i++), resultSet.getDouble(2), 0.00001);
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }

    // With time filter
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("SELECT sin(s) FROM root.sg.d WHERE time >= 5");

      double i = 5;
      while (resultSet.next()) {
        Assert.assertEquals(Math.sin(i++), resultSet.getDouble(2), 0.00001);
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }
  }

  @Test
  public void testSelectInto() throws SQLException {
    System.out.println("Testing testSelectInto");
    for (int i = 0; i < 10; i++) {
      writeStatement.execute(
          String.format(
              "CREATE timeseries root.sg.device%s.s WITH datatype=DOUBLE, encoding=RLE, compression=SNAPPY",
              i));
      writeStatement.execute(
          String.format(
              "CREATE timeseries root.sg.device%s.t WITH datatype=DOUBLE, encoding=RLE, compression=SNAPPY",
              i));
      writeStatement.execute(
          String.format("INSERT INTO root.sg.device%s(timestamp,s) VALUES(1,1)", i));
    }

    writeStatement.execute(
        "SELECT device0.s, device1.s, device2.s, device3.s, device4.s, device5.s, device6.s, device7.s, device8.s, device9.s "
            + "INTO device0.t, device1.t, device2.t, device3.t, device4.t, device5.t, device6.t, device7.t, device8.t, device9.t "
            + "FROM root.sg;");

    for (int i = 0; i < 10; i++) {
      writeStatement.execute(
          String.format("INSERT INTO root.sg.device%s(timestamp,s) VALUES(2,2)", i));
      writeStatement.execute(
          String.format("SELECT device%s.s into device%s.t from root.sg;", i, i));
    }

    for (Statement readStatement : readStatements) {
      for (int i = 0; i < 10; ++i) {
        try (ResultSet resultSet =
            readStatement.executeQuery(String.format("SELECT s, t FROM root.sg.device%s", i))) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(1, Double.parseDouble(resultSet.getString(1)), 0);
          Assert.assertEquals(
              Double.parseDouble(resultSet.getString(1)),
              Double.parseDouble(resultSet.getString(2)),
              0);
          Assert.assertEquals(
              Double.parseDouble(resultSet.getString(2)),
              Double.parseDouble(resultSet.getString(3)),
              0);

          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(2, Double.parseDouble(resultSet.getString(1)), 0);
          Assert.assertEquals(
              Double.parseDouble(resultSet.getString(1)),
              Double.parseDouble(resultSet.getString(2)),
              0);
          Assert.assertEquals(
              Double.parseDouble(resultSet.getString(2)),
              Double.parseDouble(resultSet.getString(3)),
              0);

          Assert.assertFalse(resultSet.next());
        }
      }
    }
  }

  @Test
  public void SetSystemReadOnlyWritableTest() throws SQLException {
    System.out.println("Testing SetSystemReadOnlyWritableTest");
    String setReadOnly = "SET SYSTEM TO READONLY";
    String createTimeSeries =
        "create timeseries root.ln.wf01.wt1 WITH DATATYPE=DOUBLE, ENCODING=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)";
    String setWritable = "SET SYSTEM TO WRITABLE";

    writeStatement.execute(setReadOnly);

    try {
      writeStatement.execute(createTimeSeries);
    } catch (Exception e) {
      System.out.println(e.getClass().getName() + ":" + e.getMessage());
      Assert.assertTrue(
          e.getMessage()
              .contains("Database is read-only, and does not accept non-query operation now"));
    }

    writeStatement.execute(setWritable);
  }

  @Test
  public void testAutoCreateSchemaInClusterMode()
      throws IoTDBConnectionException, StatementExecutionException, SQLException {
    System.out.println("Testing testAutoCreateSchemaInClusterMode");
    List<String> measurementList = new ArrayList<>();
    measurementList.add("s1");
    measurementList.add("s2");
    measurementList.add("s3");

    List<TSDataType> typeList = new ArrayList<>();
    typeList.add(TSDataType.INT64);
    typeList.add(TSDataType.INT64);
    typeList.add(TSDataType.INT64);

    List<Object> valueList = new ArrayList<>();
    valueList.add(1L);
    valueList.add(2L);
    valueList.add(3L);

    for (int i = 0; i < 5; i++) {
      String sg = "root.sg" + String.valueOf(i);
      session.setStorageGroup(sg);
      for (int j = 0; j < 10; j++) {
        session.createTimeseries(
            String.format("%s.d1.s%s", sg, j),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.SNAPPY);
        session.createTimeseries(
            String.format("%s.d2.s%s", sg, j),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.SNAPPY);
        session.createTimeseries(
            String.format("%s.d3.s%s", sg, j),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.SNAPPY);
        session.createTimeseries(
            String.format("%s.d4.s%s", sg, j),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.SNAPPY);
      }
    }

    // step 1: insert into existing time series.
    for (int i = 0; i < 5; i++) {
      for (long t = 0; t < 3; t++) {
        session.insertRecord(
            String.format("root.sg%s.d1", i), t, measurementList, typeList, 1L, 2L, 3L);
      }
    }

    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<String> deviceList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String devicePath = String.format("root.sg%s.d2", i);
      deviceList.add(devicePath);
      typesList.add(typeList);
      measurementsList.add(measurementList);
      valuesList.add(valueList);
    }

    for (long t = 0; t < 3; t++) {
      List<Long> timeList = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        timeList.add(t);
      }
      session.insertRecords(deviceList, timeList, measurementsList, typesList, valuesList);
    }

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Map<String, Tablet> tabletMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      Tablet tablet = new Tablet(String.format("root.sg%s.d3", i), schemaList, 10);
      for (long row = 0; row < 3; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 2L);
        tablet.addValue("s3", rowIndex, 3L);
      }
      session.insertTablet(tablet);
      tablet.setPrefixPath(String.format("root.sg%s.d4", i));
      tabletMap.put(String.format("root.sg%s.d4", i), tablet);
    }

    session.insertTablets(tabletMap);

    // step 2: test auto create sg and time series schema
    for (int i = 5; i < 10; i++) {
      for (long t = 0; t < 3; t++) {
        session.insertRecord(
            String.format("root.sg%s.d1", i), t, measurementList, typeList, 1L, 2L, 3L);
      }
    }

    deviceList.clear();
    for (int i = 5; i < 10; i++) {
      String device_path = String.format("root.sg%s.d2", i);
      deviceList.add(device_path);
    }

    for (long t = 0; t < 3; t++) {
      List<Long> timeList = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        timeList.add(t);
      }
      session.insertRecords(deviceList, timeList, measurementsList, typesList, valuesList);
    }

    tabletMap.clear();
    for (int i = 5; i < 10; i++) {
      Tablet tablet = new Tablet(String.format("root.sg%s.d3", i), schemaList, 10);
      for (long row = 0; row < 3; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, row);
        tablet.addValue("s1", rowIndex, 1L);
        tablet.addValue("s2", rowIndex, 2L);
        tablet.addValue("s3", rowIndex, 3L);
      }
      session.insertTablet(tablet);
      tablet.setPrefixPath(String.format("root.sg%s.d4", i));
      tabletMap.put(String.format("root.sg%s.d4", i), tablet);
    }

    session.insertTablets(tabletMap);

    measurementsList.clear();
    List<Long> timeList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      timeList.add((long) i);
      List<String> measurements = new ArrayList<>();
      measurements.add(String.format("s%d", i));
      measurements.add(String.format("s%d", i + 5));
      measurements.add(String.format("s%d", i + 10));
      measurementsList.add(measurements);
    }

    session.insertRecordsOfOneDevice(
        "root.sg0.d5", timeList, measurementsList, typesList, valuesList);
    session.insertRecordsOfOneDevice(
        "root.sg20.d1", timeList, measurementsList, typesList, valuesList);

    for (Statement readStatement : readStatements) {
      for (int i = 0; i < 10; i++) {
        for (int d = 1; d <= 4; d++) {
          ResultSet resultSet =
              readStatement.executeQuery(String.format("SELECT s1,s2,s3 from root.sg%s.d%s", i, d));
          for (long t = 0; t < 3; t++) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getLong(1), t);
            Assert.assertEquals(resultSet.getString(2), "1");
            Assert.assertEquals(resultSet.getString(3), "2");
            Assert.assertEquals(resultSet.getString(4), "3");
          }
        }
      }

      for (int i = 0; i < 5; i++) {
        ResultSet resultSet =
            readStatement.executeQuery(
                String.format("select s%d,s%d,s%d from root.sg0.d5", i, i + 5, i + 10));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getLong(1), i);
        Assert.assertEquals(resultSet.getString(2), "1");
        Assert.assertEquals(resultSet.getString(3), "2");
        Assert.assertEquals(resultSet.getString(4), "3");

        resultSet =
            readStatement.executeQuery(
                String.format("select s%d,s%d,s%d from root.sg20.d1", i, i + 5, i + 10));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getLong(1), i);
        Assert.assertEquals(resultSet.getString(2), "1");
        Assert.assertEquals(resultSet.getString(3), "2");
        Assert.assertEquals(resultSet.getString(4), "3");
      }
    }

    // test create time series
    for (int i = 0; i < 5; i++) {
      session.createTimeseries(
          String.format("root.sg1%s.d1.s1", i),
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY);
    }

    List<String> path = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 5; i < 10; i++) {
      path.add(String.format("root.sg1%s.d1.s1", i));
      dataTypes.add(TSDataType.INT64);
      encodings.add(TSEncoding.RLE);
      compressionTypes.add(CompressionType.SNAPPY);
    }
    session.createMultiTimeseries(
        path, dataTypes, encodings, compressionTypes, null, null, null, null);
    for (Statement readStatement : readStatements) {
      for (int i = 0; i < 10; i++) {
        ResultSet resultSet =
            readStatement.executeQuery(String.format("show timeseries root.sg1%s.d1.s1", i));
        Assert.assertTrue(resultSet.next());
      }
    }
  }

  @Ignore("Aligned timeseries not supported yet")
  @Test
  public void testAutoCreateSchemaForAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException, SQLException {
    System.out.println("Testing testAutoCreateSchemaForAlignedTimeseries");
    List<String> multiMeasurementComponents = new ArrayList<>();
    multiMeasurementComponents.add("s1");
    multiMeasurementComponents.add("s2");
    multiMeasurementComponents.add("s3");

    List<TSDataType> types = new ArrayList<>();
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT32);
    types.add(TSDataType.FLOAT);

    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(2);
    values.add(3.0f);

    List<Long> times = new ArrayList<>();
    times.add(1L);
    times.add(2L);
    times.add(3L);

    session.setStorageGroup(String.format("root.sg0"));

    for (long time = 1; time <= 3; time++) {
      session.insertAlignedRecord(
          "root.sg1.d1.v1", time, multiMeasurementComponents, types, values);
    }
    List<String> multiSeriesIds = new ArrayList<>();
    List<List<String>> multiMeasurementComponentsList = new ArrayList<>();
    List<List<TSDataType>> typeList = new ArrayList<>();
    List<List<Object>> valueList = new ArrayList<>();

    for (int i = 2; i <= 4; i++) {
      multiMeasurementComponentsList.add(multiMeasurementComponents);
      typeList.add(types);
      valueList.add(values);
      multiSeriesIds.add(String.format("root.sg%d.d1.v1", i));
    }
    for (long time = 1; time <= 3; time++) {
      List<Long> tmp_times = new ArrayList<>();
      tmp_times.add(time);
      tmp_times.add(time);
      tmp_times.add(time);
      session.insertAlignedRecords(
          multiSeriesIds, tmp_times, multiMeasurementComponentsList, typeList, valueList);
    }
    multiSeriesIds.clear();
    multiSeriesIds.add("root.sg0.d2.v1");
    multiSeriesIds.add("root.sg0.d2.v1");
    multiSeriesIds.add("root.sg0.d2.v1");

    session.insertAlignedRecordsOfOneDevice(
        "root.sg5.d1.v1", times, multiMeasurementComponentsList, typeList, valueList);

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT32));
    schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));

    Tablet tablet = new Tablet("root.sg6.d1.v1", schemaList);
    tablet.setAligned(true);

    for (long row = 1; row <= 3; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, row);
      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, 1L);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, 2);
      tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, 3.0f);
    }
    session.insertTablet(tablet, true);
    tablet.reset();

    List<MeasurementSchema> schemaList1 = new ArrayList<>();
    schemaList1.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList1.add(new MeasurementSchema("s2", TSDataType.INT32));
    schemaList1.add(new MeasurementSchema("s3", TSDataType.FLOAT));
    List<MeasurementSchema> schemaList2 = new ArrayList<>();
    schemaList2.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList2.add(new MeasurementSchema("s2", TSDataType.INT32));
    schemaList2.add(new MeasurementSchema("s3", TSDataType.FLOAT));
    List<MeasurementSchema> schemaList3 = new ArrayList<>();
    schemaList3.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList3.add(new MeasurementSchema("s2", TSDataType.INT32));
    schemaList3.add(new MeasurementSchema("s3", TSDataType.FLOAT));

    Tablet tablet1 = new Tablet("root.sg7.d1.v1", schemaList1, 100);
    Tablet tablet2 = new Tablet("root.sg8.d1.v1", schemaList2, 100);
    Tablet tablet3 = new Tablet("root.sg9.d1.v1", schemaList3, 100);
    tablet1.setAligned(true);
    tablet2.setAligned(true);
    tablet3.setAligned(true);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put("root.sg7.d1.v1", tablet1);
    tabletMap.put("root.sg8.d1.v1", tablet2);
    tabletMap.put("root.sg9.d1.v1", tablet3);

    for (long row = 1; row <= 3; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, row);
      tablet2.addTimestamp(row2, row);
      tablet3.addTimestamp(row3, row);
      for (int i = 0; i < 3; i++) {
        tablet1.addValue(schemaList1.get(i).getMeasurementId(), row1, values.get(i));
        tablet2.addValue(schemaList2.get(i).getMeasurementId(), row2, values.get(i));
        tablet3.addValue(schemaList3.get(i).getMeasurementId(), row3, values.get(i));
      }
    }
    session.insertTablets(tabletMap, true);

    tabletMap.clear();

    for (Statement readStatement : readStatements) {
      for (int sg = 1; sg <= 9; sg++) {
        ResultSet resultSet =
            readStatement.executeQuery(String.format("SELECT * from root.sg%d.d1.v1", sg));
        for (long t = 1; t <= 3; t++) {
          Assert.assertTrue(resultSet.next());
          Assert.assertEquals(t, resultSet.getLong(1));
          Assert.assertEquals("1", resultSet.getString("root.sg" + sg + ".d1.v1.s1"));
          Assert.assertEquals("2", resultSet.getString("root.sg" + sg + ".d1.v1.s2"));
          Assert.assertEquals("3.0", resultSet.getString("root.sg" + sg + ".d1.v1.s3"));
        }
      }
    }
  }

  @Test
  public void testInsertTabletWithNullValues()
      throws IoTDBConnectionException, StatementExecutionException, SQLException {
    System.out.println("Testing testInsertTabletWithNullValues");
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s0", TSDataType.DOUBLE, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.RLE));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList);
    for (long time = 0; time < 10; time++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, time);

      tablet.addValue(schemaList.get(0).getMeasurementId(), rowIndex, (double) time);
      tablet.addValue(schemaList.get(1).getMeasurementId(), rowIndex, (float) time);
      tablet.addValue(schemaList.get(2).getMeasurementId(), rowIndex, time);
      tablet.addValue(schemaList.get(3).getMeasurementId(), rowIndex, (int) time);
      tablet.addValue(schemaList.get(4).getMeasurementId(), rowIndex, time % 2 == 0);
      tablet.addValue(
          schemaList.get(5).getMeasurementId(), rowIndex, new Binary(String.valueOf(time)));
    }

    BitMap[] bitMaps = new BitMap[schemaList.size()];
    for (int i = 0; i < schemaList.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(10);
      }
      bitMaps[i].mark(i);
    }
    tablet.bitMaps = bitMaps;

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    ResultSet resultSet;
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select count(*) from root.sg1.d1");
      Assert.assertTrue(resultSet.next());
      for (int i = 1; i <= schemaList.size(); ++i) {
        Assert.assertEquals(9L, resultSet.getLong(i));
      }
    }
  }

  // --------------------- AS Tests -----------------------------------------
  private static String[] asSqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s3 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg.d1(timestamp,s1,s2) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg.d2(timestamp,s1,s2,s3) values(400, 73.4, 26.3, 83.0)"
      };

  private void prepareAsData() {
    for (String sql : asSqls) {
      try {
        writeStatement.execute(sql);
      } catch (SQLException throwables) {
        fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void alignByDeviceWithAsFailTest() {
    prepareAsData();
    // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
    for (Statement readStatement : readStatements) {
      try {
        readStatement.execute("select * as speed from root.sg.d1 align by device");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("alias speed can only be matched with one time series"));
      }
    }
  }

  @Test
  public void alignByDeviceWithAsAggregationTest() {
    prepareAsData();
    String[] retArray =
        new String[] {
          "root.sg.d2,4,4,4,",
        };

    for (Statement readStatement : readStatements) {
      try {
        boolean hasResultSet =
            readStatement.execute(
                "select count(s1) as s1_num, count(s2), count(s3) as s3_num from root.sg.d2 align by device");
        Assert.assertTrue(hasResultSet);

        try (ResultSet resultSet = readStatement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            header.append(resultSetMetaData.getColumnName(i)).append(",");
          }
          assertEquals("Device,s1_num,count(s2),s3_num,", header.toString());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            assertEquals(retArray[cnt], builder.toString());
            cnt++;
          }
          assertEquals(retArray.length, cnt);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void alignByDeviceWithAsTest() {
    prepareAsData();
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,20.7,",
          "200,root.sg.d1,15.2,22.9,",
          "300,root.sg.d1,30.3,25.1,",
          "400,root.sg.d1,50.4,28.3,"
        };

    for (Statement readStatement : readStatements) {
      try {
        boolean hasResultSet =
            readStatement.execute(
                "select s1 as speed, s2 as temperature from root.sg.d1 align by device");
        Assert.assertTrue(hasResultSet);

        try (ResultSet resultSet = readStatement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            header.append(resultSetMetaData.getColumnName(i)).append(",");
          }
          assertEquals("Time,Device,speed,temperature,", header.toString());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            assertEquals(retArray[cnt], builder.toString());
            cnt++;
          }
          assertEquals(retArray.length, cnt);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void alignByDeviceWithAsMixedTest() {
    prepareAsData();
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,20.7,",
          "200,root.sg.d1,15.2,22.9,",
          "300,root.sg.d1,30.3,25.1,",
          "400,root.sg.d1,50.4,28.3,",
          "100,root.sg.d2,11.1,20.2,",
          "200,root.sg.d2,20.2,21.8,",
          "300,root.sg.d2,45.3,23.4,",
          "400,root.sg.d2,73.4,26.3,"
        };

    for (Statement readStatement : readStatements) {
      try {
        boolean hasResultSet =
            readStatement.execute("select s1 as speed, s2 from root.sg.* align by device");
        Assert.assertTrue(hasResultSet);

        try (ResultSet resultSet = readStatement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            header.append(resultSetMetaData.getColumnName(i)).append(",");
          }
          assertEquals("Time,Device,speed,s2,", header.toString());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            assertEquals(retArray[cnt], builder.toString());
            cnt++;
          }
          assertEquals(retArray.length, cnt);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void alignByDeviceWithAsDuplicatedTest() {
    prepareAsData();
    String[] retArray =
        new String[] {
          "100,root.sg.d1,10.1,10.1,",
          "200,root.sg.d1,15.2,15.2,",
          "300,root.sg.d1,30.3,30.3,",
          "400,root.sg.d1,50.4,50.4,"
        };

    for (Statement readStatement : readStatements) {
      try {
        boolean hasResultSet =
            readStatement.execute("select s1 as speed, s1 from root.sg.d1 align by device");
        Assert.assertTrue(hasResultSet);

        try (ResultSet resultSet = readStatement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          StringBuilder header = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            header.append(resultSetMetaData.getColumnName(i)).append(",");
          }
          assertEquals("Time,Device,speed,speed,", header.toString());

          int cnt = 0;
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i)).append(",");
            }
            assertEquals(retArray[cnt], builder.toString());
            cnt++;
          }
          assertEquals(retArray.length, cnt);
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void deleteStorageGroupTagTest() {
    List<String> ret =
        Collections.singletonList(
            "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
                + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}");

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    try {
      writeStatement.execute(sql);
      for (Statement readStatement : readStatements) {
        boolean hasResult = readStatement.execute("show timeseries");
        assertTrue(hasResult);
        int count = 0;
        try (ResultSet resultSet = readStatement.getResultSet()) {
          while (resultSet.next()) {
            String ans =
                resultSet.getString("timeseries")
                    + ","
                    + resultSet.getString("alias")
                    + ","
                    + resultSet.getString("storage group")
                    + ","
                    + resultSet.getString("dataType")
                    + ","
                    + resultSet.getString("encoding")
                    + ","
                    + resultSet.getString("compression")
                    + ","
                    + resultSet.getString("tags")
                    + ","
                    + resultSet.getString("attributes");
            assertTrue(ret.contains(ans));
            count++;
          }
        }
        assertEquals(ret.size(), count);
      }

      writeStatement.execute("delete storage group root.turbine");

    } catch (Exception e) {
      assertEquals("703: DELETE_STORAGE_GROUP not supported yet", e.getMessage());
    }
  }

  @Test
  public void testEmptyDataSet() throws SQLException {
    try {

      ResultSet resultSet = statement.executeQuery("select * from root.**");
      // has an empty time column
      Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
      try {
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 group by ([0, 100), 20ms, 20ms)");
        // has an empty time column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root");
        // has no column
        Assert.assertEquals(0, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select * from root.** align by device");
        // has time and device columns
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root align by device");
        // has device column
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 "
                    + "group by ([0, 100), 20ms, 20ms) align by device");
        // has time and device columns
        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }
      } finally {
        resultSet.close();
      }

      resultSet.close();
    }
  }
}
