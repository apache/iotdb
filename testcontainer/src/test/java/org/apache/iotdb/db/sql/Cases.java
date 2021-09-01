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
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

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

  // if we seperate the test into multiply test() methods, then the docker container have to be
  // built
  // several times. So, if the test cases are not conflict, we can put them into one method.
  // but if you want to avoid other cases' impact, use a seperate test() method.
  @Test
  public void multiCasesTest() throws SQLException {

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
      session.executeNonQueryStatement(" delete from root.ln.wf011.*");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      Assert.assertFalse(e instanceof BatchExecutionException);
    }

    // test dictionary encoding
    writeStatement.execute(
            "create timeseries root.ln.wf01.wt02.city WITH DATATYPE=TEXT, ENCODING=DICTIONARY");
    initDataArray =
            new String[] {
                    "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(250, \"Nanjing\")",
                    "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(300, \"Nanjing\")",
                    "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(350, \"Singapore\")",
                    "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(400, \"Shanghai\")"
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
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1266
  @Test
  public void showTimeseriesRowsTest() throws SQLException {

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
  }

  @Test
  public void clusterLastQueryTest() throws IoTDBConnectionException, StatementExecutionException {

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
  public void testCreateFunctionPlan1() {
    try {
      PhysicalPlan plan =
              processor.parseSQLToPhysicalPlan(
                      "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        Assert.fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      Assert.assertEquals("udf", createFunctionPlan.getUdfName());
      Assert.assertEquals(
              "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      Assert.assertFalse(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testCreateFunctionPlan2() { // create temporary function
    try {
      PhysicalPlan plan =
              processor.parseSQLToPhysicalPlan(
                      "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        Assert.fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      Assert.assertEquals("udf", createFunctionPlan.getUdfName());
      Assert.assertEquals(
              "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      Assert.assertTrue(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testDropFunctionPlan() { // drop function
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
    String sql = "SHOW FUNCTIONS";

    ShowFunctionsPlan plan = (ShowFunctionsPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
    Assert.assertEquals(ShowPlan.ShowContentType.FUNCTIONS, plan.getShowContentType());
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1407
  @Test
  public void showTimeseriesTagsTest() throws SQLException {
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
              readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01 where tag1=v1");
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
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES root where tag1=v1");
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
  public void testAutoCreateSchemaInClusterMode()
          throws IoTDBConnectionException, StatementExecutionException, SQLException {
    List<String> measurement_list = new ArrayList<>();
    measurement_list.add("s1");
    measurement_list.add("s2");
    measurement_list.add("s3");

    List<TSDataType> type_list = new ArrayList<>();
    type_list.add(TSDataType.INT64);
    type_list.add(TSDataType.INT64);
    type_list.add(TSDataType.INT64);

    List<Object> value_list = new ArrayList<>();
    value_list.add(1L);
    value_list.add(2L);
    value_list.add(3L);

    for (int i = 0; i < 10; i++) {
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
    for (int i = 0; i < 10; i++) {
      for (long t = 0; t < 3; t++) {
        session.insertRecord(
                String.format("root.sg%s.d1", i), t, measurement_list, type_list, 1L, 2L, 3L);
      }
    }

    List<List<String>> measurements_list = new ArrayList<>();
    List<List<Object>> values_list = new ArrayList<>();
    List<List<TSDataType>> types_List = new ArrayList<>();
    List<String> device_list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String device_path = String.format("root.sg%s.d2", i);
      device_list.add(device_path);
      types_List.add(type_list);
      measurements_list.add(measurement_list);
      values_list.add(value_list);
    }

    for (long t = 0; t < 3; t++) {
      List<Long> time_list = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        time_list.add(t);
      }
      session.insertRecords(device_list, time_list, measurements_list, types_List, values_list);
    }

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Map<String, Tablet> tabletMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
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
    for (int i = 10; i < 20; i++) {
      for (long t = 0; t < 3; t++) {
        session.insertRecord(
                String.format("root.sg%s.d1", i), t, measurement_list, type_list, 1L, 2L, 3L);
      }
    }

    device_list.clear();
    for (int i = 10; i < 20; i++) {
      String device_path = String.format("root.sg%s.d2", i);
      device_list.add(device_path);
    }

    for (long t = 0; t < 3; t++) {
      List<Long> time_list = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        time_list.add(t);
      }
      session.insertRecords(device_list, time_list, measurements_list, types_List, values_list);
    }

    tabletMap.clear();
    for (int i = 10; i < 20; i++) {
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

    for (Statement readStatement : readStatements) {
      for (int i = 0; i < 20; i++) {
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
    }
  }
}
