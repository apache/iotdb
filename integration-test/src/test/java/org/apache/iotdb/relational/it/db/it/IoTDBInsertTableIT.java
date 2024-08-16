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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInsertTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getDataNodeCommonConfig()
        .setWriteMemoryProportion("10000000:1");
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists test");
      statement.execute("use test");
      statement.execute(
          "CREATE TABLE sg10(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
      statement.execute(
          "CREATE TABLE sg11(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInsertMultiPartition() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("create table sg1 (id1 string id, s1 int32 measurement)");
      statement.execute("insert into sg1(id1,time,s1) values('d1',1,2)");
      statement.execute("flush");
      statement.execute("insert into sg1(id1,time,s1) values('d1',2,2)");
      statement.execute("insert into sg1(id1,time,s1) values('d1',604800001,2)");
      statement.execute("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertTimeAtAnyIndex() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.addBatch(
          "create table IF NOT EXISTS db2(id1 string id, s1 int32 measurement, s2 int32 measurement)");
      statement.addBatch("insert into db2(id1, s1, s2, time) values ('d1', 2, 3, 1)");
      statement.addBatch("insert into db2(id1, s1, time, s2) values ('d1', 20, 10, 30)");
      statement.addBatch("insert into db2(id1, \"time\", s1, s2) values ('d1', 100, 200, 300)");
      statement.executeBatch();

      try (ResultSet resultSet = statement.executeQuery("select time, s1 from db2")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertEquals(2, resultSet.getInt(2));
        assertTrue(resultSet.next());
        assertEquals(10, resultSet.getLong(1));
        assertEquals(20, resultSet.getInt(2));
        assertTrue(resultSet.next());
        assertEquals(100, resultSet.getLong(1));
        assertEquals(200, resultSet.getInt(2));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testInsertMultiTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.addBatch("use \"test\"");
        statement.addBatch(
            "create table t3(id1 string id, s1 int32 measurement, s2 int32 measurement)");
        statement.addBatch("insert into t3(id1, s1, s2, time, time) values ('d1', 2, 3, 1, 1)");
        statement.executeBatch();
        fail();
      } catch (SQLException e) {
        // expected
      }

    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testPartialInsertionAllFailed() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("USE \"test\"");
        statement.execute("create table sg4 (id1 string id, s0 int32 measurement)");
        statement.execute("INSERT INTO sg4(id1, timestamp, s0) VALUES ('id', 1, 1)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Unknown column category"));
      }
    }
  }

  @Test
  public void testPartialInsertionRestart() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");
      statement.execute("SET CONFIGURATION enable_auto_create_schema=false");
      statement.execute(
          "create table sg5 (id1 string id, s1 text measurement, s2 double measurement)");

      try {
        statement.execute("INSERT INTO sg5(id1,time,s1,s2) VALUES('d1', 100,'test','test')");
      } catch (SQLException e) {
        // ignore
      }
    } finally {
      try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("SET CONFIGURATION enable_auto_create_schema=true");
      }
    }

    // TODO: replace restartDaemon() with new methods in Env.
    /*
    long time = 0;
    try {
      EnvironmentUtils.restartDaemon();
      StorageEngine.getInstance().recover();
      // wait for recover
      while (!StorageEngine.getInstance().isAllSgReady()) {
        Thread.sleep(500);
        time += 500;
        if (time > 10000) {
          logger.warn("wait too long in restart, wait for: " + time / 1000 + "s");
        }
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
     */

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");

      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM sg5")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
          assertEquals("test", resultSet.getString("s1"));
        }
        assertEquals(1, cnt);
      }
      try (ResultSet resultSet = statement.executeQuery("SELECT s2 FROM sg5")) {
        assertNotNull(resultSet);
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testPartialInsertTablet() {
    try (ISession session = EnvFactory.getEnv().getSessionConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      session.executeNonQueryStatement("use \"test\"");
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema=false");
      session.executeNonQueryStatement(
          "create table sg6 (id1 string id, s1 int64 measurement, s2 int64 measurement)");
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("id1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
      final List<Tablet.ColumnType> columnTypes =
          Arrays.asList(
              Tablet.ColumnType.ID,
              Tablet.ColumnType.MEASUREMENT,
              Tablet.ColumnType.MEASUREMENT,
              Tablet.ColumnType.MEASUREMENT);
      Tablet tablet = new Tablet("sg6", schemaList, columnTypes, 300);
      long timestamp = 0;
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
          }
        }
        timestamp++;
      }
      timestamp = System.currentTimeMillis();
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.rowSize++;
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
          }
        }
        timestamp++;
      }
      try {
        session.insertRelationalTablet(tablet);
      } catch (Exception e) {
        if (!e.getMessage().contains("507")) {
          fail(e.getMessage());
        }
      } finally {
        session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema=false");
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM sg6")) {
        assertEquals(dataSet.getColumnNames().size(), 4);
        assertEquals(dataSet.getColumnNames().get(0), "time");
        assertEquals(dataSet.getColumnNames().get(1), "id1");
        assertEquals(dataSet.getColumnNames().get(2), "s1");
        assertEquals(dataSet.getColumnNames().get(3), "s2");
        int cnt = 0;
        while (dataSet.hasNext()) {
          RowRecord rowRecord = dataSet.next();
          long time = rowRecord.getFields().get(0).getLongV();
          assertEquals(time, rowRecord.getFields().get(2).getLongV());
          assertEquals(time, rowRecord.getFields().get(3).getLongV());
          cnt++;
        }
        Assert.assertEquals(200, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertNull() {
    String[] retArray =
        new String[] {
          "1,d2,null,1.0,1,", "2,d2,true,null,2,", "3,d2,true,3.0,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute(
          "CREATE TABLE sg7 (id1 string id, s1 boolean measurement, s2 float measurement, s3 int32 measurement)");
      statement.execute("insert into sg7(id1,time,s1,s2,s3) values('d2',1,null,1.0,1)");
      statement.execute("insert into sg7(id1,time,s1,s2,s3) values('d2',2,true,null,2)");
      statement.execute("insert into sg7(id1,time,s1,s2,s3) values('d2',3,true,3.0,null)");

      try (ResultSet resultSet = statement.executeQuery("select * from sg7")) {
        assertNotNull(resultSet);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "time,id1,s1,s2,s3",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.INTEGER,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            if (i == 1) {
              actualBuilder.append(resultSet.getTimestamp(i).getTime()).append(",");
            } else {
              actualBuilder.append(resultSet.getString(i)).append(",");
            }
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertNaN() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute(
          "CREATE TABLE sg8 (id1 string id, s1 float measurement, s2 double measurement)");
      // NaN should be a string literal, i.e., 'NaN', not NaN or "NaN"
      try {
        statement.execute("insert into sg8(id1,time,s1,s2) values('d2',1,NaN,NaN)");
        fail("expected exception");
      } catch (SQLException e) {
        assertEquals(
            "701: Cannot insert identifier NaN, please use string literal", e.getMessage());
      }
      try {
        statement.execute("insert into sg8(id1,time,s1,s2) values('d2',1,\"NaN\",\"NaN\")");
        fail("expected exception");
      } catch (SQLException e) {
        assertEquals(
            "701: Cannot insert identifier \"NaN\", please use string literal", e.getMessage());
      }

      statement.execute("insert into sg8(id1,time,s1,s2) values('d2',1,'NaN','NaN')");

      try (ResultSet resultSet = statement.executeQuery("select * from sg8")) {
        assertNotNull(resultSet);
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(1, resultSet.getLong("time"));
          assertTrue(Float.isNaN(resultSet.getFloat("s1")));
          assertTrue(Double.isNaN(resultSet.getDouble("s2")));
          cnt++;
        }
        assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore // aggregation
  @Test
  public void testInsertWithoutTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");
      statement.execute(
          "CREATE TABLE sg9(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
      statement.execute("insert into sg9(id1, s1, s2, s3) values ('d1',1, 1, '1')");
      Thread.sleep(1);
      statement.execute("insert into sg9(id1, s2, s1, s3) values ('d1',2, 2, '2')");
      Thread.sleep(1);
      statement.execute("insert into sg9(id1, s3, s2, s1) values ('d1','3', 3, 3)");
      Thread.sleep(1);
      statement.execute("insert into sg9(id1, s1) values ('d1',1)");
      statement.execute("insert into sg9(id1, s2) values ('d1',2)");
      statement.execute("insert into sg9(id1, s3) values ('d1','3')");
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    String expectedHeader = "count(s1),count(s2),count(s3),";
    String[] retArray = new String[] {"4,4,4,"};
    resultSetEqualTest("select count(s1), count(s2), count(s3) from sg9", expectedHeader, retArray);
  }

  @Test
  public void testInsertMultiRow() {
    assertTableNonQueryTestFail(
        "insert into sg10(s3) values ('d1', '1'), ('d1', '2')",
        "need timestamps when insert multi rows",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg10(id1, s1, s2) values ('d1', 1, 1), ('d1', 2, 2)",
        "need timestamps when insert multi rows",
        "test");
  }

  @Test
  public void testInsertWithMultiTimesColumns() {
    assertTableNonQueryTestFail(
        "insert into sg11(id1, time, time) values ('d1', 1, 1)",
        "One row should only have one time value",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg11(id1, time, s1, time) values ('d1', 1, 1, 1)",
        "One row should only have one time value",
        "test");
  }

  @Ignore // aggregation
  @Test
  public void testInsertMultiRow2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Statement st0 = connection.createStatement();
      st0.execute("use \"test\"");
      st0.execute(
          "create table wf12 (id1 string id, status boolean measurement, temperature float measurement)");
      st0.execute("insert into wf12(id1, time, status) values ('wt01', 1, true)");
      st0.execute(
          "insert into wf12(id1, time, status) values ('wt01', 2, true), ('wt01', 3, false)");
      st0.execute(
          "insert into wf12(id1, time, status) values ('wt01', 4, true), ('wt01', 5, true), ('wt01', 6, false)");

      st0.execute(
          "insert into wf12(id1, time, temperature, status) values ('wt01', 7, 15.3, true)");
      st0.execute(
          "insert into wf12(id1, time, temperature, status) values ('wt01', 8, 18.3, false), ('wt01', 9, 23.1, false)");
      st0.execute(
          "insert into wf12(id1, time, temperature, status) values ('wt01', 10, 22.3, true), ('wt01', 11, 18.8, false), ('wt01', 12, 24.4, true)");
      st0.close();

      Statement st1 = connection.createStatement();
      ResultSet rs1 = st1.executeQuery("select count(status) from wf12");
      rs1.next();
      long countStatus = rs1.getLong(1);
      assertEquals(countStatus, 12L);

      ResultSet rs2 = st1.executeQuery("select count(temperature) from wf12");
      rs2.next();
      long countTemperature = rs2.getLong(1);
      assertEquals(countTemperature, 6L);

      st1.close();
    }
  }

  @Test
  public void testInsertMultiRowWithMisMatchDataType() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      try {
        Statement st1 = connection.createStatement();
        st1.execute("use \"test\"");
        st1.execute(
            "create table wf13 (id1 string id, status boolean measurement, temperature float measurement)");
        st1.execute(
            "insert into wf13(id1, time, status) values('wt01', 1, 1.0), ('wt01', 2, 'hello')");
        fail();
      } catch (SQLException e) {
        assertTrue(
            e.getMessage().contains(Integer.toString(TSStatusCode.METADATA_ERROR.getStatusCode())));
      }
    }
  }

  @Test
  public void testInsertMultiRowWithNull() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute(
          "create table wf14 (id1 string id, status boolean measurement, temperature float measurement)");
      st1.execute("insert into wt14(time, s1, s2) values(100, null, 1), (101, null, 2)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains(Integer.toString(TSStatusCode.METADATA_ERROR.getStatusCode())));
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      try (Statement st2 = connection.createStatement()) {
        st2.execute("use \"test\"");
        st2.execute(
            "CREATE TABLE wf15 (wt string id, s1 double measurement, s2 double measurement)");
        st2.execute(
            "INSERT INTO wf15(wt, time, s1) VALUES ('1', 6, 10),('1', 7,12),('1', 8,14),('1', 9,160),('1', 10,null),('1', 11,58)");
      } catch (SQLException e) {
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void testInsertMultiRowWithWrongTimestampPrecision() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      try {
        st1.execute("use \"test\"");
        st1.execute(
            "insert into wf16(id1, time, status) values('wt01', 1618283005586000, true), ('wt01', 1618283005586001, false)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Current system timestamp precision is ms"));
      }
    }
  }

  @Test
  public void testInsertMultiRowWithMultiTimePartition() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute("create table sg17 (id1 string id, s1 int32 measurement)");
      st1.execute("insert into sg17(id1, time, s1) values('d1', 604800010,1)");
      st1.execute("flush");
      st1.execute("insert into sg17(id1, time, s1) values('d1', 604799990,1), ('d1', 604800001,1)");
      st1.execute("flush");

      ResultSet rs1 = st1.executeQuery("select time, s1 from sg17");
      assertTrue(rs1.next());
      assertEquals(604799990, rs1.getLong("time"));
      assertTrue(rs1.next());
      assertEquals(604800001, rs1.getLong("time"));
      assertTrue(rs1.next());
      assertEquals(604800010, rs1.getLong("time"));
      assertFalse(rs1.next());
    }
  }

  @Test
  public void testInsertAttributes() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute(
          "create table if not exists sg18 (id1 string id, s1 string attribute, s2 int32 measurement)");
      st1.execute("insert into sg18(id1, s1, s2) values('d1','1', 1)");
      st1.execute("insert into sg18(id1, s1, s2) values('d2', 2, 2)");

      ResultSet rs1 = st1.executeQuery("select time, s1, s2 from sg18 order by s1");
      assertTrue(rs1.next());
      assertEquals("1", rs1.getString("s1"));
      assertTrue(rs1.next());
      assertEquals("2", rs1.getString("s1"));
      assertFalse(rs1.next());
    }
  }

  @Test
  public void testInsertCaseSensitivity() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute(
          "create table if not exists sg19 (id1 string id, ss1 string attribute, ss2 int32 measurement)");
      // lower case
      st1.execute("insert into sg19(time, id1, ss1, ss2) values(1, 'd1','1', 1)");
      st1.execute("insert into sg19(time, id1, ss1, ss2) values(2, 'd2', 2, 2)");
      // upper case
      st1.execute("insert into sg19(TIME, ID1, SS1, SS2) values(3, 'd3','3', 3)");
      st1.execute("insert into sg19(TIME, ID1, SS1, SS2) values(4, 'd4', 4, 4)");
      // mixed
      st1.execute("insert into sg19(TIme, Id1, Ss1, Ss2) values(5, 'd5','5', 5)");
      st1.execute("insert into sg19(TIme, Id1, sS1, sS2) values(6, 'd6', 6, 6)");

      ResultSet rs1 = st1.executeQuery("select time, ss1, ss2 from sg19 order by time");
      for (int i = 1; i <= 6; i++) {
        assertTrue(rs1.next());
        assertEquals(i, rs1.getLong("time"));
        assertEquals(String.valueOf(i), rs1.getString("ss1"));
        assertEquals(i, rs1.getInt("ss2"));
      }
      assertFalse(rs1.next());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      assertNotNull(typeIndex);
      assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }
}
