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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.SimpleEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showRegionColumnHeaders;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
          "CREATE TABLE sg10(tag1 string tag, s1 int64 field, s2 float field, s3 string field)");
      statement.execute(
          "CREATE TABLE sg11(tag1 string tag, s1 int64 field, s2 float field, s3 string field)");
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
      statement.execute("create table sg1 (tag1 string tag, s1 int32 field)");
      statement.execute("insert into sg1(tag1,time,s1) values('d1',1,2)");
      statement.execute("flush");
      statement.execute("insert into sg1(tag1,time,s1) values('d1',2,2)");
      statement.execute("insert into sg1(tag1,time,s1) values('d1',604800001,2)");
      statement.execute("flush");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowRegion() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("create table sg2 (tag1 string tag, s1 int32 field)");
      statement.execute("insert into sg2(tag1,time,s1) values('d1',1,2)");
      statement.execute("flush");
      statement.execute("insert into sg2(tag1,time,s1) values('d1',2,2)");
      statement.execute("insert into sg2(tag1,time,s1) values('d1',604800001,2)");
      statement.execute("flush");

      // Test show regions in table model
      try (final ResultSet resultSet = statement.executeQuery("show regions")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showRegionColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showRegionColumnHeaders.size(); i++) {
          assertEquals(
              showRegionColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        assertTrue(resultSet.next());
      }

      try (final ResultSet resultSet = statement.executeQuery("show regions from test")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showRegionColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showRegionColumnHeaders.size(); i++) {
          assertEquals(
              showRegionColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        assertTrue(resultSet.next());
      }
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertTimeAtAnyIndex() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.addBatch(
          "create table IF NOT EXISTS db2(tag1 string tag, s1 int32 field, s2 int32 field)");
      statement.addBatch("insert into db2(tag1, s1, s2, time) values ('d1', 2, 3, 1)");
      statement.addBatch("insert into db2(tag1, s1, time, s2) values ('d1', 20, 10, 30)");
      statement.addBatch("insert into db2(tag1, \"time\", s1, s2) values ('d1', 100, 200, 300)");
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
        statement.addBatch("create table t3(tag1 string tag, s1 int32 field, s2 int32 field)");
        statement.addBatch("insert into t3(tag1, s1, s2, time, time) values ('d1', 2, 3, 1, 1)");
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
        statement.execute("create table sg4 (tag1 string tag, s0 int32 field)");
        statement.execute("INSERT INTO sg4(tag1, timestamp, s0) VALUES ('tag', 1, 1)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Unknown column category"));
      }
    }
  }

  @Test
  public void testPartialInsertTablet() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use \"test\"");
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='false'");
      session.executeNonQueryStatement(
          "create table sg6 (tag1 string tag, s1 int64 field, s2 int64 field)");
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD);
      Tablet tablet =
          new Tablet(
              "sg6",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              300);
      long timestamp = 0;
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
          }
        }
        timestamp++;
      }
      timestamp = System.currentTimeMillis();
      for (long row = 0; row < 100; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        for (int s = 0; s < 4; s++) {
          long value = timestamp;
          if (s == 0) {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, "d1");
          } else {
            tablet.addValue(schemaList.get(s).getMeasurementName(), rowIndex, value);
          }
        }
        timestamp++;
      }
      try {
        session.insert(tablet);
      } catch (Exception e) {
        if (!e.getMessage().contains("507")) {
          fail(e.getMessage());
        }
      } finally {
        session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='false'");
      }
      try (SessionDataSet dataSet = session.executeQueryStatement("SELECT * FROM sg6")) {
        assertEquals(dataSet.getColumnNames().size(), 4);
        assertEquals(dataSet.getColumnNames().get(0), "time");
        assertEquals(dataSet.getColumnNames().get(1), "tag1");
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
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='true'");
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
          "CREATE TABLE sg7 (tag1 string tag, s1 boolean field, s2 float field, s3 int32 field)");
      statement.execute("insert into sg7(tag1,time,s1,s2,s3) values('d2',1,null,1.0,1)");
      statement.execute("insert into sg7(tag1,time,s1,s2,s3) values('d2',2,true,null,2)");
      statement.execute("insert into sg7(tag1,time,s1,s2,s3) values('d2',3,true,3.0,null)");

      try (ResultSet resultSet = statement.executeQuery("select * from sg7")) {
        assertNotNull(resultSet);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "time,tag1,s1,s2,s3",
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
      statement.execute("CREATE TABLE sg8 (tag1 string tag, s1 float field, s2 double field)");
      // NaN should be a string literal, i.e., 'NaN', not NaN or "NaN"
      try {
        statement.execute("insert into sg8(tag1,time,s1,s2) values('d2',1,NaN,NaN)");
        fail("expected exception");
      } catch (SQLException e) {
        assertEquals(
            "701: Cannot insert identifier NaN, please use string literal", e.getMessage());
      }
      try {
        statement.execute("insert into sg8(tag1,time,s1,s2) values('d2',1,\"NaN\",\"NaN\")");
        fail("expected exception");
      } catch (SQLException e) {
        assertEquals(
            "701: Cannot insert identifier \"NaN\", please use string literal", e.getMessage());
      }

      statement.execute("insert into sg8(tag1,time,s1,s2) values('d2',1,'NaN','NaN')");

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

  // aggregation
  @Test
  public void testInsertWithoutTime() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE \"test\"");
      statement.execute(
          "CREATE TABLE sg9(tag1 string tag, s1 int64 field, s2 float field, s3 string field)");
      statement.execute("insert into sg9(tag1, s1, s2, s3) values ('d1',1, 1, '1')");
      Thread.sleep(1);
      statement.execute("insert into sg9(tag1, s2, s1, s3) values ('d1',2, 2, '2')");
      Thread.sleep(1);
      statement.execute("insert into sg9(tag1, s3, s2, s1) values ('d1','3', 3, 3)");
      Thread.sleep(1);
      statement.execute("insert into sg9(tag1, s1) values ('d1',1)");
      statement.execute("insert into sg9(tag1, s2) values ('d1',2)");
      statement.execute("insert into sg9(tag1, s3) values ('d1','3')");
    } catch (SQLException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    String[] expectedHeader = new String[] {"_col0", "_col1", "_col2"};
    String[] retArray = new String[] {"4,4,4,"};
    tableResultSetEqualTest(
        "select count(s1), count(s2), count(s3) from sg9", expectedHeader, retArray, "test");
  }

  @Test
  public void testInsertMultiRow() {
    assertTableNonQueryTestFail(
        "insert into sg10(s3) values ('d1', '1'), ('d1', '2')",
        "need timestamps when insert multi rows",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg10(tag1, s1, s2) values ('d1', 1, 1), ('d1', 2, 2)",
        "need timestamps when insert multi rows",
        "test");
  }

  @Test
  public void testInsertWithMultiTimesColumns() {
    assertTableNonQueryTestFail(
        "insert into sg11(tag1, time, time) values ('d1', 1, 1)",
        "One row should only have one time value",
        "test");
    assertTableNonQueryTestFail(
        "insert into sg11(tag1, time, s1, time) values ('d1', 1, 1, 1)",
        "One row should only have one time value",
        "test");
  }

  // aggregation
  @Test
  public void testInsertMultiRow2() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      Statement st0 = connection.createStatement();
      st0.execute("use \"test\"");
      st0.execute(
          "create table wf12 (tag1 string tag, status boolean field, temperature float field)");
      st0.execute("insert into wf12(tag1, time, status) values ('wt01', 1, true)");
      st0.execute(
          "insert into wf12(tag1, time, status) values ('wt01', 2, true), ('wt01', 3, false)");
      st0.execute(
          "insert into wf12(tag1, time, status) values ('wt01', 4, true), ('wt01', 5, true), ('wt01', 6, false)");

      st0.execute(
          "insert into wf12(tag1, time, temperature, status) values ('wt01', 7, 15.3, true)");
      st0.execute(
          "insert into wf12(tag1, time, temperature, status) values ('wt01', 8, 18.3, false), ('wt01', 9, 23.1, false)");
      st0.execute(
          "insert into wf12(tag1, time, temperature, status) values ('wt01', 10, 22.3, true), ('wt01', 11, 18.8, false), ('wt01', 12, 24.4, true)");
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
            "create table wf13 (tag1 string tag, status boolean field, temperature float field)");
        st1.execute(
            "insert into wf13(tag1, time, status) values('wt01', 1, 1.0), ('wt01', 2, 'hello')");
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
      st1.execute("SET CONFIGURATION enable_auto_create_schema='false'");
      st1.execute("use \"test\"");
      st1.execute(
          "create table wf14 (tag1 string tag, status boolean field, temperature float field)");
      st1.execute("insert into wt14(time, s1, s2) values(100, null, 1), (101, null, 2)");
      fail();
    } catch (SQLException e) {
      assertEquals("550: Table 'test.wt14' does not exist.", e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      try (Statement st2 = connection.createStatement()) {
        st2.execute("use \"test\"");
        st2.execute("CREATE TABLE wf15 (wt string tag, s1 double field, s2 double field)");
        st2.execute(
            "INSERT INTO wf15(wt, time, s1) VALUES ('1', 6, 10),('1', 7,12),('1', 8,14),('1', 9,160),('1', 10,null),('1', 11,58)");

        st2.execute("SET CONFIGURATION enable_auto_create_schema='true'");
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
            "insert into wf16(tag1, time, status) values('wt01', 1618283005586000, true), ('wt01', 1618283005586001, false)");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Current system timestamp precision is ms"));
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      try {
        st1.execute("use \"test\"");
        st1.execute(
            "insert into wf16(tag1, time, status) values('wt01', -1618283005586000, true), ('wt01', -1618283005586001, false)");
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
      st1.execute("create table sg17 (tag1 string tag, s1 int32 field)");
      st1.execute("insert into sg17(tag1, time, s1) values('d1', 604800010,1)");
      st1.execute("flush");
      st1.execute(
          "insert into sg17(tag1, time, s1) values('d1', 604799990,1), ('d1', 604800001,1)");
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
          "create table if not exists sg18 (tag1 string tag, s1 string attribute, s2 int32 field)");
      st1.execute("insert into sg18(tag1, s1, s2) values('d1','1', 1)");
      st1.execute("insert into sg18(tag1, s1, s2) values('d2', 2, 2)");

      ResultSet rs1 = st1.executeQuery("select time, s1, s2 from sg18 order by s1");
      assertTrue(rs1.next());
      assertEquals("1", rs1.getString("s1"));
      assertTrue(rs1.next());
      assertEquals("2", rs1.getString("s1"));
      assertFalse(rs1.next());
    }
  }

  @Test
  public void testInsertCaseSensitivity()
      throws SQLException, IoTDBConnectionException, StatementExecutionException {
    // column case sensitivity
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute(
          "create table if not exists sg19 (tag1 string tag, ss1 string attribute, ss2 int32 field)");
      // lower case
      st1.execute("insert into sg19(time, tag1, ss1, ss2) values(1, 'd1','1', 1)");
      st1.execute("insert into sg19(time, tag1, ss1, ss2) values(2, 'd2', 2, 2)");
      // upper case
      st1.execute("insert into sg19(TIME, TAG1, SS1, SS2) values(3, 'd3','3', 3)");
      st1.execute("insert into sg19(TIME, TAG1, SS1, SS2) values(4, 'd4', 4, 4)");
      // mixed
      st1.execute("insert into sg19(TIme, Tag1, Ss1, Ss2) values(5, 'd5','5', 5)");
      st1.execute("insert into sg19(TIme, Tag1, sS1, sS2) values(6, 'd6', 6, 6)");

      ResultSet rs1 = st1.executeQuery("select time, ss1, ss2 from sg19 order by time");
      for (int i = 1; i <= 6; i++) {
        assertTrue(rs1.next());
        assertEquals(i, rs1.getLong("time"));
        assertEquals(String.valueOf(i), rs1.getString("ss1"));
        assertEquals(i, rs1.getInt("ss2"));
      }
      assertFalse(rs1.next());
    }

    // table case sensitivity with record and auto creation
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"test\"");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;

      Tablet tablet =
          new Tablet(
              "TaBle19_2",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);
      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      int cnt = 0;
      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table19_2 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(15, cnt);
    }

    // table case sensitivity with record and no auto creation
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"test\"");
      session.executeNonQueryStatement(
          "CREATE TABLE tAbLE19_3 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);
      List<String> fieldIds =
          schemaList.stream()
              .map(IMeasurementSchema::getMeasurementName)
              .collect(Collectors.toList());
      List<TSDataType> dataTypes = IMeasurementSchema.getDataTypeList(schemaList);

      long timestamp = 0;

      Tablet tablet =
          new Tablet(
              "TaBle19_3",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);
      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      int cnt = 0;
      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table19_3 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(15, cnt);
    }

    // table case sensitivity with tablet and no auto creation
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"test\"");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "TaBle19_4",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table19_4 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(15, cnt);
    }

    // table case sensitivity with tablet and auto creation
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"test\"");
      session.executeNonQueryStatement(
          "CREATE TABLE tAbLE19_5 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "TaBle19_5",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table19_5 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(15, cnt);
    }
  }

  @Test
  public void testInsertKeyword() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='true'");
      session.executeNonQueryStatement("USE \"test\"");
      session.executeNonQueryStatement(
          "create table table20 ("
              + "device_id string tag,"
              + "attribute STRING ATTRIBUTE,"
              + "boolean boolean FIELD,"
              + "int32 int32 FIELD,"
              + "int64 int64 FIELD,"
              + "float float FIELD,"
              + "double double FIELD,"
              + "text text FIELD,"
              + "string string FIELD,"
              + "blob blob FIELD,"
              + "timestamp01 timestamp FIELD,"
              + "date date FIELD)");

      List<IMeasurementSchema> schemas = new ArrayList<>();
      schemas.add(new MeasurementSchema("device_id", TSDataType.STRING));
      schemas.add(new MeasurementSchema("attribute", TSDataType.STRING));
      schemas.add(new MeasurementSchema("boolean", TSDataType.BOOLEAN));
      schemas.add(new MeasurementSchema("int32", TSDataType.INT32));
      schemas.add(new MeasurementSchema("int64", TSDataType.INT64));
      schemas.add(new MeasurementSchema("float", TSDataType.FLOAT));
      schemas.add(new MeasurementSchema("double", TSDataType.DOUBLE));
      schemas.add(new MeasurementSchema("text", TSDataType.TEXT));
      schemas.add(new MeasurementSchema("string", TSDataType.STRING));
      schemas.add(new MeasurementSchema("blob", TSDataType.BLOB));
      schemas.add(new MeasurementSchema("timestamp", TSDataType.TIMESTAMP));
      schemas.add(new MeasurementSchema("date", TSDataType.DATE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.TAG,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table20",
              IMeasurementSchema.getMeasurementNameList(schemas),
              IMeasurementSchema.getDataTypeList(schemas),
              columnTypes,
              10);

      for (long row = 0; row < 10; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("device_id", rowIndex, "1");
        tablet.addValue("attribute", rowIndex, "1");
        tablet.addValue("boolean", rowIndex, true);
        tablet.addValue("int32", rowIndex, Integer.valueOf("1"));
        tablet.addValue("int64", rowIndex, Long.valueOf("1"));
        tablet.addValue("float", rowIndex, Float.valueOf("1.0"));
        tablet.addValue("double", rowIndex, Double.valueOf("1.0"));
        tablet.addValue("text", rowIndex, "true");
        tablet.addValue("string", rowIndex, "true");
        tablet.addValue("blob", rowIndex, new Binary("iotdb", Charset.defaultCharset()));
        tablet.addValue("timestamp", rowIndex, 1L);
        tablet.addValue("date", rowIndex, LocalDate.parse("2024-08-15"));
      }
      session.insert(tablet);

      SessionDataSet rs1 =
          session.executeQueryStatement(
              "select time, device_id, attribute, boolean, int32, int64, float, double, text, string, blob, timestamp, date from table20 order by time");
      for (int i = 0; i < 10; i++) {
        RowRecord rec = rs1.next();
        assertEquals(i, rec.getFields().get(0).getLongV());
        assertEquals("1", rec.getFields().get(1).getStringValue());
        assertEquals("1", rec.getFields().get(2).getStringValue());
        assertTrue(rec.getFields().get(3).getBoolV());
        assertEquals(1, rec.getFields().get(4).getIntV());
        assertEquals(1, rec.getFields().get(5).getLongV());
        assertEquals(1.0, rec.getFields().get(6).getFloatV(), 0.001);
        assertEquals(1.0, rec.getFields().get(7).getDoubleV(), 0.001);
        assertEquals("true", rec.getFields().get(8).getStringValue());
        assertEquals("true", rec.getFields().get(9).getStringValue());
        assertEquals("0x696f746462", rec.getFields().get(10).getStringValue());
        assertEquals(1, rec.getFields().get(11).getLongV());
        assertEquals("20240815", rec.getFields().get(12).getStringValue());
      }
      assertFalse(rs1.hasNext());
    }
  }

  @Test
  public void testInsertSingleColumn() throws SQLException, InterruptedException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute(
          "create table if not exists sg21 (tag1 string tag, ss1 string attribute, ss2 int32 field)");
      // only tag
      try {
        st1.execute("insert into sg21(tag1) values('1')");
      } catch (SQLException e) {
        assertEquals("507: No Field column present, please check the request", e.getMessage());
      }
      // only time
      try {
        st1.execute("insert into sg21(time) values(1)");
      } catch (SQLException e) {
        assertEquals(
            "507: No column other than Time present, please check the request", e.getMessage());
      }
      // sleep a while to avoid the same timestamp between two insertions
      Thread.sleep(10);
      // only attribute
      try {
        st1.execute("insert into sg21(ss1) values('1')");
      } catch (SQLException e) {
        assertEquals("507: No Field column present, please check the request", e.getMessage());
      }
      // sleep a while to avoid the same timestamp between two insertions
      Thread.sleep(10);
      // only field
      st1.execute("insert into sg21(ss2) values(1)");

      ResultSet rs1 = st1.executeQuery("show devices from sg21");
      assertTrue(rs1.next());
      // from "insert into sg21(ss2) values(1)"
      assertEquals(null, rs1.getString("tag1"));
      assertFalse(rs1.next());
      // from "insert into sg21(tag1) values('1')"
      assertEquals(null, rs1.getString("tag1"));
      assertFalse(rs1.next());

      rs1 = st1.executeQuery("select time, ss1, ss2 from sg21 order by time");
      assertTrue(rs1.next());
      rs1.getString("ss1");
      assertTrue(rs1.wasNull());
      rs1.getInt("ss2");
      assertEquals(1, rs1.getInt("ss2"));

      assertFalse(rs1.next());
    }
  }

  @Test
  public void testInsertWithTTL() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use \"test\"");
      statement.execute("create table sg22 (tag1 string tag, s1 int64 field)");
      statement.execute("alter table sg22 set properties TTL=1");
      statement.execute(
          String.format(
              "insert into sg22(tag1,time,s1) values('d1',%s,2)",
              System.currentTimeMillis() - 10000));
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("less than ttl time bound"));
    }
  }

  @Test
  public void testInsertTabletWithTTL()
      throws IoTDBConnectionException, StatementExecutionException {
    long ttl = 1;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use \"test\"");
      session.executeNonQueryStatement("create table sg23 (tag1 string tag, s1 int64 field)");
      session.executeNonQueryStatement("alter table sg23 set properties TTL=" + ttl);

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

      // all expired
      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "sg23",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 3; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("s1", rowIndex, row);
      }
      try {
        session.insert(tablet);
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("less than ttl time bound"));
      }

      // partial expired
      tablet.reset();
      timestamp = System.currentTimeMillis() - 10000;
      for (long row = 0; row < 4; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("s1", rowIndex, row);
        timestamp += 10000;
      }

      try {
        session.insert(tablet);
        fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("less than ttl time bound"));
      }

      // part of data is indeed inserted
      long timeLowerBound = System.currentTimeMillis() - ttl;
      SessionDataSet dataSet = session.executeQueryStatement("select time, s1 from sg23");
      int count = 0;
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        Assert.assertTrue(record.getFields().get(0).getLongV() > timeLowerBound);
        count++;
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testInsertUnsequenceData()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION enable_auto_create_schema='true'");
      session.executeNonQueryStatement("USE \"test\"");
      // the table is missing column "m2"
      session.executeNonQueryStatement(
          "CREATE TABLE table4 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      // the insertion contains "m2"
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("m2", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.TAG,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table4",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            // a partial insertion should be reported
            if (!e.getMessage()
                .equals(
                    "507: Fail to insert fields [m2] caused by [Column m2 does not exists or fails to be created]")) {
              throw e;
            }
          }
          tablet.reset();
        }
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, 14 - row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            if (!e.getMessage()
                .equals(
                    "507: Fail to insert fields [m2] caused by [Column m2 does not exists or fails to be created]")) {
              throw e;
            }
          }
          tablet.reset();
        }
      }
      session.executeNonQueryStatement("FLush");

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table4");
      while (dataSet.hasNext()) {
        dataSet.next();
        cnt++;
      }
      assertEquals(29, cnt);
    }
  }

  @Test
  public void testInsertAllNullRow() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("use \"test\"");
      st1.execute("create table table5(d1 string tag, s1 int32 field, s2 int32 field)");

      st1.execute("insert into table5(time, d1,s1,s2) values(1,'a',1,null)");
      // insert all null row
      st1.execute("insert into table5(time, d1,s1,s2) values(2,'a',null,null)");

      ResultSet rs1 = st1.executeQuery("select * from table5");
      assertTrue(rs1.next());
      assertEquals("1", rs1.getString("s1"));
      assertNull(rs1.getString("s2"));
      assertTrue(rs1.next());
      assertNull(rs1.getString("s1"));
      assertNull(rs1.getString("s2"));
      assertFalse(rs1.next());

      st1.execute("flush");

      rs1 = st1.executeQuery("select * from table5");
      assertTrue(rs1.next());
      assertEquals("1", rs1.getString("s1"));
      assertNull(rs1.getString("s2"));
      assertTrue(rs1.next());
      assertNull(rs1.getString("s1"));
      assertNull(rs1.getString("s2"));
      assertFalse(rs1.next());
    }
  }

  @Test
  public void testInsertWithChangedSchema() throws SQLException, IOException {
    SimpleEnv simpleEnv = new SimpleEnv();
    simpleEnv.initClusterEnvironment(1, 1);
    try (Connection connection = simpleEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement st1 = connection.createStatement()) {
      st1.execute("create database \"test\"");
      st1.execute("use \"test\"");
      st1.execute(
          "create table insert_with_changed_schema(d1 string tag, s1 int32 field, s2 int32 field)");
      st1.execute("insert into insert_with_changed_schema(time, d1,s1,s2) values(1,'a',1, 1)");

      st1.execute("drop  table insert_with_changed_schema");
      st1.execute(
          "create table insert_with_changed_schema(d3 string tag, d2 string tag, s1 int32 field, s2 int32 field)");
      st1.execute(
          "insert into insert_with_changed_schema(time, d3, d2, s1, s2) values(1,'a', 'b', 1, 1)");
      st1.execute("flush");

      List<File> files =
          FileUtils.listFilesRecursively(
              new File(simpleEnv.getDataNodeWrapper(0).getDataNodeDir()),
              f ->
                  f.getName().endsWith(TSFILE_SUFFIX)
                      && f.getAbsolutePath().contains(File.separator + "test" + File.separator));

      assertEquals(1, files.size());
      File file = files.get(0);
      try (ITsFileReader tsFileReader = new TsFileReaderBuilder().file(file).build()) {
        List<TableSchema> allTableSchema = tsFileReader.getAllTableSchema();
        assertEquals(1, allTableSchema.size());
        TableSchema tableSchema = allTableSchema.get(0);
        List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
        assertEquals(4, columnSchemas.size());
        assertEquals("d3", columnSchemas.get(0).getMeasurementName());
        assertEquals("d2", columnSchemas.get(1).getMeasurementName());
        assertEquals("s1", columnSchemas.get(2).getMeasurementName());
        assertEquals("s2", columnSchemas.get(3).getMeasurementName());
        assertEquals(ColumnCategory.TAG, tableSchema.getColumnTypes().get(0));
        assertEquals(ColumnCategory.TAG, tableSchema.getColumnTypes().get(1));
        assertEquals(ColumnCategory.FIELD, tableSchema.getColumnTypes().get(2));
        assertEquals(ColumnCategory.FIELD, tableSchema.getColumnTypes().get(3));
      }
    } finally {
      simpleEnv.cleanClusterEnvironment();
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
