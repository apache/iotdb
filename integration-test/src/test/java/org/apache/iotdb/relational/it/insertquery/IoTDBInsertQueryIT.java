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

package org.apache.iotdb.relational.it.insertquery;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInsertQueryIT {

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        "CREATE TABLE IF NOT EXISTS vehicle0(deviceId STRING TAG, manufacturer STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD, s4 BOOLEAN FIELD)",
        "CREATE TABLE IF NOT EXISTS tb_0(s0 INT32 FIELD, s1 TEXT FIELD, deviceId STRING TAG)",
      };
  private static final String insertTemplate =
      "INSERT INTO vehicle0(time,deviceId,manufacturer,s0,s1,s2,s3,s4) VALUES(%d,'d%d','%s',%d,%d,%f,'%d',%b)";

  private static final String insertIntoQuery = "INSERT INTO vehicle%d SELECT * FROM vehicle0";

  private static final String createTableTemplate =
      "CREATE TABLE IF NOT EXISTS vehicle%d(deviceId STRING TAG, manufacturer STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD, s4 BOOLEAN FIELD)";

  private static final String dropTableTemplate = "DROP TABLE IF EXISTS vehicle%d";

  private static final String[] manufacturers = {"huawei", "ZTE"};

  @BeforeClass
  public static void setUpClass() {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws SQLException {
    prepareDatabase();
    prepareData();
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS test");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  protected static void prepareDatabase() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSimple() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> expectedHeader = new ArrayList<>();
      List<String> expectedRetArray = new ArrayList<>();

      statement.execute("USE test");
      ResultSet resultSet =
          statement.executeQuery("select * from vehicle0 order by time, deviceId, manufacturer");
      buildExpectedResult(resultSet, expectedHeader, expectedRetArray);

      // insert into vehicle1 select * from vehicle0
      statement.execute(String.format(createTableTemplate, 1));
      statement.execute("INSERT INTO vehicle1 SELECT * FROM vehicle0");
      ResultSet r1 =
          statement.executeQuery(
              String.format("SELECT * FROM vehicle%d order by time, deviceId, manufacturer", 1));
      resultSetEqualTest(r1, expectedHeader, expectedRetArray);

      // insert into vehicle2 table vehicle0
      statement.execute(String.format(createTableTemplate, 2));
      statement.execute("INSERT INTO vehicle2 TABLE vehicle0");
      ResultSet r2 =
          statement.executeQuery(
              String.format("SELECT * FROM vehicle%d order by time, deviceId, manufacturer", 2));
      resultSetEqualTest(r2, expectedHeader, expectedRetArray);

      // insert into vehicle3 (select * from vehicle0 order by time desc)
      statement.execute(String.format(createTableTemplate, 3));
      statement.execute("INSERT INTO vehicle3 (SELECT * FROM vehicle0 order by time desc)");
      ResultSet r3 =
          statement.executeQuery(
              String.format("SELECT * FROM vehicle%d order by time, deviceId, manufacturer", 3));
      resultSetEqualTest(r3, expectedHeader, expectedRetArray);

      // drop tables
      for (int tableId = 1; tableId <= 3; tableId++) {
        statement.execute(String.format(dropTableTemplate, tableId));
      }
    }
  }

  @Test
  public void testInsertIntoSourceTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      // prepare vehicle1
      statement.execute(String.format(createTableTemplate, 1));
      statement.execute("INSERT INTO vehicle1 SELECT * FROM vehicle0");

      // vehicle1 row count
      ResultSet resultSet = statement.executeQuery("select count(*) from vehicle1");
      if (!resultSet.next()) {
        fail("Should get count value!");
      }
      long count = resultSet.getLong(1);

      statement.execute(
          "INSERT INTO vehicle1(time,deviceId,manufacturer,s0,s1,s2,s3,s4) SELECT time+1000,deviceId,manufacturer,s0,s1,s2,s3,s4 FROM vehicle1");
      resultSet = statement.executeQuery("select count(*) from vehicle1");
      if (!resultSet.next()) {
        fail("Should get count value!");
      }
      long newCount = resultSet.getLong(1);
      assertEquals(count * 2, newCount);

      statement.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testTagCategory() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> expectedHeader = new ArrayList<>();
      List<String> expectedRetArray = new ArrayList<>();

      statement.execute("USE test");
      ResultSet resultSet = statement.executeQuery("select * from tb_0 order by time, deviceId");
      buildExpectedResult(resultSet, expectedHeader, expectedRetArray);

      statement.execute(
          "CREATE TABLE IF NOT EXISTS tb_1(s0 INT32 FIELD, s1 TEXT FIELD, deviceId STRING TAG)");
      statement.execute("INSERT INTO tb_1 SELECT * FROM tb_0");
      ResultSet r1 = statement.executeQuery("select * from tb_1 order by time, deviceId");
      resultSetEqualTest(r1, expectedHeader, expectedRetArray);

      statement.execute("DROP TABLE IF EXISTS tb_1");
    }
  }

  @Test
  public void testQualifiedName() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> expectedHeader = new ArrayList<>();
      List<String> expectedRetArray = new ArrayList<>();
      ResultSet resultSet =
          statement.executeQuery(
              "SELECT * FROM test.vehicle0 order by time, deviceId, manufacturer");
      buildExpectedResult(resultSet, expectedHeader, expectedRetArray);

      statement.execute(
          "CREATE TABLE test.vehicle1(deviceId STRING TAG, manufacturer STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD, s4 BOOLEAN FIELD)");
      statement.execute("INSERT INTO test.vehicle1 SELECT * FROM test.vehicle0");
      ResultSet r1 =
          statement.executeQuery(
              String.format(
                  "SELECT * FROM test.vehicle%d order by time, deviceId, manufacturer", 1));
      resultSetEqualTest(r1, expectedHeader, expectedRetArray);

      statement.execute(String.format("DROP TABLE IF EXISTS test.vehicle%d", 1));
    }
  }

  @Test
  public void testPartialColumn() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");
      statement.execute(
          "CREATE TABLE IF NOT EXISTS vehicle1(deviceId STRING TAG, manufacturer STRING TAG, s1 INT64 FIELD, s2 FLOAT FIELD)");

      // without time column
      try {
        statement.execute(
            "INSERT INTO vehicle1(deviceId, manufacturer, s1) SELECT deviceId, manufacturer, s1 FROM vehicle0");
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(), e.getMessage().contains("701: time column can not be null"));
      }

      // without field column
      try {
        statement.execute(
            "INSERT INTO vehicle1(time, deviceId, manufacturer) SELECT time, deviceId, manufacturer FROM vehicle0");
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("701: No Field column present"));
      }

      // It is allowed to insert without tags
      statement.execute("INSERT INTO vehicle1(time, s1, s2) SELECT time, s1, s2 FROM vehicle0");
      ResultSet resultSet = statement.executeQuery("SELECT deviceId, manufacturer FROM vehicle1");
      while (resultSet.next()) {
        for (int i = 1; i <= 2; i++) {
          assertNull(resultSet.getString(i));
        }
      }

      // insert partial tag & fields
      statement.execute("DELETE FROM vehicle1");
      statement.execute(
          "INSERT INTO vehicle1(time, s2, manufacturer) SELECT time, s2, manufacturer FROM vehicle0");
      resultSet = statement.executeQuery("SELECT deviceId, manufacturer, s1, s2 FROM vehicle1");
      while (resultSet.next()) {
        for (int i = 1; i <= 4; i++) {
          if (i % 2 == 1) {
            assertNull(resultSet.getString(i));
          } else {
            assertNotNull(resultSet.getString(i));
          }
        }
      }

      // drop table vehicle1
      statement.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testPrivileges() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement adminStmt = adminCon.createStatement();
    try {
      // create user
      adminStmt.execute("CREATE USER tmpuser 'tmppw123456789'");
      // create table vehicle1
      adminStmt.execute("USE test");
      adminStmt.execute(String.format(createTableTemplate, 1));
      adminStmt.execute("GRANT INSERT ON test.vehicle1 TO USER tmpuser");

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE test");
        // insert into vehicle1 select * from vehicle0
        statement.execute(String.format(insertIntoQuery, 1));
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "803: Access Denied: No permissions for this operation, please add privilege SELECT ON test.vehicle0"));
      }

      // select privilege on vehicle0 but no write privilege on vehicle1
      adminStmt.execute("REVOKE INSERT ON test.vehicle1 FROM USER tmpuser");
      adminStmt.execute("GRANT SELECT ON test.vehicle0 TO USER tmpuser");

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE test");
        // insert into vehicle1 select * from vehicle0
        statement.execute(String.format(insertIntoQuery, 1));
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "803: Access Denied: No permissions for this operation, please add privilege INSERT ON test.vehicle1"));
      }

      // grant write privilege on vehicle1 again
      adminStmt.execute("GRANT INSERT ON test.vehicle1 TO USER tmpuser");
      // adminStmt.execute("GRANT WRITE_DATA ON test.vehicle1 TO USER tmpuser");
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE test");
        // insert into vehicle1 select * from vehicle0
        statement.execute(String.format(insertIntoQuery, 1));
      }

    } finally {
      adminStmt.execute("DROP USER tmpuser");
      adminStmt.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testAggregate() throws SQLException {
    String querySql =
        "SELECT window_start as time, deviceid, manufacturer, max(s1) as max_s1, sum(s2) as sum_s2 FROM TUMBLE(DATA => vehicle0, TIMECOL => 'time', SIZE => 50ms) GROUP BY window_start, window_end, deviceId, manufacturer ORDER BY window_start, deviceId, manufacturer";
    String insertIntoQuerySql = "INSERT INTO vehicle1 (" + querySql + ")";

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      List<String> expectedHeader = new ArrayList<>();
      List<String> expectedRetArray = new ArrayList<>();
      statement.execute("USE test");
      ResultSet resultSet = statement.executeQuery(querySql);
      buildExpectedResult(resultSet, expectedHeader, expectedRetArray);

      // create table vehicle1 and run insert into query
      statement.execute(
          "CREATE TABLE IF NOT EXISTS vehicle1(deviceId STRING TAG, manufacturer STRING TAG, max_s1 INT64 FIELD, sum_s2 DOUBLE FIELD)");
      statement.execute(insertIntoQuerySql);

      ResultSet r1 =
          statement.executeQuery(
              String.format("SELECT * FROM vehicle%d ORDER BY time, deviceId, manufacturer", 1));
      resultSetEqualTest(r1, expectedHeader, expectedRetArray);

      // drop table vehicle1
      statement.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testTableNotExists() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE test");

      // insert into vehicle1 select * from vehicle0
      statement.execute(String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage(), e.getMessage().contains("550: Table 'test.vehicle1' does not exist"));
    }
  }

  @Test
  public void testColumnTypeMismatch1() throws SQLException {
    String errMsg =
        "701: Insert query has mismatched column types: Table: [TIMESTAMP, STRING, STRING, INT64, INT64, FLOAT, TEXT, BOOLEAN], Query: [TIMESTAMP, STRING, STRING, INT32, INT64, FLOAT, TEXT, BOOLEAN]";
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    try {
      statement.execute("USE test");
      // create table
      statement.execute(
          "CREATE TABLE IF NOT EXISTS vehicle1(deviceId STRING TAG, manufacturer STRING TAG, s0 INT64 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD, s4 BOOLEAN FIELD)");
      // insert into vehicle1 select * from vehicle0
      statement.execute(String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    } finally {
      statement.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testColumnTypeMismatch2() throws SQLException {
    String errMsg =
        "701: Insert query has mismatched column types: Table: [TIMESTAMP, STRING, STRING, INT32, INT64, FLOAT, TEXT], Query: [TIMESTAMP, STRING, STRING, INT32, INT64, FLOAT, TEXT, BOOLEAN]";
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    try {
      statement.execute("USE test");
      // create table
      statement.execute(
          "CREATE TABLE IF NOT EXISTS vehicle1(deviceId STRING TAG, manufacturer STRING TAG, s0 INT32 FIELD, s1 INT64 FIELD, s2 FLOAT FIELD, s3 TEXT FIELD)");
      // insert into vehicle1 select * from vehicle0
      statement.execute(String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(errMsg));
    } finally {
      statement.execute(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testExplain() throws SQLException {
    Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();
    try {
      statement.execute("USE test");
      statement.execute("EXPLAIN " + String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains("700: line 1:9: mismatched input 'INSERT'. Expecting: 'ANALYZE', <query>"));
    }

    try {
      statement.execute("USE test");
      statement.execute("EXPLAIN ANALYZE " + String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage(),
          e.getMessage()
              .contains(
                  "700: line 1:17: mismatched input 'INSERT'. Expecting: 'VERBOSE', <query>"));
    }
  }

  @Test
  public void testNonQueryStatement() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use test");
      session.executeNonQueryStatement(String.format(createTableTemplate, 1));

      // insert into vehicle1 select * from vehicle0
      session.executeNonQueryStatement(String.format(insertIntoQuery, 1));
      SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT * FROM vehicle1 order by time, deviceId, manufacturer");
      assertEquals(dataSet.getColumnNames().size(), 8);
      assertEquals(dataSet.getColumnNames().get(0), "time");
      assertEquals(dataSet.getColumnNames().get(3), "s0");
      assertEquals(dataSet.getColumnNames().get(4), "s1");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        long time = rowRecord.getFields().get(0).getLongV();
        assertEquals(time, rowRecord.getFields().get(3).getIntV());
        assertEquals(time, rowRecord.getFields().get(4).getLongV());
        cnt++;
      }
      Assert.assertEquals(1200, cnt);
      session.executeNonQueryStatement(String.format(dropTableTemplate, 1));
    }
  }

  @Test
  public void testQueryStatement() throws IoTDBConnectionException, StatementExecutionException {
    ITableSession session = EnvFactory.getEnv().getTableSessionConnection();
    try {
      session.executeNonQueryStatement("use test");
      session.executeNonQueryStatement(String.format(createTableTemplate, 1));
      SessionDataSet dataSet = session.executeQueryStatement(String.format(insertIntoQuery, 1));
      fail("No exception!");
    } catch (Exception e) {
      if (e.getMessage() != null) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Cannot invoke \"java.util.List.size()\" because \"columnNameList\" is null"));
      }
    } finally {
      session.executeNonQueryStatement(String.format(dropTableTemplate, 1));
      session.close();
    }
  }

  @Test
  public void testDeviceTagLast() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use test");
      session.executeNonQueryStatement(
          "create table source(s1 boolean field, device_id string tag)");
      session.executeNonQueryStatement(
          "insert into source(time, device_id, s1) values(1, 'd1', false)");
      session.executeNonQueryStatement(
          "create table target(device_id string tag, s1 boolean field)");

      session.executeNonQueryStatement(
          "INSERT INTO target(time, device_id, s1) SELECT time, device_id, s1 FROM source");
      try (SessionDataSet dataSet =
          session.executeQueryStatement("SELECT time, device_id, s1 FROM target")) {
        assertEquals(3, dataSet.getColumnNames().size());
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        int count = 0;
        while (iterator.next()) {
          count++;
          assertEquals(1, iterator.getInt(1));
          assertEquals("d1", iterator.getString(2));
          assertFalse(iterator.getBoolean(3));
        }
        assertEquals(1, count);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void buildExpectedResult(
      ResultSet resultSet, List<String> expectedHeader, List<String> expectedRetArray)
      throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      expectedHeader.add(metaData.getColumnName(i));
    }

    while (resultSet.next()) {
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        builder.append(resultSet.getString(i)).append(",");
      }
      expectedRetArray.add(builder.toString());
    }
  }

  private void resultSetEqualTest(
      ResultSet resultSet, List<String> expectedHeader, List<String> expectedRetArray)
      throws SQLException {
    // meta data
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      assertEquals(expectedHeader.get(i - 1), resultSetMetaData.getColumnName(i));
    }
    assertEquals(expectedHeader.size(), resultSetMetaData.getColumnCount());

    int cnt = 0;
    while (resultSet.next()) {
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i <= expectedHeader.size(); i++) {
        builder.append(resultSet.getString(i)).append(",");
      }
      assertEquals(expectedRetArray.get(cnt), builder.toString());
      cnt++;
    }
  }

  protected static void prepareData() throws SQLException {
    int deviceNum = 3;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      // prepare seq file
      for (int d = 0; d < deviceNum; d++) {
        for (int i = 201; i <= 300; i++) {
          String sql =
              String.format(
                  insertTemplate, i, d, manufacturers[i % 2], i, i, (double) i, i, i % 2 == 0);
          statement.execute(sql);
          statement.execute(
              String.format("insert into tb_0 values(%d, %d, '%d', 'd%d')", i, i, i, d));
        }
      }
      statement.execute("flush");

      // prepare unseq File
      for (int d = 0; d < deviceNum; d++) {
        for (int i = 1; i <= 100; i++) {
          String sql =
              String.format(
                  insertTemplate, i, d, manufacturers[i % 2], i, i, (double) i, i, i % 2 == 0);
          statement.execute(sql);
          statement.execute(
              String.format("insert into tb_0 values(%d, %d, '%d', 'd%d')", i, i, i, d));
        }
      }
      statement.execute("flush");

      for (int d = 0; d < deviceNum; d++) {
        // prepare BufferWrite cache
        for (int i = 301; i <= 400; i++) {
          String sql =
              String.format(
                  insertTemplate, i, d, manufacturers[i % 2], i, i, (double) i, i, i % 2 == 0);
          statement.execute(sql);
          statement.execute(
              String.format("insert into tb_0 values(%d, %d, '%d', 'd%d')", i, i, i, d));
        }
        // prepare Overflow cache
        for (int i = 101; i <= 200; i++) {
          String sql =
              String.format(
                  insertTemplate, i, d, manufacturers[i % 2], i, i, (double) i, i, i % 2 == 0);
          statement.execute(sql);
          statement.execute(
              String.format("insert into tb_0 values(%d, %d, '%d', 'd%d')", i, i, i, d));
        }
      }
    }
  }
}
