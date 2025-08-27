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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCteIT {
  private static final String DATABASE_NAME = "testdb";

  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS testdb",
        "USE testdb",
        "CREATE TABLE IF NOT EXISTS testtb(deviceid STRING TAG, voltage FLOAT FIELD)",
        "INSERT INTO testtb VALUES(1000, 'd1', 100.0)",
        "INSERT INTO testtb VALUES(2000, 'd1', 200.0)",
        "INSERT INTO testtb VALUES(1000, 'd2', 300.0)",
      };

  private static final String dropDbSqls = "DROP DATABASE IF EXISTS testdb";

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

  @AfterClass
  public static void tearDownClass() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws SQLException {
    prepareData();
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute(dropDbSqls);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testQuery() {
    String[] expectedHeader = new String[] {"time", "deviceid", "voltage"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:01.000Z,d1,100.0,",
          "1970-01-01T00:00:02.000Z,d1,200.0,",
          "1970-01-01T00:00:01.000Z,d2,300.0,"
        };
    tableResultSetEqualTest(
        "with cte as (select * from testtb) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"deviceid", "voltage"};
    retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select deviceid, voltage from testtb) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"deviceid", "avg_voltage"};
    retArray = new String[] {"d1,150.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select deviceid, avg(voltage) as avg_voltage from testtb group by deviceid) select * from cte order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testPartialColumn() {
    String[] expectedHeader = new String[] {"id", "v"};
    String[] retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with cte(id, v) as (select deviceid, voltage from testtb) select * from cte order by id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "with cte(v) as (select deviceid, voltage from testtb) select * from cte order by id",
        "701: Column alias list has 1 entries but relation has 2 columns",
        DATABASE_NAME);
  }

  @Test
  public void testExplain() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE testdb");
      // explain
      ResultSet resultSet =
          statement.executeQuery(
              "explain with cte as (select * from testtb) select * from cte order by deviceid");
      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 1);
      assertEquals(metaData.getColumnName(1), "distribution plan");

      // explain analyze
      resultSet =
          statement.executeQuery(
              "explain analyze with cte as (select * from testtb) select * from cte order by deviceid");
      metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 1);
      assertEquals(metaData.getColumnName(1), "Explain Analyze");
    }
  }

  @Test
  public void testMultiReference() {
    String[] expectedHeader = new String[] {"time", "deviceid", "voltage"};
    String[] retArray = new String[] {"1970-01-01T00:00:01.000Z,d2,300.0,"};
    tableResultSetEqualTest(
        "with cte as (select * from testtb) select * from cte where voltage > (select avg(voltage) from cte)",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDomain() {
    String[] expectedHeader = new String[] {"deviceid", "voltage"};
    String[] retArray = new String[] {"d1,100.0,", "d1,200.0,", "d2,300.0,"};
    tableResultSetEqualTest(
        "with testtb as (select deviceid, voltage from testtb) select * from testtb order by deviceid",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableAssertTestFail(
        "with testtb as (select voltage from testtb) select * from testtb order by deviceid",
        "616: Column 'deviceid' cannot be resolved",
        DATABASE_NAME);
  }

  @Test
  public void testSession() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use testdb");
      SessionDataSet dataSet =
          session.executeQueryStatement("with cte as (select * from testtb) select * from cte");

      assertEquals(dataSet.getColumnNames().size(), 3);
      assertEquals(dataSet.getColumnNames().get(0), "time");
      assertEquals(dataSet.getColumnNames().get(1), "deviceid");
      assertEquals(dataSet.getColumnNames().get(2), "voltage");
      int cnt = 0;
      while (dataSet.hasNext()) {
        dataSet.next();
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    }
  }

  @Test
  public void testJdbc() throws ClassNotFoundException, SQLException {
    BaseEnv env = EnvFactory.getEnv();
    String uri = String.format("jdbc:iotdb://%s:%s?sql_dialect=table", env.getIP(), env.getPort());
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection =
            DriverManager.getConnection(
                uri, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("use testdb");
      ResultSet resultSet =
          statement.executeQuery("with cte as (select * from testtb) select * from cte");

      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(metaData.getColumnCount(), 3);
      assertEquals(metaData.getColumnLabel(1), "time");
      assertEquals(metaData.getColumnLabel(2), "deviceid");
      assertEquals(metaData.getColumnLabel(3), "voltage");

      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    }
  }

  @Test
  public void testNest() {
    String sql1 =
        "WITH"
            + " cte1 AS (select deviceid, voltage from testtb where voltage > 200),"
            + " cte2 AS (SELECT voltage FROM cte1)"
            + " SELECT * FROM cte2";

    String sql2 =
        "WITH"
            + " cte2 AS (SELECT voltage FROM cte1),"
            + " cte1 AS (select deviceid, voltage from testtb where voltage > 200)"
            + " SELECT * FROM cte2";

    String[] expectedHeader = new String[] {"voltage"};
    String[] retArray = new String[] {"300.0,"};
    tableResultSetEqualTest(sql1, expectedHeader, retArray, DATABASE_NAME);

    tableAssertTestFail(sql2, "550: Table 'testdb.cte1' does not exist.", DATABASE_NAME);
  }

  @Test
  public void testRecursive() {
    String sql =
        "WITH RECURSIVE t(n) AS ("
            + " VALUES (1)"
            + " UNION ALL"
            + " SELECT n+1 FROM t WHERE n < 100)"
            + " SELECT sum(n) FROM t";

    tableAssertTestFail(sql, "701: recursive cte is not supported yet", DATABASE_NAME);
  }

  @Test
  public void testPrivileges() throws SQLException {
    Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement adminStmt = adminCon.createStatement();
    try {
      adminStmt.execute("CREATE USER tmpuser 'tmppw123456789'");
      adminStmt.execute("USE testdb");
      adminStmt.execute(
          "CREATE TABLE IF NOT EXISTS testtb1(deviceid STRING TAG, voltage FLOAT FIELD)");
      adminStmt.execute("GRANT SELECT ON testdb.testtb TO USER tmpuser");

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE testdb");
        statement.execute("with cte as (select * from testtb) select * from cte");
      }

      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection("tmpuser", "tmppw123456789", BaseEnv.TABLE_SQL_DIALECT);
          Statement statement = connection.createStatement()) {
        statement.execute("USE testdb");
        statement.execute("with cte as (select * from testtb1) select * from testtb");
        fail("No exception!");
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "803: Access Denied: No permissions for this operation, please add privilege SELECT ON testdb.testtb1"));
      }
    } finally {
      adminStmt.execute("DROP USER tmpuser");
      adminStmt.execute("DROP TABLE IF EXISTS testtb1");
    }
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
