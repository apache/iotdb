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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLimitZeroIT {
  private static final String DATABASE = "limit_zero";
  private static final String[] NO_ROWS = new String[0];
  private static final String[] VALUES = {"1,", "2,", "10,", "30,"};

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setMaxTsBlockLineNumber(2)
        .setDefaultDataRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(
        new String[] {
          "CREATE DATABASE root.limit_zero",
          "CREATE ALIGNED TIMESERIES root.limit_zero.d1(v INT32)",
          "INSERT INTO root.limit_zero.d1(time, v) ALIGNED VALUES (1, 1), (2, 2)"
        });
    prepareTableData(
        new String[] {
          "CREATE DATABASE " + DATABASE,
          "USE " + DATABASE,
          "CREATE TABLE t(device STRING TAG, label STRING ATTRIBUTE, v INT32 FIELD, w DOUBLE FIELD)",
          "CREATE TABLE empty_table(device STRING TAG, v INT32 FIELD)",
          "CREATE TABLE target(device STRING TAG, v INT32 FIELD)",
          "CREATE VIEW tree_view(device TAG, v INT32 FIELD) AS root.limit_zero.**",
          "INSERT INTO t(time, device, label, v, w) VALUES (1, 'd1', 'a', 1, 1.0), (1, 'd2', 'b', 10, 10.0)",
          "FLUSH",
          "INSERT INTO t(time, device, label, v, w) VALUES (2, 'd1', 'a', 2, NULL), (3, 'd2', 'b', 30, 30.0)"
        });
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testScanProjectionAndPagination() {
    String[] queries = {
      "SELECT v FROM t LIMIT 0",
      "SELECT v FROM t WHERE device = 'd1' LIMIT 0",
      "SELECT v FROM t WHERE v > 1 LIMIT 0",
      "SELECT v FROM t WHERE v + 1 > 1 LIMIT 0",
      "SELECT v FROM empty_table LIMIT 0",
      "SELECT v FROM t LIMIT 0 OFFSET 0",
      "SELECT v FROM t LIMIT 0 OFFSET 1",
      "SELECT v FROM t OFFSET 9223372036854775807 LIMIT 0",
      "SELECT v FROM t WHERE device = 'd1' LIMIT 0 OFFSET 1"
    };
    for (String sql : queries) {
      assertEmpty(sql, "v");
    }
    assertEmpty(
        "SELECT v + 1 AS n, cast(v AS DOUBLE) AS d, device FROM t LIMIT 0", "n", "d", "device");
    assertEmpty("SELECT diff(v) AS d FROM t LIMIT 0", "d");
  }

  @Test
  public void testSortAndTopK() {
    for (String ordering :
        new String[] {
          "time", "time DESC", "device, time", "device DESC", "v DESC", "v + 1", "w NULLS LAST"
        }) {
      assertEmpty("SELECT v FROM t ORDER BY " + ordering + " LIMIT 0", "v");
      assertEmpty("SELECT v FROM t ORDER BY " + ordering + " LIMIT 0 OFFSET 1", "v");
    }
  }

  @Test
  public void testAggregationAndDistinct() {
    assertEmpty(
        "SELECT count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM t LIMIT 0",
        "c",
        "s",
        "a",
        "mn",
        "mx");
    assertEmpty("SELECT count(*) AS c FROM empty_table LIMIT 0", "c");
    assertEmpty("SELECT count(*) AS c FROM t WHERE v < 0 LIMIT 0", "c");
    assertEmpty("SELECT DISTINCT device FROM t LIMIT 0", "device");
    assertEmpty("SELECT count(DISTINCT device) AS c FROM t LIMIT 0", "c");
    assertEmpty("SELECT device, count(*) AS c FROM t GROUP BY device LIMIT 0", "device", "c");
    assertEmpty(
        "SELECT device, count(*) AS c FROM t GROUP BY device HAVING count(*) > 1 ORDER BY c DESC LIMIT 0 OFFSET 1",
        "device",
        "c");
  }

  @Test
  public void testWindowAndFill() {
    assertEmpty(
        "SELECT v, row_number() OVER (PARTITION BY device ORDER BY time) AS n FROM t LIMIT 0",
        "v",
        "n");
    assertEmpty("SELECT v, sum(v) OVER (ORDER BY time) AS s FROM t ORDER BY s LIMIT 0", "v", "s");
    assertEmpty("SELECT w FROM t FILL METHOD CONSTANT 0 LIMIT 0", "w");
    assertEmpty(
        "SELECT time, w FROM t FILL METHOD PREVIOUS TIME_BOUND 2ms TIME_COLUMN 1 ORDER BY time LIMIT 0",
        "time",
        "w");
    assertEmpty(
        "SELECT time, w FROM t FILL METHOD LINEAR TIME_COLUMN 1 ORDER BY time LIMIT 0",
        "time",
        "w");
    assertEmpty(
        "SELECT date_bin_gapfill(1ms, time) AS bucket, avg(v) AS a FROM t WHERE time >= 0 AND time < 5 GROUP BY 1 FILL METHOD PREVIOUS ORDER BY 1 LIMIT 0",
        "bucket",
        "a");
  }

  @Test
  public void testNestedLimitsAndAggregates() {
    assertEmpty("SELECT v FROM (SELECT v FROM t LIMIT 0) q LIMIT 1", "v");
    assertEmpty("SELECT v FROM (SELECT v FROM t LIMIT 1) q LIMIT 0", "v");
    assertEmpty("SELECT v FROM (SELECT v FROM t LIMIT 0 OFFSET 1) q ORDER BY v LIMIT 2", "v");
    assertEmpty("SELECT v FROM (SELECT v FROM t LIMIT 0) q WHERE v > 0", "v");
    assertEmpty("SELECT DISTINCT v FROM (SELECT v FROM t LIMIT 0) q", "v");
    assertEmpty("SELECT row_number() OVER (ORDER BY v) AS n FROM (SELECT v FROM t LIMIT 0) q", "n");
    assertEmpty("SELECT w FROM (SELECT w FROM t LIMIT 0) q FILL METHOD CONSTANT 0", "w");
    assertResult(
        "SELECT count(*) AS c, sum(v) AS s FROM (SELECT v FROM t LIMIT 0) q",
        new String[] {"c", "s"},
        "0,null,");
    assertResult(
        "SELECT count(*) AS c FROM (SELECT count(*) AS c FROM t LIMIT 0) q",
        new String[] {"c"},
        "0,");
    assertResult(
        "SELECT count(*) AS c FROM (SELECT DISTINCT device FROM t LIMIT 0) q",
        new String[] {"c"},
        "0,");
    assertEmpty(
        "SELECT device, count(*) AS c FROM (SELECT device, v FROM t LIMIT 0) q GROUP BY device",
        "device",
        "c");
  }

  @Test
  public void testTableView() {
    assertEmpty("SELECT v FROM tree_view LIMIT 0", "v");
    assertEmpty("SELECT v FROM tree_view ORDER BY v DESC LIMIT 0 OFFSET 1", "v");
    assertEmpty("SELECT count(*) AS c FROM tree_view LIMIT 0", "c");
    assertResult(
        "SELECT count(*) AS c FROM (SELECT v FROM tree_view LIMIT 0) q", new String[] {"c"}, "0,");
    assertResult("SELECT count(*) AS c FROM tree_view", new String[] {"c"}, "2,");
  }

  @Test
  public void testSetOperations() {
    for (String operator :
        new String[] {"UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"}) {
      assertEmpty("(SELECT v FROM t) " + operator + " (SELECT v FROM t) LIMIT 0", "v");
      assertEmpty("(SELECT v FROM t LIMIT 0) " + operator + " (SELECT v FROM t LIMIT 0)", "v");
    }
    for (String operator : new String[] {"UNION", "UNION ALL"}) {
      assertResult(
          "(SELECT v FROM t LIMIT 0) " + operator + " (SELECT v FROM t) ORDER BY v",
          new String[] {"v"},
          VALUES);
      assertResult(
          "(SELECT v FROM t) " + operator + " (SELECT v FROM t LIMIT 0) ORDER BY v",
          new String[] {"v"},
          VALUES);
    }
    for (String operator : new String[] {"INTERSECT", "INTERSECT ALL"}) {
      assertEmpty("(SELECT v FROM t LIMIT 0) " + operator + " (SELECT v FROM t)", "v");
      assertEmpty("(SELECT v FROM t) " + operator + " (SELECT v FROM t LIMIT 0)", "v");
    }
    for (String operator : new String[] {"EXCEPT", "EXCEPT ALL"}) {
      assertEmpty("(SELECT v FROM t LIMIT 0) " + operator + " (SELECT v FROM t)", "v");
      assertResult(
          "(SELECT v FROM t) " + operator + " (SELECT v FROM t LIMIT 0) ORDER BY v",
          new String[] {"v"},
          VALUES);
    }
  }

  @Test
  public void testJoins() {
    assertEmpty("SELECT a.v FROM t a JOIN t b ON a.time = b.time LIMIT 0", "v");
    assertEmpty("SELECT a.v FROM t a JOIN (SELECT time FROM t LIMIT 0) b ON a.time = b.time", "v");
    assertEmpty("SELECT a.v FROM t a CROSS JOIN (SELECT v FROM t LIMIT 0) b", "v");
    assertResult(
        "SELECT a.v, b.v AS other FROM t a LEFT JOIN (SELECT time, v FROM t LIMIT 0) b ON a.time = b.time ORDER BY a.v",
        new String[] {"v", "other"},
        "1,null,",
        "2,null,",
        "10,null,",
        "30,null,");
    assertResult(
        "SELECT a.v AS other, b.v FROM (SELECT time, v FROM t LIMIT 0) a RIGHT JOIN t b ON a.time = b.time ORDER BY b.v",
        new String[] {"other", "v"},
        "null,1,",
        "null,2,",
        "null,10,",
        "null,30,");
    assertResult(
        "SELECT a.v AS other, b.v FROM (SELECT time, v FROM t LIMIT 0) a FULL JOIN t b ON a.time = b.time ORDER BY b.v",
        new String[] {"other", "v"},
        "null,1,",
        "null,2,",
        "null,10,",
        "null,30,");
    assertEmpty(
        "SELECT a.v FROM (SELECT time, v FROM t LIMIT 0) a FULL JOIN (SELECT time, v FROM t LIMIT 0) b ON a.time = b.time",
        "v");
  }

  @Test
  public void testAsofJoins() {
    assertEmpty("SELECT a.v FROM t a ASOF LEFT JOIN t b ON a.time >= b.time LIMIT 0", "v");
    assertEmpty(
        "SELECT a.v FROM t a ASOF INNER JOIN (SELECT time, v FROM t LIMIT 0) b ON a.time >= b.time",
        "v");
    assertEmpty(
        "SELECT a.v FROM (SELECT time, v FROM t LIMIT 0) a ASOF LEFT JOIN t b ON a.time >= b.time",
        "v");
    assertResult(
        "SELECT a.v, b.v AS other FROM t a ASOF LEFT JOIN (SELECT time, v FROM t LIMIT 0) b ON a.time >= b.time ORDER BY a.v",
        new String[] {"v", "other"},
        "1,null,",
        "2,null,",
        "10,null,",
        "30,null,");
    assertResult(
        "SELECT a.v, b.v AS other FROM t a ASOF LEFT JOIN (SELECT time, v FROM t WHERE device = 'd1' ORDER BY time LIMIT 1) b ON a.time >= b.time ORDER BY a.v",
        new String[] {"v", "other"},
        "1,1,",
        "2,1,",
        "10,1,",
        "30,1,");
  }

  @Test
  public void testSubqueryPredicates() {
    assertEmpty("SELECT v FROM t WHERE EXISTS (SELECT v FROM t LIMIT 0)", "v");
    assertResult(
        "SELECT v FROM t WHERE NOT EXISTS (SELECT v FROM t LIMIT 0) ORDER BY v",
        new String[] {"v"},
        VALUES);
    assertEmpty("SELECT v FROM t WHERE v IN (SELECT v FROM t LIMIT 0)", "v");
    assertResult(
        "SELECT v FROM t WHERE v NOT IN (SELECT v FROM t LIMIT 0) ORDER BY v",
        new String[] {"v"},
        VALUES);
    assertEmpty("SELECT v FROM t WHERE v > ANY (SELECT v FROM t LIMIT 0)", "v");
    assertResult(
        "SELECT v FROM t WHERE v > ALL (SELECT v FROM t LIMIT 0) ORDER BY v",
        new String[] {"v"},
        VALUES);
    assertResult(
        "SELECT v, (SELECT v FROM t LIMIT 0) AS scalar FROM t ORDER BY v",
        new String[] {"v", "scalar"},
        "1,null,",
        "2,null,",
        "10,null,",
        "30,null,");
    assertEmpty(
        "SELECT a.v FROM t a WHERE EXISTS (SELECT b.v FROM t b WHERE a.device = b.device LIMIT 0)",
        "v");
    assertResult(
        "SELECT a.v FROM t a WHERE NOT EXISTS (SELECT b.v FROM t b WHERE a.device = b.device LIMIT 0) ORDER BY a.v",
        new String[] {"v"},
        VALUES);
    assertResult(
        "SELECT a.v, (SELECT b.v FROM t b WHERE a.device = b.device LIMIT 0) AS scalar FROM t a ORDER BY a.v",
        new String[] {"v", "scalar"},
        "1,null,",
        "2,null,",
        "10,null,",
        "30,null,");
    assertResult(
        "SELECT a.v, (SELECT count(*) FROM t b WHERE a.device = b.device LIMIT 0) AS scalar FROM t a ORDER BY a.v",
        new String[] {"v", "scalar"},
        "1,null,",
        "2,null,",
        "10,null,",
        "30,null,");
  }

  @Test
  public void testEmptyScalarSubqueryTypes() {
    assertResult(
        "SELECT (SELECT time FROM t LIMIT 0) AS ts, (SELECT device FROM t LIMIT 0) AS d, (SELECT v FROM t LIMIT 0) AS v, (SELECT w FROM t LIMIT 0) AS w",
        new String[] {"ts", "d", "v", "w"},
        "null,null,null,null,");
    assertResult(
        "SELECT (SELECT v FROM t WHERE v < 0) AS scalar", new String[] {"scalar"}, "null,");
    assertEmpty("SELECT v FROM t WHERE v = (SELECT v FROM t LIMIT 0)", "v");
  }

  @Test
  public void testCte() {
    for (String materialization : new String[] {"", "MATERIALIZED"}) {
      String cte = "WITH q AS " + materialization + " (SELECT v FROM t LIMIT 0) ";
      assertEmpty(cte + "SELECT v FROM q", "v");
      assertResult(cte + "SELECT count(*) AS c FROM q", new String[] {"c"}, "0,");
      assertEmpty(cte + "SELECT v FROM q UNION ALL SELECT v FROM q", "v");
      assertResult(
          cte + "SELECT v FROM q UNION ALL SELECT v FROM t ORDER BY v", new String[] {"v"}, VALUES);
    }
  }

  @Test
  public void testNoFromAndMetadata() throws SQLException {
    assertEmpty("SELECT 1 AS n LIMIT 0", "n");
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "SELECT time AS ts, device AS d, v AS value, cast(v AS DOUBLE) AS number FROM t LIMIT 0")) {
        ResultSetMetaData metadata = result.getMetaData();
        assertEquals(4, metadata.getColumnCount());
        assertEquals("ts", metadata.getColumnLabel(1));
        assertEquals("d", metadata.getColumnLabel(2));
        assertEquals("value", metadata.getColumnLabel(3));
        assertEquals("number", metadata.getColumnLabel(4));
        assertEquals(Types.TIMESTAMP, metadata.getColumnType(1));
        assertEquals(Types.VARCHAR, metadata.getColumnType(2));
        assertEquals(Types.INTEGER, metadata.getColumnType(3));
        assertEquals(Types.DOUBLE, metadata.getColumnType(4));
        assertFalse(result.next());
      }
    }
  }

  @Test
  public void testInformationSchemaAndDevices() {
    assertEmpty("SELECT table_name FROM information_schema.tables LIMIT 0", "table_name");
    assertEmpty(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name LIMIT 0 OFFSET 1",
        "table_name");
    assertEmpty("SHOW DEVICES FROM t LIMIT 0", "device", "label");
    assertEmpty("SHOW DEVICES FROM t WHERE device = 'd1' LIMIT 0", "device", "label");
    assertEmpty("SHOW DEVICES FROM t WHERE label = 'a' LIMIT 0 OFFSET 1", "device", "label");
  }

  @Test
  public void testInsertSelect() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "INSERT INTO target(time, device, v) SELECT time, device, v FROM t LIMIT 0");
    }
    assertResult("SELECT count(*) AS c FROM target", new String[] {"c"}, "0,");
  }

  @Test
  public void testValidationAndNonzeroLimits() throws SQLException {
    assertResult("SELECT v FROM t ORDER BY v LIMIT ALL", new String[] {"v"}, VALUES);
    assertResult("SELECT v FROM t ORDER BY v", new String[] {"v"}, VALUES);
    assertResult("SELECT v FROM t ORDER BY v LIMIT 1 OFFSET 1", new String[] {"v"}, "2,");
    assertResult("SELECT count(*) AS c FROM t", new String[] {"c"}, "4,");
    assertResult("SELECT 1 AS n", new String[] {"n"}, "1,");
    assertResult("SELECT count(*) AS c FROM (SELECT 1 AS n) q", new String[] {"c"}, "1,");
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      assertThrows(
          SQLException.class, () -> statement.executeQuery("SELECT missing_column FROM t LIMIT 0"));
      assertThrows(
          SQLException.class, () -> statement.executeQuery("SELECT v FROM missing_table LIMIT 0"));
      assertThrows(SQLException.class, () -> statement.executeQuery("SELECT v FROM t LIMIT -1"));
      assertThrows(
          SQLException.class,
          () -> statement.executeQuery("SELECT (SELECT v FROM t) AS scalar FROM t"));
    }
  }

  private static void assertEmpty(String sql, String... header) {
    assertResult(sql, header, NO_ROWS);
  }

  private static void assertResult(String sql, String[] header, String... rows) {
    tableResultSetEqualTest(sql, header, rows, DATABASE);
  }
}
