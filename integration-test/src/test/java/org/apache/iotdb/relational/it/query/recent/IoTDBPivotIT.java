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
import org.apache.iotdb.itbase.runtime.ClusterTestConnection;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTestUnordered;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPivotIT {
  private static final String DATABASE = "pivot_it";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE readings (device STRING TAG, region STRING FIELD, "
              + "status STRING FIELD, temp INT32 FIELD)");
      statement.execute(
          "INSERT INTO readings (time,device,region,status,temp) VALUES "
              + "(1,'d1','north','on',10),(2,'d1','south','off',20),"
              + "(3,'d1',NULL,'on',30)");
      statement.execute("FLUSH");
      statement.execute(
          "CREATE TABLE ranked (device STRING TAG, region STRING FIELD, temp INT32 FIELD)");
      statement.execute(
          "INSERT INTO ranked (time,device,region,temp) VALUES "
              + "(1,'d1','north',1),(2,'d2','north',2),"
              + "(3,'d1','south',3),(4,'d2','south',5)");
      statement.execute("FLUSH");
      statement.execute(
          "CREATE TABLE device_metrics (device_id STRING TAG, region STRING FIELD, "
              + "status STRING FIELD, temperature DOUBLE FIELD, humidity DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO device_metrics "
              + "(time,device_id,region,status,temperature,humidity) VALUES "
              + "(0,'device_1','north','online',20.0,40.0),"
              + "(1,'device_1','north','online',22.0,42.0),"
              + "(2,'device_1','south','offline',30.0,50.0),"
              + "(3,'device_1','south','offline',34.0,54.0),"
              + "(4,'device_1',NULL,'online',26.0,46.0),"
              + "(0,'device_2','north','offline',16.0,60.0),"
              + "(1,'device_2','north','offline',18.0,62.0),"
              + "(2,'device_2','south','offline',28.0,70.0),"
              + "(3,'device_2','south','offline',32.0,NULL),"
              + "(4,'device_2',NULL,'offline',24.0,66.0)");
      statement.execute("FLUSH");
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStaticPivotNullAndPostTransformLimit() {
    tableResultSetEqualTest(
        "SELECT device,north,south,\"NULL\" FROM (PIVOT readings "
            + "ON region IN ('north','south',NULL) USING SUM(temp) GROUP BY device) "
            + "ORDER BY device LIMIT 1",
        new String[] {"device", "north", "south", "NULL"},
        new String[] {"d1,10.0,20.0,30.0,"},
        DATABASE);
  }

  @Test
  public void testSqlStyleCartesianProductAndAliases() {
    tableResultSetEqualTest(
        "SELECT device,n_on_total,n_off_total,s_on_total,s_off_total "
            + "FROM readings PIVOT (SUM(temp) AS total FOR "
            + "region IN ('north' AS n, 'south' AS s) "
            + "status IN ('on','off') GROUP BY device)",
        new String[] {"device", "n_on_total", "n_off_total", "s_on_total", "s_off_total"},
        new String[] {"d1,10.0,null,null,20.0,"},
        DATABASE);
  }

  @Test
  public void testOnExpressionAndMultipleAggregates() {
    tableResultSetEqualTest(
        "SELECT device,north_on,south_off FROM (PIVOT readings "
            + "ON region || '_' || status USING SUM(temp) GROUP BY device)",
        new String[] {"device", "north_on", "south_off"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "SELECT device,north_total,north_n,south_total,south_n "
            + "FROM (PIVOT readings ON region IN ('north','south') "
            + "USING SUM(temp) AS total, COUNT(temp) AS n GROUP BY device)",
        new String[] {"device", "north_total", "north_n", "south_total", "south_n"},
        new String[] {"d1,10.0,1,20.0,1,"},
        DATABASE);
  }

  @Test
  public void testDynamicPivotDiscoversFromMaterializedInput() {
    tableResultSetEqualTest(
        "PIVOT readings ON region USING SUM(temp) GROUP BY device ORDER BY device",
        new String[] {"device", "north", "south"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "WITH p AS (PIVOT readings ON region IN ('north'), status "
            + "USING SUM(temp) GROUP BY device) "
            + "SELECT device,north_off,north_on FROM p ORDER BY device",
        new String[] {"device", "north_off", "north_on"},
        new String[] {"d1,null,10.0,"},
        DATABASE);
  }

  @Test
  public void testDynamicPivotInsideLaterCteCanReadEarlierCte() {
    tableResultSetEqualTest(
        "WITH source AS (SELECT device,region,temp FROM readings), "
            + "p AS (PIVOT source ON region USING SUM(temp) GROUP BY device) "
            + "SELECT device,north,south FROM p ORDER BY device",
        new String[] {"device", "north", "south"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testNestedDynamicPivotUsesPreparedInnerInput() {
    tableResultSetEqualTest(
        "PIVOT (PIVOT readings ON region USING SUM(temp) GROUP BY device) "
            + "ON device USING SUM(north) GROUP BY south",
        new String[] {"south", "d1"},
        new String[] {"20.0,10.0,"},
        DATABASE);
  }

  @Test
  public void testDynamicPivotEmptyAndNullValues() {
    tableResultSetEqualTest(
        "PIVOT (SELECT device, region, temp FROM readings WHERE false) "
            + "ON region USING SUM(temp) GROUP BY device",
        new String[] {"device"},
        new String[0],
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT (SELECT device, CAST(NULL AS STRING) AS empty_value, temp FROM readings) "
            + "ON empty_value USING SUM(temp) GROUP BY device ORDER BY device",
        new String[] {"device"},
        new String[] {"d1,"},
        DATABASE);
  }

  @Test
  public void testDynamicValueStringSortAndStaticNameCollision() {
    tableResultSetEqualTest(
        "PIVOT (SELECT device, temp AS k, temp FROM readings "
            + "UNION ALL SELECT device, 100 AS k, temp FROM readings) "
            + "ON k USING COUNT(temp) GROUP BY device",
        new String[] {"device", "10", "100", "20", "30"},
        new String[] {"d1,1,3,1,1,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT readings ON region IN ('north' AS device,'south' AS device) "
            + "USING SUM(temp) GROUP BY device",
        new String[] {"device", "device_1", "device_2"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testEmptyGlobalPivotAndDynamicWithoutOutputColumns() throws Exception {
    tableResultSetEqualTest(
        "PIVOT (SELECT region,temp FROM readings WHERE false) "
            + "ON region IN ('north') USING COUNT(temp)",
        new String[] {"north"},
        new String[] {"0,"},
        DATABASE);
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      SQLException error =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT (SELECT region,temp FROM readings WHERE false) "
                          + "ON region USING COUNT(temp)"));
      assertTrue(error.getMessage().contains("no columns"));
    }
  }

  @Test
  public void testPreparedParameterInDynamicPivotInput() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection()) {
      Connection directConnection =
          connection instanceof ClusterTestConnection
              ? ((ClusterTestConnection) connection).writeConnection.getUnderlyingConnection()
              : connection;
      try (Statement statement = directConnection.createStatement()) {
        statement.execute("USE " + DATABASE);
      }
      try (PreparedStatement statement =
          directConnection.prepareStatement(
              "PIVOT (SELECT device,region,temp FROM readings WHERE temp >= ?) "
                  + "ON region USING SUM(temp) GROUP BY device")) {
        statement.setInt(1, 20);
        try (ResultSet result = statement.executeQuery()) {
          assertTrue(result.next());
          assertTrue(result.getMetaData().getColumnCount() == 2);
          assertTrue("device".equals(result.getMetaData().getColumnName(1)));
          assertTrue("south".equals(result.getMetaData().getColumnName(2)));
          assertTrue("d1".equals(result.getString(1)));
          assertTrue("20.0".equals(result.getString(2)));
          assertTrue(!result.next());
        }
      }
    }
  }

  @Test
  public void testPivotLimitBoundary() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("SET CONFIGURATION pivot_limit='3'");
      tableResultSetEqualTest(
          "PIVOT readings ON region IN ('north','south') USING SUM(temp) GROUP BY device",
          new String[] {"device", "north", "south"},
          new String[] {"d1,10.0,20.0,"},
          DATABASE);

      statement.execute("SET CONFIGURATION pivot_limit='2'");
      SQLException error =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT readings ON region IN ('north','south') "
                          + "USING SUM(temp) GROUP BY device"));
      assertTrue(error.getMessage().contains("exceeds pivot_limit 2"));
    } finally {
      try (Connection connection = EnvFactory.getEnv().getTableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SET CONFIGURATION pivot_limit='100000'");
      }
    }
  }

  @Test
  public void testDynamicPivotInputBudgetsAndCleanup() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("SET CONFIGURATION pivot_input_buffer_size_in_bytes='1'");
      SQLException byteLimitError =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT readings ON region USING SUM(temp) GROUP BY device"));
      assertTrue(byteLimitError.getMessage().contains("pivot_input_buffer_size_in_bytes"));
      statement.execute("SET CONFIGURATION pivot_input_buffer_size_in_bytes='67108864'");

      statement.execute("SET CONFIGURATION pivot_input_max_rows='1'");
      SQLException rowLimitError =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT readings ON region USING SUM(temp) GROUP BY device"));
      assertTrue(rowLimitError.getMessage().contains("pivot_input_max_rows"));
    } finally {
      try (Connection connection = EnvFactory.getEnv().getTableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SET CONFIGURATION pivot_input_buffer_size_in_bytes='67108864'");
        statement.execute("SET CONFIGURATION pivot_input_max_rows='1000000'");
      }
    }
    tableResultSetEqualTest(
        "PIVOT readings ON region USING SUM(temp) GROUP BY device",
        new String[] {"device", "north", "south"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testDuplicateInValueRejected() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String[] sqls = {
        "PIVOT readings ON region IN ('north','north') USING COUNT(temp) GROUP BY device",
        "PIVOT readings ON region IN ('north' AS n1,'north' AS n2) "
            + "USING COUNT(temp) GROUP BY device",
        "PIVOT readings ON region IN (NULL AS n1,NULL AS n2) " + "USING COUNT(temp) GROUP BY device"
      };
      for (String sql : sqls) {
        SQLException error =
            assertThrows(sql, SQLException.class, () -> statement.executeQuery(sql));
        assertTrue(
            error.getMessage(), error.getMessage().contains("Duplicate value in PIVOT IN list"));
      }
    }
  }

  @Test
  public void testExplain() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "EXPLAIN PIVOT readings ON region IN ('north') "
                  + "USING SUM(temp) GROUP BY device")) {
        assertTrue(result.next());
      }
    }
  }

  @Test
  public void testStaticGlobalAggregation() {
    tableResultSetEqualTest(
        "PIVOT (SELECT region,temp FROM readings) "
            + "ON region IN ('north','south') USING SUM(temp)",
        new String[] {"north", "south"},
        new String[] {"10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testStaticPivotImplicitGroupingIncludesTime() {
    tableResultSetEqualTest(
        "PIVOT readings ON region IN ('north','south') USING SUM(temp) ORDER BY time",
        new String[] {"time", "device", "status", "north", "south"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,on,10.0,null,",
          "1970-01-01T00:00:00.002Z,d1,off,null,20.0,",
          "1970-01-01T00:00:00.003Z,d1,on,null,null,"
        },
        DATABASE);
  }

  @Test
  public void testStaticPivotUnmatchedValueStillProducesColumn() {
    tableResultSetEqualTest(
        "PIVOT readings ON region IN ('east') USING SUM(temp) GROUP BY device",
        new String[] {"device", "east"},
        new String[] {"d1,null,"},
        DATABASE);
  }

  @Test
  public void testOrderByGeneratedColumn() {
    tableResultSetEqualTest(
        "SELECT device,north,south FROM (PIVOT ranked "
            + "ON region IN ('north','south') USING SUM(temp) GROUP BY device) "
            + "ORDER BY south DESC LIMIT 1",
        new String[] {"device", "north", "south"},
        new String[] {"d2,2.0,5.0,"},
        DATABASE);
  }

  @Test
  public void testRecursiveCteDynamicPivotReportsUnsupported() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      SQLException error =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "WITH RECURSIVE r AS (PIVOT readings ON region USING SUM(temp) "
                          + "GROUP BY device) SELECT * FROM r"));
      assertTrue(error.getMessage().toLowerCase(Locale.ENGLISH).contains("cte"));
    }
  }

  @Test
  public void testDynamicPivotDiscoveryValueBudgetAndCleanup() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("DROP TABLE IF EXISTS wide_values");
      statement.execute(
          "CREATE TABLE wide_values "
              + "(device STRING TAG, region STRING FIELD, temp INT32 FIELD)");
      for (int batch = 0; batch < 10; batch++) {
        StringBuilder insert =
            new StringBuilder("INSERT INTO wide_values (time,device,region,temp) VALUES ");
        for (int i = 0; i < 100; i++) {
          int id = batch * 100 + i;
          if (i > 0) {
            insert.append(',');
          }
          insert
              .append('(')
              .append(id)
              .append(",'d1','r")
              .append(String.format("%04d", id))
              .append('_')
              .append("x".repeat(60))
              .append("',")
              .append(id)
              .append(')');
        }
        statement.execute(insert.toString());
      }
      statement.execute("FLUSH");
      statement.execute("SET CONFIGURATION pivot_input_buffer_size_in_bytes='196608'");
      SQLException error =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT wide_values ON region USING SUM(temp) GROUP BY device"));
      assertTrue(error.getMessage().contains("pivot_input_buffer_size_in_bytes"));
    } finally {
      try (Connection connection = EnvFactory.getEnv().getTableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("USE " + DATABASE);
        statement.execute("SET CONFIGURATION pivot_input_buffer_size_in_bytes='67108864'");
        statement.execute("DROP TABLE IF EXISTS wide_values");
      }
    }
    tableResultSetEqualTest(
        "PIVOT readings ON region USING SUM(temp) GROUP BY device",
        new String[] {"device", "north", "south"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testMultipleDynamicPivotsUseIndependentRowBudgets() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute("SET CONFIGURATION pivot_input_max_rows='3'");
      tableResultSetEqualTestUnordered(
          "SELECT south,d1 FROM (PIVOT (PIVOT readings ON region USING SUM(temp)) "
              + "ON device USING SUM(north) GROUP BY south)",
          new String[] {"south", "d1"},
          new String[] {"null,10.0,", "20.0,null,"},
          DATABASE);
    } finally {
      try (Connection connection = EnvFactory.getEnv().getTableConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SET CONFIGURATION pivot_input_max_rows='1000000'");
      }
    }
  }

  @Test
  public void testAnalysisFailureAfterDiscoveryReleasesResources() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      SQLException error =
          assertThrows(
              SQLException.class,
              () ->
                  statement.executeQuery(
                      "PIVOT readings ON region USING COUNT(DISTINCT temp) GROUP BY device"));
      assertTrue(error.getMessage().contains("DISTINCT"));
    }
    tableResultSetEqualTest(
        "PIVOT readings ON region USING SUM(temp) GROUP BY device",
        new String[] {"device", "north", "south"},
        new String[] {"d1,10.0,20.0,"},
        DATABASE);
  }

  @Test
  public void testRequirementStaticDynamicAndMixedPivot() {
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north','south') "
            + "USING AVG(temperature) AS avg_temp GROUP BY device_id ORDER BY device_id",
        new String[] {"device_id", "north_avg_temp", "south_avg_temp"},
        new String[] {"device_1,21.0,32.0,", "device_2,17.0,30.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region "
            + "USING AVG(temperature) AS avg_temp GROUP BY device_id ORDER BY device_id",
        new String[] {"device_id", "north_avg_temp", "south_avg_temp"},
        new String[] {"device_1,21.0,32.0,", "device_2,17.0,30.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north' AS n,'south' AS s), status "
            + "USING AVG(temperature) AS avg_temp GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id",
          "n_offline_avg_temp",
          "n_online_avg_temp",
          "s_offline_avg_temp",
          "s_online_avg_temp"
        },
        new String[] {"device_1,null,21.0,32.0,null,", "device_2,17.0,null,30.0,null,"},
        DATABASE);
  }

  @Test
  public void testRequirementOnExpressionNullSemantics() {
    tableResultSetEqualTest(
        "PIVOT (SELECT device_id,region,status,temperature,"
            + "CAST(NULL AS STRING) AS empty_key FROM device_metrics) "
            + "ON CASE WHEN region IS NULL OR status IS NULL THEN empty_key "
            + "ELSE region || '_' || status END USING AVG(temperature) AS avg_temp "
            + "GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id", "north_offline_avg_temp", "north_online_avg_temp", "south_offline_avg_temp"
        },
        new String[] {"device_1,null,21.0,32.0,", "device_2,17.0,null,30.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region || '_' || status "
            + "USING AVG(temperature) AS avg_temp GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id",
          "_offline_avg_temp",
          "_online_avg_temp",
          "north_offline_avg_temp",
          "north_online_avg_temp",
          "south_offline_avg_temp"
        },
        new String[] {"device_1,null,26.0,null,21.0,32.0,", "device_2,24.0,null,17.0,null,30.0,"},
        DATABASE);
  }

  @Test
  public void testRequirementMultipleAggregatesAndGlobalAggregation() {
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north' AS north,'south' AS south) "
            + "USING AVG(temperature) AS avg_temp, MAX(temperature) AS max_temp, "
            + "AVG(humidity) AS avg_humidity GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id",
          "north_avg_temp",
          "north_max_temp",
          "north_avg_humidity",
          "south_avg_temp",
          "south_max_temp",
          "south_avg_humidity"
        },
        new String[] {
          "device_1,21.0,22.0,41.0,32.0,34.0,52.0,", "device_2,17.0,18.0,61.0,30.0,32.0,70.0,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north','south') "
            + "USING AVG(temperature), MAX(temperature) GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id",
          "north_avg(temperature)",
          "north_max(temperature)",
          "south_avg(temperature)",
          "south_max(temperature)"
        },
        new String[] {"device_1,21.0,22.0,32.0,34.0,", "device_2,17.0,18.0,30.0,32.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT (SELECT region,temperature FROM device_metrics) "
            + "ON region IN ('north','south') USING AVG(temperature) AS avg_temp",
        new String[] {"north_avg_temp", "south_avg_temp"},
        new String[] {"19.0,31.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT (SELECT region,temperature FROM device_metrics WHERE false) "
            + "ON region IN ('north','south') USING AVG(temperature) AS avg_temp",
        new String[] {"north_avg_temp", "south_avg_temp"},
        new String[] {"null,null,"},
        DATABASE);
  }

  @Test
  public void testRequirementImplicitGroupingAndEmptyStaticInput() {
    tableResultSetEqualTest(
        "PIVOT (SELECT device_id,region,temperature FROM device_metrics) "
            + "ON region IN ('north' AS north,'south' AS south) "
            + "USING AVG(temperature) AS avg_temp ORDER BY device_id",
        new String[] {"device_id", "north_avg_temp", "south_avg_temp"},
        new String[] {"device_1,21.0,32.0,", "device_2,17.0,30.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT (SELECT device_id,region,temperature FROM device_metrics WHERE false) "
            + "ON region IN ('north','south') USING AVG(temperature) AS avg_temp "
            + "GROUP BY device_id",
        new String[] {"device_id", "north_avg_temp", "south_avg_temp"},
        new String[0],
        DATABASE);
  }

  @Test
  public void testRequirementMultipleDynamicOnItemsWithEmptyValueSet() {
    tableResultSetEqualTest(
        "PIVOT (SELECT device_id,region,CAST(NULL AS STRING) AS empty_status,temperature "
            + "FROM device_metrics WHERE region IS NOT NULL) "
            + "ON region, empty_status USING AVG(temperature) AS avg_temp "
            + "GROUP BY device_id ORDER BY device_id",
        new String[] {"device_id"},
        new String[] {"device_1,", "device_2,"},
        DATABASE);
  }

  @Test
  public void testRequirementDuplicateGeneratedColumnNames() {
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north' AS north,'south' AS south) "
            + "USING AVG(temperature) AS metric, MAX(temperature) AS metric "
            + "GROUP BY device_id ORDER BY device_id",
        new String[] {
          "device_id", "north_metric", "north_metric_1", "south_metric", "south_metric_1"
        },
        new String[] {"device_1,21.0,22.0,32.0,34.0,", "device_2,17.0,18.0,30.0,32.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north' AS r,'south' AS r) "
            + "USING AVG(temperature) AS avg_temp GROUP BY device_id ORDER BY device_id",
        new String[] {"device_id", "r_avg_temp", "r_avg_temp_1"},
        new String[] {"device_1,21.0,32.0,", "device_2,17.0,30.0,"},
        DATABASE);
  }

  @Test
  public void testRequirementUsingAndGroupByConstraints() throws Exception {
    tableResultSetEqualTest(
        "PIVOT device_metrics ON region IN ('north','south') "
            + "USING AVG(temperature) * 2 AS avg_temp_x2 "
            + "GROUP BY device_id ORDER BY device_id",
        new String[] {"device_id", "north_avg_temp_x2", "south_avg_temp_x2"},
        new String[] {"device_1,42.0,64.0,", "device_2,34.0,60.0,"},
        DATABASE);
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String[][] cases = {
        {
          "PIVOT device_metrics ON region IN ('north') "
              + "USING AVG(temperature) + MAX(temperature) AS combined GROUP BY device_id",
          "exactly one aggregate"
        },
        {
          "PIVOT device_metrics ON region IN ('north') "
              + "USING AVG(temperature) + humidity AS combined GROUP BY device_id",
          "cannot reference input columns outside its aggregate"
        },
        {
          "PIVOT device_metrics ON region IN ('north') USING temperature GROUP BY device_id",
          "exactly one aggregate"
        },
        {
          "PIVOT device_metrics ON region IN ('north') "
              + "USING AVG(temperature) GROUP BY upper(device_id)",
          "GROUP BY accepts only input column references"
        }
      };
      for (String[] testCase : cases) {
        SQLException error =
            assertThrows(
                testCase[0], SQLException.class, () -> statement.executeQuery(testCase[0]));
        assertTrue(
            error.getMessage(),
            error
                .getMessage()
                .toLowerCase(Locale.ENGLISH)
                .contains(testCase[1].toLowerCase(Locale.ENGLISH)));
      }
      assertThrows(
          SQLException.class,
          () ->
              statement.executeQuery(
                  "SELECT * FROM device_metrics "
                      + "PIVOT (AVG(temperature) FOR region GROUP BY device_id)"));
    }
  }
}
