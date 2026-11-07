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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTestUnordered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBUnpivotIT {
  private static final String DATABASE = "unpivot_it";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE TABLE readings (device STRING TAG, a INT32 FIELD, b INT32 FIELD, "
              + "c INT32 FIELD, d INT32 FIELD)");
      statement.execute(
          "INSERT INTO readings (time, device, a, b, c, d) VALUES "
              + "(1, 'd1', 10, NULL, 30, 40), (2, 'd1', 20, 21, 31, 41)");
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
      statement.execute(
          "CREATE TABLE device_snapshots (device_id STRING TAG, "
              + "temperature_current DOUBLE FIELD, humidity_current DOUBLE FIELD, "
              + "temperature_previous DOUBLE FIELD, humidity_previous DOUBLE FIELD)");
      statement.execute(
          "INSERT INTO device_snapshots "
              + "(time,device_id,temperature_current,humidity_current,"
              + "temperature_previous,humidity_previous) VALUES "
              + "(0,'device_1',20.0,40.0,18.0,38.0),(0,'device_2',25.0,NULL,NULL,NULL)");
      statement.execute("FLUSH");
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSqlStyleNullFilterAndPostTransformLimit() {
    tableResultSetEqualTestUnordered(
        "SELECT time, device, metric, val FROM readings UNPIVOT "
            + "(val FOR metric IN (a AS first, b AS second))",
        new String[] {"time", "device", "metric", "val"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,first,10,",
          "1970-01-01T00:00:00.002Z,d1,first,20,",
          "1970-01-01T00:00:00.002Z,d1,second,21,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "SELECT time, device, metric, val FROM readings UNPIVOT "
            + "(val FOR metric IN (a AS first, b AS second)) ORDER BY time, metric LIMIT 2",
        new String[] {"time", "device", "metric", "val"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,first,10,", "1970-01-01T00:00:00.002Z,d1,first,20,"
        },
        DATABASE);
  }

  @Test
  public void testMultiValueAndIncludeNulls() {
    tableResultSetEqualTestUnordered(
        "SELECT time, device, metric, v1, v2 FROM readings "
            + "UNPIVOT INCLUDE NULLS ((v1,v2) FOR metric IN "
            + "((a,b) AS ab, (c,d) AS cd))",
        new String[] {"time", "device", "metric", "v1", "v2"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,ab,10,null,",
          "1970-01-01T00:00:00.001Z,d1,cd,30,40,",
          "1970-01-01T00:00:00.002Z,d1,ab,20,21,",
          "1970-01-01T00:00:00.002Z,d1,cd,31,41,"
        },
        DATABASE);
  }

  @Test
  public void testMultiValueQualifiedSourceColumns() {
    tableResultSetEqualTestUnordered(
        "UNPIVOT readings ON (readings.a, readings.b) AS ab, "
            + "(readings.c, readings.d) AS cd INTO NAME metric VALUES v1, v2",
        new String[] {"time", "device", "metric", "v1", "v2"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,cd,30,40,",
          "1970-01-01T00:00:00.002Z,d1,ab,20,21,",
          "1970-01-01T00:00:00.002Z,d1,cd,31,41,"
        },
        DATABASE);
  }

  @Test
  public void testDefaultMultiValueNullFilteringAndScalarSource() {
    tableResultSetEqualTest(
        "SELECT time,metric,v1,v2 FROM readings UNPIVOT "
            + "((v1,v2) FOR metric IN ((a,b) AS ab, (c,d) AS cd)) "
            + "ORDER BY time,metric",
        new String[] {"time", "metric", "v1", "v2"},
        new String[] {
          "1970-01-01T00:00:00.001Z,cd,30,40,",
          "1970-01-01T00:00:00.002Z,ab,20,21,",
          "1970-01-01T00:00:00.002Z,cd,31,41,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "SELECT time,metric,val FROM (UNPIVOT readings "
            + "ON a + 1 AS incremented, b INTO NAME metric VALUE val) "
            + "ORDER BY time,metric",
        new String[] {"time", "metric", "val"},
        new String[] {
          "1970-01-01T00:00:00.001Z,incremented,11,",
          "1970-01-01T00:00:00.002Z,b,21,",
          "1970-01-01T00:00:00.002Z,incremented,21,"
        },
        DATABASE);
  }

  @Test
  public void testSimplifiedColumnsAndCte() {
    tableResultSetEqualTestUnordered(
        "WITH expanded AS (UNPIVOT readings ON COLUMNS(* EXCLUDE (time,device)) "
            + "INTO NAME metric VALUE val) SELECT time,device,metric,val FROM expanded",
        new String[] {"time", "device", "metric", "val"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,a,10,",
          "1970-01-01T00:00:00.001Z,d1,c,30,",
          "1970-01-01T00:00:00.001Z,d1,d,40,",
          "1970-01-01T00:00:00.002Z,d1,a,20,",
          "1970-01-01T00:00:00.002Z,d1,b,21,",
          "1970-01-01T00:00:00.002Z,d1,c,31,",
          "1970-01-01T00:00:00.002Z,d1,d,41,"
        },
        DATABASE);
  }

  @Test
  public void testColumnsStarAndCommonNumericType() {
    tableResultSetEqualTest(
        "UNPIVOT (SELECT a,b FROM readings) ON COLUMNS(*) ORDER BY name,value",
        new String[] {"name", "value"},
        new String[] {"a,10,", "a,20,", "b,21,"},
        DATABASE);
    tableResultSetEqualTest(
        "UNPIVOT (SELECT CAST(a AS INT64) AS a64, CAST(c AS FLOAT) AS cf "
            + "FROM readings) ON a64,cf INTO NAME metric VALUE val ORDER BY metric,val",
        new String[] {"metric", "val"},
        new String[] {"a64,10.0,", "a64,20.0,", "cf,30.0,", "cf,31.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "UNPIVOT (SELECT CAST(a AS INT64) AS i64, CAST(b AS INT32) AS i32 "
            + "FROM readings WHERE b IS NOT NULL) ON i64,i32 "
            + "INTO NAME metric VALUE val ORDER BY metric,val",
        new String[] {"metric", "val"},
        new String[] {"i32,21,", "i64,20,"},
        DATABASE);
  }

  @Test
  public void testSemanticErrors() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      for (String sql :
          new String[] {
            "UNPIVOT readings ON a, device",
            "UNPIVOT readings ON a INTO NAME device VALUE val",
            "UNPIVOT readings ON COLUMNS(* EXCLUDE (time,device,a,b,c,d)) "
                + "INTO NAME n VALUE val",
            "UNPIVOT readings ON (a,b),(c) INTO NAME n VALUES v1,v2",
            "UNPIVOT readings ON a INCLUDE NULLS"
          }) {
        assertThrows(sql, SQLException.class, () -> statement.executeQuery(sql));
      }
      String[][] semanticCases = {
        {
          "UNPIVOT readings ON (a,b) AS ab,(c,d) AS cd INTO NAME n VALUE v",
          "number of source columns in each UNPIVOT group must match"
        },
        {
          "UNPIVOT readings ON (a + b,c) AS ab,(c,d) AS cd " + "INTO NAME n VALUES v1,v2",
          "do not support expressions or casts"
        },
        {
          "UNPIVOT readings ON a + b INTO NAME n VALUE v", "must reference exactly one input column"
        },
        {
          "UNPIVOT readings ON COLUMNS(* EXCLUDE (time,device)) " + "INTO NAME n VALUES v1,v2",
          "cannot be used to infer source column groups"
        },
        {
          "UNPIVOT readings ON a INTO NAME val VALUE val",
          "Duplicate column name in UNPIVOT output: val"
        },
        {
          "UNPIVOT readings ON (a,b) AS ab,(c,d) AS cd INTO NAME n VALUES v,v",
          "Duplicate column name in UNPIVOT output: v"
        },
        {
          "UNPIVOT readings ON a,device INTO NAME n VALUE v",
          "UNPIVOT source types are not compatible"
        },
        {
          "UNPIVOT readings ON COLUMNS(* EXCLUDE (missing)) INTO NAME n VALUE v",
          "UNPIVOT source column does not exist: missing"
        },
        {
          "UNPIVOT (SELECT * FROM readings x JOIN readings y ON x.device = y.device) "
              + "ON COLUMNS(* EXCLUDE (a)) INTO NAME n VALUE v",
          "UNPIVOT source column is ambiguous: a"
        }
      };
      for (String[] testCase : semanticCases) {
        SQLException error =
            assertThrows(
                testCase[0], SQLException.class, () -> statement.executeQuery(testCase[0]));
        assertTrue(error.getMessage(), error.getMessage().contains(testCase[1]));
      }
    }
  }

  @Test
  public void testExplainShowsUnpivot() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "EXPLAIN SELECT * FROM readings UNPIVOT (val FOR metric IN (a,b))")) {
        StringBuilder plan = new StringBuilder();
        while (result.next()) {
          plan.append(result.getString(1));
        }
        assertTrue(plan.toString().contains("Unpivot"));
      }
    }
  }

  @Test
  public void testColumnsStarOnJoinWithDuplicateNames() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "UNPIVOT (SELECT * FROM (SELECT a FROM readings) x "
                  + "JOIN (SELECT a FROM readings) y ON x.a = y.a) "
                  + "ON COLUMNS(*) INTO NAME metric VALUE val")) {
        List<String> metrics = new ArrayList<>();
        List<String> values = new ArrayList<>();
        while (result.next()) {
          metrics.add(result.getString(1));
          values.add(result.getString(2));
        }
        assertEquals(4, metrics.size());
        assertTrue(metrics.stream().allMatch("a"::equals));
        Collections.sort(values);
        assertEquals(List.of("10", "10", "20", "20"), values);
      }
    }
  }

  @Test
  public void testSingleValueIncludeNulls() {
    tableResultSetEqualTest(
        "SELECT time,metric,val FROM readings UNPIVOT INCLUDE NULLS "
            + "(val FOR metric IN (a,b)) ORDER BY time,metric",
        new String[] {"time", "metric", "val"},
        new String[] {
          "1970-01-01T00:00:00.001Z,a,10,",
          "1970-01-01T00:00:00.001Z,b,null,",
          "1970-01-01T00:00:00.002Z,a,20,",
          "1970-01-01T00:00:00.002Z,b,21,"
        },
        DATABASE);
  }

  @Test
  public void testEmptyInputUnpivot() {
    tableResultSetEqualTest(
        "UNPIVOT (SELECT a,b FROM readings WHERE false) " + "ON a,b INTO NAME metric VALUE val",
        new String[] {"metric", "val"},
        new String[0],
        DATABASE);
  }

  @Test
  public void testMultiValueDefaultGroupNames() {
    tableResultSetEqualTest(
        "SELECT time,metric,v1,v2 FROM readings UNPIVOT "
            + "((v1,v2) FOR metric IN ((a,b),(c,d))) ORDER BY time,metric",
        new String[] {"time", "metric", "v1", "v2"},
        new String[] {
          "1970-01-01T00:00:00.001Z,c_d,30,40,",
          "1970-01-01T00:00:00.002Z,a_b,20,21,",
          "1970-01-01T00:00:00.002Z,c_d,31,41,"
        },
        DATABASE);
  }

  @Test
  public void testExplainJsonKeepsUnpivotAttributes() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "EXPLAIN (FORMAT JSON) SELECT * FROM readings UNPIVOT (val FOR metric IN (a,b))")) {
        assertTrue(result.next());
        String json = result.getString(1);
        assertTrue(json, json.contains("\"Sources\""));
        assertTrue(json, json.contains("\"IncludeNulls\""));
        assertTrue(json, json.contains("UnpivotNode"));
      }
    }
  }

  @Test
  public void testRequirementSingleValueSimplifiedAndSqlStyle() {
    String[] header = {"time", "device_id", "measurement", "value"};
    String[] rows = {
      "1970-01-01T00:00:00.000Z,device_1,humidity,40.0,",
      "1970-01-01T00:00:00.000Z,device_1,temperature,20.0,",
      "1970-01-01T00:00:00.001Z,device_1,humidity,42.0,",
      "1970-01-01T00:00:00.001Z,device_1,temperature,22.0,",
      "1970-01-01T00:00:00.002Z,device_1,humidity,50.0,",
      "1970-01-01T00:00:00.002Z,device_1,temperature,30.0,",
      "1970-01-01T00:00:00.003Z,device_1,humidity,54.0,",
      "1970-01-01T00:00:00.003Z,device_1,temperature,34.0,",
      "1970-01-01T00:00:00.004Z,device_1,humidity,46.0,",
      "1970-01-01T00:00:00.004Z,device_1,temperature,26.0,"
    };
    tableResultSetEqualTest(
        "UNPIVOT (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_1') ON temperature,humidity "
            + "INTO NAME measurement VALUE value ORDER BY time,measurement",
        header,
        rows,
        DATABASE);
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_1') AS source_data "
            + "UNPIVOT (value FOR measurement IN (temperature,humidity)) "
            + "ORDER BY time,measurement",
        header,
        rows,
        DATABASE);
  }

  @Test
  public void testRequirementComputedSourceAndAliases() {
    tableResultSetEqualTest(
        "UNPIVOT (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_1') ON temperature * 1.8 + 32 AS temperature_f, "
            + "humidity AS humidity_pct INTO NAME measurement VALUE value "
            + "ORDER BY time,measurement",
        new String[] {"time", "device_id", "measurement", "value"},
        new String[] {
          "1970-01-01T00:00:00.000Z,device_1,humidity_pct,40.0,",
          "1970-01-01T00:00:00.000Z,device_1,temperature_f,68.0,",
          "1970-01-01T00:00:00.001Z,device_1,humidity_pct,42.0,",
          "1970-01-01T00:00:00.001Z,device_1,temperature_f,71.6,",
          "1970-01-01T00:00:00.002Z,device_1,humidity_pct,50.0,",
          "1970-01-01T00:00:00.002Z,device_1,temperature_f,86.0,",
          "1970-01-01T00:00:00.003Z,device_1,humidity_pct,54.0,",
          "1970-01-01T00:00:00.003Z,device_1,temperature_f,93.2,",
          "1970-01-01T00:00:00.004Z,device_1,humidity_pct,46.0,",
          "1970-01-01T00:00:00.004Z,device_1,temperature_f,78.80000000000001,"
        },
        DATABASE);
  }

  @Test
  public void testRequirementSqlStyleColumnsExclude() {
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_1' AND time = 0) AS source_data "
            + "UNPIVOT (value FOR measurement IN (COLUMNS(* EXCLUDE (time,device_id)))) "
            + "ORDER BY measurement",
        new String[] {"time", "device_id", "measurement", "value"},
        new String[] {
          "1970-01-01T00:00:00.000Z,device_1,humidity,40.0,",
          "1970-01-01T00:00:00.000Z,device_1,temperature,20.0,"
        },
        DATABASE);
  }

  @Test
  public void testRequirementMultiValueSnapshots() {
    tableResultSetEqualTest(
        "UNPIVOT device_snapshots ON (temperature_current,humidity_current) AS current, "
            + "(temperature_previous,humidity_previous) AS previous "
            + "INTO NAME sample VALUES temperature,humidity ORDER BY device_id,sample",
        new String[] {"time", "device_id", "sample", "temperature", "humidity"},
        new String[] {
          "1970-01-01T00:00:00.000Z,device_1,current,20.0,40.0,",
          "1970-01-01T00:00:00.000Z,device_1,previous,18.0,38.0,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "SELECT * FROM device_snapshots UNPIVOT INCLUDE NULLS "
            + "((temperature,humidity) FOR sample IN "
            + "((temperature_current,humidity_current) AS current, "
            + "(temperature_previous,humidity_previous) AS previous)) "
            + "ORDER BY device_id,sample",
        new String[] {"time", "device_id", "sample", "temperature", "humidity"},
        new String[] {
          "1970-01-01T00:00:00.000Z,device_1,current,20.0,40.0,",
          "1970-01-01T00:00:00.000Z,device_1,previous,18.0,38.0,",
          "1970-01-01T00:00:00.000Z,device_2,current,25.0,null,",
          "1970-01-01T00:00:00.000Z,device_2,previous,null,null,"
        },
        DATABASE);
    tableResultSetEqualTest(
        "UNPIVOT device_snapshots ON (temperature_current,humidity_current), "
            + "(temperature_previous,humidity_previous) AS previous "
            + "INTO NAME sample VALUES temperature,humidity ORDER BY device_id,sample",
        new String[] {"time", "device_id", "sample", "temperature", "humidity"},
        new String[] {
          "1970-01-01T00:00:00.000Z,device_1,previous,18.0,38.0,",
          "1970-01-01T00:00:00.000Z,device_1,temperature_current_humidity_current,20.0,40.0,"
        },
        DATABASE);
  }

  @Test
  public void testRequirementOrderByValueDescLimit() {
    tableResultSetEqualTest(
        "UNPIVOT (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_1') ON temperature,humidity "
            + "INTO NAME measurement VALUE value ORDER BY value DESC LIMIT 3",
        new String[] {"time", "device_id", "measurement", "value"},
        new String[] {
          "1970-01-01T00:00:00.003Z,device_1,humidity,54.0,",
          "1970-01-01T00:00:00.002Z,device_1,humidity,50.0,",
          "1970-01-01T00:00:00.004Z,device_1,humidity,46.0,"
        },
        DATABASE);
  }

  @Test
  public void testRequirementSingleValueNullHandling() {
    tableResultSetEqualTest(
        "UNPIVOT (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_2' AND time = 3) ON temperature,humidity "
            + "INTO NAME measurement VALUE value ORDER BY measurement",
        new String[] {"time", "device_id", "measurement", "value"},
        new String[] {"1970-01-01T00:00:00.003Z,device_2,temperature,32.0,"},
        DATABASE);
    tableResultSetEqualTest(
        "SELECT * FROM (SELECT time,device_id,temperature,humidity FROM device_metrics "
            + "WHERE device_id = 'device_2' AND time = 3) AS source_data "
            + "UNPIVOT INCLUDE NULLS (value FOR measurement IN (temperature,humidity)) "
            + "ORDER BY measurement",
        new String[] {"time", "device_id", "measurement", "value"},
        new String[] {
          "1970-01-01T00:00:00.003Z,device_2,humidity,null,",
          "1970-01-01T00:00:00.003Z,device_2,temperature,32.0,"
        },
        DATABASE);
  }
}
