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
package org.apache.iotdb.db.it.query;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

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
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for alias series query operations. This test covers: - Basic SELECT queries on
 * alias series (should display alias path in header) - SELECT queries with WHERE clauses (using
 * both full paths and relative paths) - Aggregation queries - ALIGN BY DEVICE queries -
 * Multi-device queries - Mixed queries (alias series and physical series) - Verification that
 * disabled series are not shown in results
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasSeriesQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // Create databases
      statement.execute("CREATE DATABASE root.db");
      statement.execute("CREATE DATABASE root.view");

      // Create physical time series
      statement.execute(
          "CREATE TIMESERIES root.db.d1.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.db.d1.pressure WITH DATATYPE=DOUBLE, ENCODING=GORILLA, COMPRESSION=LZ4");
      statement.execute(
          "CREATE TIMESERIES root.db.d2.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY");

      // Insert data
      statement.execute(
          "INSERT INTO root.db.d1(timestamp, temperature, pressure) VALUES (1509465600012, 2.1, 1013.25)");
      statement.execute(
          "INSERT INTO root.db.d1(timestamp, temperature, pressure) VALUES (1509465601000, 2.5, 1014.50)");
      statement.execute(
          "INSERT INTO root.db.d1(timestamp, temperature, pressure) VALUES (1509465602000, 3.0, 1015.75)");
      statement.execute(
          "INSERT INTO root.db.d2(timestamp, temperature) VALUES (1509465600012, 15.5)");

      // Create alias series (RENAME operation)
      statement.execute(
          "ALTER TIMESERIES root.db.d1.temperature RENAME TO root.view.d1.temperature");

      // Create additional time series with TAGS for GROUP BY TAGS testing
      statement.execute(
          "CREATE TIMESERIES root.db.d3.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY TAGS('location'='beijing', 'sensor_type'='sensor')");
      statement.execute(
          "CREATE TIMESERIES root.db.d4.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY TAGS('location'='shanghai', 'sensor_type'='sensor')");
      statement.execute(
          "CREATE TIMESERIES root.db.d5.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSION=SNAPPY TAGS('location'='beijing', 'sensor_type'='sensor')");

      // Insert data for tagged series
      statement.execute(
          "INSERT INTO root.db.d3(timestamp, temperature) VALUES (1509465600012, 20.1)");
      statement.execute(
          "INSERT INTO root.db.d3(timestamp, temperature) VALUES (1509465601000, 20.5)");
      statement.execute(
          "INSERT INTO root.db.d4(timestamp, temperature) VALUES (1509465600012, 25.1)");
      statement.execute(
          "INSERT INTO root.db.d4(timestamp, temperature) VALUES (1509465601000, 25.5)");
      statement.execute(
          "INSERT INTO root.db.d5(timestamp, temperature) VALUES (1509465600012, 22.1)");
      statement.execute(
          "INSERT INTO root.db.d5(timestamp, temperature) VALUES (1509465601000, 22.5)");

      // Create alias series from tagged series
      statement.execute(
          "ALTER TIMESERIES root.db.d3.temperature RENAME TO root.view.d3.temperature");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Basic SELECT queries on alias series
  // ============================================================================

  @Test
  public void testBasicSelectAliasSeries() {
    // Query alias series - header should show alias path (root.view.d1.temperature)
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {"1509465600012,2.1,", "1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest("SELECT temperature FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSelectPhysicalSeries() {
    // Query physical series that is not renamed - should work normally
    String expectedHeader = "Time,root.db.d1.pressure,";
    String[] retArray =
        new String[] {"1509465600012,1013.25,", "1509465601000,1014.5,", "1509465602000,1015.75,"};

    resultSetEqualTest("SELECT pressure FROM root.db.d1", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: SELECT queries with WHERE clauses
  // ============================================================================

  @Test
  public void testSelectAliasSeriesWithWhereClause() {
    // WHERE clause using relative path
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature > 2.5", expectedHeader, retArray);
  }

  @Test
  public void testSelectAliasSeriesWithWhereClauseFullPath() {
    // WHERE clause using full path
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465600012,2.1,", "1509465601000,2.5,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE root.view.d1.temperature <= 2.5",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSelectWithTimeFilter() {
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE time >= 1509465601000",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Aggregation queries
  // ============================================================================

  @Test
  public void testAggregationQuery() {
    String expectedHeader = "COUNT(root.view.d1.temperature),";
    String[] retArray = new String[] {"3,"};

    resultSetEqualTest("SELECT COUNT(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testAvgQuery() {
    String expectedHeader = "AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.5333333015441895,"};

    resultSetEqualTest("SELECT AVG(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testMaxQuery() {
    String expectedHeader = "max_value(root.view.d1.temperature),";
    String[] retArray = new String[] {"3.0,"};

    resultSetEqualTest("SELECT max_value(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testMinQuery() {
    String expectedHeader = "min_value(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.1,"};

    resultSetEqualTest("SELECT min_value(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSumQuery() {
    String expectedHeader = "SUM(root.view.d1.temperature),";
    String[] retArray = new String[] {"7.599999904632568,"};

    resultSetEqualTest("SELECT SUM(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: Wildcard queries
  // ============================================================================

  @Test
  public void testWildcardQuery() {
    // SELECT * should include alias series
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {"1509465600012,2.1,", "1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest("SELECT * FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testWildcardPathQuery() {
    // Query with wildcard path
    String expectedHeader = "Time,root.view.d1.temperature,root.view.d3.temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,2.1,20.1,", "1509465601000,2.5,20.5,", "1509465602000,3.0,null,"
        };

    resultSetEqualTest("SELECT * FROM root.view.**", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: ORDER BY queries
  // ============================================================================

  @Test
  public void testOrderByTimeDesc() {
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {"1509465602000,3.0,", "1509465601000,2.5,", "1509465600012,2.1,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 ORDER BY TIME DESC", expectedHeader, retArray);
  }

  @Test
  public void testOrderByTimeAsc() {
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {"1509465600012,2.1,", "1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 ORDER BY TIME ASC", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: Pagination queries (LIMIT/OFFSET)
  // ============================================================================

  @Test
  public void testLimitQuery() {
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465600012,2.1,", "1509465601000,2.5,"};

    resultSetEqualTest("SELECT temperature FROM root.view.d1 LIMIT 2", expectedHeader, retArray);
  }

  @Test
  public void testLimitOffsetQuery() {
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 LIMIT 2 OFFSET 1", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: Multi-series queries
  // ============================================================================

  @Test
  public void testSelectMultipleSeries() {
    String expectedHeader = "Time,root.view.d1.temperature,root.db.d1.pressure,";
    String[] retArray =
        new String[] {
          "1509465600012,2.1,1013.25,", "1509465601000,2.5,1014.5,", "1509465602000,3.0,1015.75,"
        };

    resultSetEqualTest(
        "SELECT temperature, pressure FROM root.view.d1, root.db.d1", expectedHeader, retArray);
  }

  // ============================================================================
  // Test: GROUP BY TIME queries
  // ============================================================================

  @Test
  public void testGroupByTimeQuery() {
    String expectedHeader = "Time,AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"1509465600000,2.299999952316284,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 GROUP BY ([1509465600000, 1509465603000), 2s)",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: ALIGN BY DEVICE queries
  // ============================================================================

  @Test
  public void testAlignByDeviceWithAliasSeries() {
    // Query alias series using ALIGN BY DEVICE
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,root.view.d1,2.1,",
          "1509465601000,root.view.d1,2.5,",
          "1509465602000,root.view.d1,3.0,"
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 ALIGN BY DEVICE", expectedHeader, retArray);
  }

  @Test
  public void testAlignByDeviceMultipleDevices() {
    // Query multiple devices including alias series
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,root.db.d2,15.5,",
          "1509465600012,root.view.d1,2.1,",
          "1509465601000,root.view.d1,2.5,",
          "1509465602000,root.view.d1,3.0,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1, root.db.d2 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceMixedSeries() {
    // ALIGN BY DEVICE with alias series and physical series from different devices
    // Note: Devices are sorted alphabetically, so root.db.d1 comes before root.view.d1
    String expectedHeader = "Time,Device,temperature,pressure,";
    String[] retArray =
        new String[] {
          "1509465600012,root.db.d1,null,1013.25,",
          "1509465601000,root.db.d1,null,1014.5,",
          "1509465602000,root.db.d1,null,1015.75,",
          "1509465600012,root.view.d1,2.1,null,",
          "1509465601000,root.view.d1,2.5,null,",
          "1509465602000,root.view.d1,3.0,null,"
        };

    resultSetEqualTest(
        "SELECT temperature, pressure FROM root.view.d1, root.db.d1 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Complex queries
  // ============================================================================

  @Test
  public void testComplexQueryWithMultipleConditions() {
    String expectedHeader = "Time,root.view.d1.temperature,root.db.d1.pressure,";
    String[] retArray =
        new String[] {
          "1509465600012,2.1,1013.25,", "1509465601000,2.5,1014.5,", "1509465602000,3.0,1015.75,"
        };

    resultSetEqualTest(
        "SELECT temperature, pressure FROM root.view.d1, root.db.d1 WHERE root.view.d1.temperature > 2.0 AND root.db.d1.pressure > 1013.0",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Verify disabled series are not shown
  // ============================================================================

  @Test
  public void testDisabledSeriesNotShown() {
    // The original physical path root.db.d1.temperature should not appear in queries
    // (it's disabled after rename)
    String expectedHeader = "Time,";
    String[] retArray = new String[] {};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // This query should return empty result since root.db.d1.temperature is disabled
      try (ResultSet resultSet = statement.executeQuery("SELECT temperature FROM root.db.d1")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(1, metaData.getColumnCount()); // Time column + header

        int count = 0;
        while (resultSet.next()) {
          count++;
        }
        assertEquals(0, count); // Should be empty
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Error cases
  // ============================================================================

  @Test
  public void testQueryNonExistentAliasSeries() {
    // Querying non-existent measurement should return empty result set, not throw exception
    String expectedHeader = ColumnHeaderConstant.TIME + ",";
    String[] emptyResultSet = new String[] {};
    resultSetEqualTest("SELECT non_exist FROM root.view.d1", expectedHeader, emptyResultSet);
  }

  @Test
  public void testInvalidFullPathInSelectClause() {
    // SELECT clause cannot use full path starting with root
    assertTestFail(
        "SELECT root.view.d1.temperature FROM root.view.d1",
        "Path can not start with root in select clause");
  }

  // ============================================================================
  // Test: GROUP BY TAGS queries
  // ============================================================================

  @Test
  public void testGroupByTagsBasic() {
    // GROUP BY TAGS with alias series
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT AVG(temperature), COUNT(temperature) FROM root.view.** GROUP BY TAGS(location)")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        // Should have location tag column + aggregation columns
        assertEquals(3, metaData.getColumnCount());

        // Check that we get results grouped by location tag
        // Note: root.view.d3.temperature is alias series from root.db.d3.temperature
        // (location=beijing)
        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          String location = resultSet.getString("location");
          // Should have beijing group (from root.view.d3.temperature alias series)
          if ("beijing".equals(location)) {
            double avg = resultSet.getDouble("avg(temperature)");
            long count = resultSet.getLong("count(temperature)");
            assertEquals(2, count);
            // Average of 20.1 and 20.5
            assertEquals(20.3, avg, 0.1);
          }
        }
        assertEquals(2, rowCount); // Should have one group (beijing from alias series)
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testGroupByTagsMultipleTags() {
    // GROUP BY TAGS with multiple tags including alias series
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT AVG(temperature), COUNT(temperature) FROM root.db.** GROUP BY TAGS(location, sensor_type)")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        // Should have location, sensor_type tag columns + aggregation columns
        assertEquals(4, metaData.getColumnCount());

        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          String location = resultSet.getString("location");
          String sensorType = resultSet.getString("sensor_type");
          // Should have groups for beijing/sensor, shanghai/sensor
          if ("beijing".equals(location) && "sensor".equals(sensorType)) {
            long count = resultSet.getLong("count(temperature)");
            // root.db.d3.temperature was renamed to alias (root.view.d3.temperature), so it's
            // disabled
            // root.db.d5.temperature has location='beijing', sensor_type='sensor' with 2 data
            // points
            // So only d5 should be counted for beijing/sensor group in root.db.**
            assertEquals(2, count); // d5 has 2 points
          }
        }
        assertEquals(3, rowCount); // beijing/sensor and shanghai/sensor
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testGroupByTagsWithAliasAndPhysical() {
    // GROUP BY TAGS query that includes both alias series and physical series
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT AVG(temperature), COUNT(temperature) FROM root.db.**, root.view.** GROUP BY TAGS(location)")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(3, metaData.getColumnCount());

        int rowCount = 0;
        boolean foundBeijing = false;
        while (resultSet.next()) {
          rowCount++;
          String location = resultSet.getString("location");
          if ("beijing".equals(location)) {
            foundBeijing = true;
            long count = resultSet.getLong("count(temperature)");
            // Should include both alias series (root.view.d3) and physical series (root.db.d5)
            // root.db.d3 is disabled (renamed to alias), so it shouldn't be counted
            assertEquals(4, count); // 2 from alias d3 + 2 from physical d5
          }
        }
        assert foundBeijing : "Should have beijing group";
        assertEquals(3, rowCount); // beijing and shanghai and null
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testGroupByTagsWithAggregationFunctions() {
    // GROUP BY TAGS with multiple aggregation functions
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT AVG(temperature), COUNT(temperature), MAX_VALUE(temperature), MIN_VALUE(temperature) FROM root.view.** GROUP BY TAGS(location)")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(5, metaData.getColumnCount()); // location + 4 aggregation columns

        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          String location = resultSet.getString("location");
          if ("beijing".equals(location)) {
            long count = resultSet.getLong("count(temperature)");
            double maxValue = resultSet.getDouble("max_value(temperature)");
            double minValue = resultSet.getDouble("min_value(temperature)");
            assertEquals(2, count);
            assertEquals(20.5, maxValue, 0.1);
            assertEquals(20.1, minValue, 0.1);
          }
        }
        assertEquals(2, rowCount);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Additional ALIGN BY DEVICE queries
  // ============================================================================

  @Test
  public void testAlignByDeviceWithAggregation() {
    // ALIGN BY DEVICE with aggregation functions
    String expectedHeader = "Device,AVG(temperature),";
    String[] retArray =
        new String[] {
          "root.view.d1,2.5333333015441895,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 ALIGN BY DEVICE", expectedHeader, retArray);
  }

  @Test
  public void testAlignByDeviceWithGroupByTime() {
    // ALIGN BY DEVICE with GROUP BY TIME
    String expectedHeader = "Time,Device,AVG(temperature),";
    String[] retArray =
        new String[] {
          "1509465600000,root.view.d1,2.299999952316284,", "1509465602000,root.view.d1,3.0,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 GROUP BY ([1509465600000, 1509465603000), 2s) ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithOrderBy() {
    // ALIGN BY DEVICE with ORDER BY
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465602000,root.view.d1,3.0,",
          "1509465601000,root.view.d1,2.5,",
          "1509465600012,root.view.d1,2.1,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 ORDER BY temperature DESC ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithLimit() {
    // ALIGN BY DEVICE with LIMIT
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,root.view.d1,2.1,", "1509465601000,root.view.d1,2.5,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 LIMIT 2 ALIGN BY DEVICE", expectedHeader, retArray);
  }

  @Test
  public void testAlignByDeviceWithLimitOffset() {
    // ALIGN BY DEVICE with LIMIT and OFFSET
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465601000,root.view.d1,2.5,", "1509465602000,root.view.d1,3.0,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 LIMIT 2 OFFSET 1 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithMultipleAggregations() {
    // ALIGN BY DEVICE with multiple aggregation functions
    String expectedHeader = "Device,AVG(temperature),COUNT(temperature),SUM(temperature),";
    String[] retArray =
        new String[] {
          "root.view.d1,2.5333333015441895,3,7.599999904632568,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature), COUNT(temperature), SUM(temperature) FROM root.view.d1 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithWhereAndOrderBy() {
    // ALIGN BY DEVICE with WHERE clause and ORDER BY
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465602000,root.view.d1,3.0,", "1509465601000,root.view.d1,2.5,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature >= 2.5 ORDER BY temperature DESC ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithAliasInSelect() {
    // ALIGN BY DEVICE with alias in SELECT clause
    String expectedHeader = "Time,Device,temp_alias,";
    String[] retArray =
        new String[] {
          "1509465600012,root.view.d1,2.1,",
          "1509465601000,root.view.d1,2.5,",
          "1509465602000,root.view.d1,3.0,",
        };

    resultSetEqualTest(
        "SELECT temperature AS temp_alias FROM root.view.d1 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Additional aggregation queries
  // ============================================================================

  @Test
  public void testAggregationWithWhereClause() {
    // Aggregation with WHERE clause filtering
    String expectedHeader = "AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.75,"}; // Average of 2.5 and 3.0 (filtered >= 2.5)

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 WHERE temperature >= 2.5",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMultipleAggregations() {
    // Multiple aggregation functions on alias series
    String expectedHeader =
        "AVG(root.view.d1.temperature),COUNT(root.view.d1.temperature),SUM(root.view.d1.temperature),MAX_VALUE(root.view.d1.temperature),MIN_VALUE(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.5333333015441895,3,7.599999904632568,3.0,2.1,"};

    resultSetEqualTest(
        "SELECT AVG(temperature), COUNT(temperature), SUM(temperature), MAX_VALUE(temperature), MIN_VALUE(temperature) FROM root.view.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeWithAliasSeries() {
    // GROUP BY TIME with alias series and aggregation
    String expectedHeader = "Time,AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"1509465600000,2.299999952316284,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 GROUP BY ([1509465600000, 1509465603000), 2s)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeWithMultipleSeries() {
    // GROUP BY TIME with alias series and physical series
    String expectedHeader = "Time,AVG(root.view.d1.temperature),AVG(root.db.d1.pressure),";
    String[] retArray =
        new String[] {
          "1509465600000,2.299999952316284,1013.875,", "1509465602000,3.0,1015.75,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature), AVG(pressure) FROM root.view.d1, root.db.d1 GROUP BY ([1509465600000, 1509465603000), 2s)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeWithHaving() {
    // GROUP BY TIME with HAVING clause
    String expectedHeader = "Time,AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 GROUP BY ([1509465600000, 1509465603000), 2s) HAVING AVG(temperature) > 2.5",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByLevel() {
    // GROUP BY LEVEL with alias series
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("SELECT COUNT(temperature) FROM root.view.** GROUP BY LEVEL=3")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(1, metaData.getColumnCount()); // Time + COUNT column

        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          long count = resultSet.getLong("COUNT(root.*.*.temperature)");
          assertEquals(5, count);
        }
        assertEquals(1, rowCount);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Additional WHERE clause scenarios
  // ============================================================================

  @Test
  public void testWhereClauseWithMultipleConditions() {
    // WHERE clause with AND conditions
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature >= 2.5 AND temperature <= 3.0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWhereClauseWithOrConditions() {
    // WHERE clause with OR conditions
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465600012,2.1,", "1509465602000,3.0,"}; // 2.1 or 3.0

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature = 2.1 OR temperature = 3.0",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWhereClauseWithNotEqual() {
    // WHERE clause with != condition
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature != 2.1", expectedHeader, retArray);
  }

  @Test
  public void testWhereClauseWithBetween() {
    // WHERE clause with BETWEEN
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465600012,2.1,", "1509465601000,2.5,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature BETWEEN 2.0 AND 2.5",
        expectedHeader,
        retArray);
  }

  @Test
  public void testWhereClauseWithIn() {
    // WHERE clause with IN
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465600012,2.1,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature IN (2.1, 3.0)",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Additional GROUP BY TAGS scenarios
  // ============================================================================

  @Test
  public void testGroupByTagsWithAliasInSelect() {
    // GROUP BY TAGS with alias in SELECT
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT AVG(temperature) AS avg_temp, COUNT(temperature) AS count_temp FROM root.view.d1 GROUP BY TAGS(location)")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(3, metaData.getColumnCount()); // location + 2 aggregation columns

        int rowCount = 0;
        while (resultSet.next()) {
          rowCount++;
          double avgTemp = resultSet.getDouble("avg(temperature)");
          long countTemp = resultSet.getLong("count(temperature)");
          assertEquals(2.5333333015441895, avgTemp, 0.1);
          assertEquals(3, countTemp);
        }
        assertEquals(1, rowCount);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  // ============================================================================
  // Test: Edge cases and special scenarios
  // ============================================================================

  @Test
  public void testEmptyResultSet() {
    // Query with condition that matches no data
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature > 100", expectedHeader, retArray);
  }

  @Test
  public void testSelectAllColumnsWithWildcard() {
    // SELECT * with alias series
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,2.1,", "1509465601000,2.5,", "1509465602000,3.0,",
        };

    resultSetEqualTest("SELECT * FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSelectWithArithmeticExpression() {
    // SELECT with arithmetic expression on alias series
    String expectedHeader = "Time,root.view.d1.temperature * 2,";
    String[] retArray =
        new String[] {
          "1509465600012,4.199999809265137,", "1509465601000,5.0,", "1509465602000,6.0,",
        };

    resultSetEqualTest("SELECT temperature * 2 FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSelectWithMultipleArithmeticExpressions() {
    // SELECT with multiple arithmetic expressions
    String expectedHeader = "Time,root.view.d1.temperature + 1,root.view.d1.temperature * 2,";
    String[] retArray =
        new String[] {
          "1509465600012,3.0999999046325684,4.199999809265137,",
          "1509465601000,3.5,5.0,",
          "1509465602000,4.0,6.0,",
        };

    resultSetEqualTest(
        "SELECT temperature + 1, temperature * 2 FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSelectWithFunctionExpression() {
    // SELECT with function expression (e.g., SIN, COS)
    String expectedHeader = "Time,SIN(root.view.d1.temperature),";
    String[] retArray =
        new String[] {
          "1509465600012,0.8632094147947462,",
          "1509465601000,0.5984721441039564,",
          "1509465602000,0.1411200080598672,",
        };

    resultSetEqualTest("SELECT SIN(temperature) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testCountAllWithAliasSeries() {
    // COUNT(*) with alias series
    String expectedHeader = "COUNT(root.view.d1.temperature),";
    String[] retArray = new String[] {"3,"};

    resultSetEqualTest("SELECT COUNT(*) FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testFirstValueLastValue() {
    // FIRST_VALUE and LAST_VALUE with alias series
    String expectedHeader =
        "FIRST_VALUE(root.view.d1.temperature),LAST_VALUE(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.1,3.0,"};

    resultSetEqualTest(
        "SELECT FIRST_VALUE(temperature), LAST_VALUE(temperature) FROM root.view.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testTimeDurationAggregation() {
    // TIME_DURATION aggregation with alias series
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery("SELECT TIME_DURATION(temperature) FROM root.view.d1")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(1, metaData.getColumnCount());

        assertTrue(resultSet.next());
        long duration = resultSet.getLong("TIME_DURATION(root.view.d1.temperature)");
        assertEquals(1988L, duration); // Time difference between first and last point
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testMaxTimeMinTime() {
    // MAX_TIME and MIN_TIME with alias series
    String expectedHeader =
        "MAX_TIME(root.view.d1.temperature),MIN_TIME(root.view.d1.temperature),";
    String[] retArray = new String[] {"1509465602000,1509465600012,"};

    resultSetEqualTest(
        "SELECT MAX_TIME(temperature), MIN_TIME(temperature) FROM root.view.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeWithSlidingWindow() {
    // GROUP BY TIME with sliding window
    String expectedHeader = "Time,AVG(root.view.d1.temperature),";
    String[] retArray =
        new String[] {
          "1509465600000,2.0999999046325684,", "1509465601000,2.5,", "1509465602000,3.0,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 GROUP BY ([1509465600000, 1509465603000), 1s)",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Edge cases and boundary conditions
  // ============================================================================

  @Test
  public void testSelectWithEmptyResult() {
    // Query with condition that matches no data
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature > 100.0", expectedHeader, retArray);
  }

  @Test
  public void testSelectAllDataWithWildcard() {
    // SELECT * from alias device should only return alias series
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray =
        new String[] {"1509465600012,2.1,", "1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest("SELECT * FROM root.view.d1", expectedHeader, retArray);
  }

  @Test
  public void testSelectWithTimeRange() {
    // Query with specific time range
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE time >= 1509465601000 AND time <= 1509465602000",
        expectedHeader,
        retArray);
  }

  @Test
  public void testSelectWithTimeRangeExclusive() {
    // Query with exclusive time range
    String expectedHeader = "Time,root.view.d1.temperature,";
    String[] retArray = new String[] {"1509465601000,2.5,"};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE time > 1509465600012 AND time < 1509465602000",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAggregationWithTimeFilter() {
    // Aggregation with time filter
    String expectedHeader = "AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"2.75,"}; // Average of 2.5 and 3.0

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 WHERE time >= 1509465601000",
        expectedHeader,
        retArray);
  }

  @Test
  public void testGroupByTimeWithTimeFilter() {
    // GROUP BY TIME with time filter
    String expectedHeader = "Time,AVG(root.view.d1.temperature),";
    String[] retArray = new String[] {"1509465601000,2.5,", "1509465602000,3.0,"};

    resultSetEqualTest(
        "SELECT AVG(temperature) FROM root.view.d1 WHERE time >= 1509465601000 GROUP BY ([1509465601000, 1509465603000), 1s)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithMultipleDevicesAndWhere() {
    // ALIGN BY DEVICE with WHERE clause filtering multiple devices
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray =
        new String[] {
          "1509465600012,root.db.d2,15.5,", "1509465602000,root.view.d1,3.0,",
        };

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1, root.db.d2 WHERE temperature > 2.8 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  @Test
  public void testAlignByDeviceWithNoMatchingData() {
    // ALIGN BY DEVICE with condition that matches no data
    String expectedHeader = "Time,Device,temperature,";
    String[] retArray = new String[] {};

    resultSetEqualTest(
        "SELECT temperature FROM root.view.d1 WHERE temperature > 100.0 ALIGN BY DEVICE",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Mixed queries with alias and physical series
  // ============================================================================

  @Test
  public void testMixedSeriesWithAggregation() {
    // Mixed alias and physical series with aggregation
    String expectedHeader = "AVG(root.view.d1.temperature),AVG(root.db.d1.pressure),";
    String[] retArray = new String[] {"2.5333333015441895,1014.5,"};

    resultSetEqualTest(
        "SELECT AVG(temperature), AVG(pressure) FROM root.view.d1, root.db.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMixedSeriesWithGroupByTime() {
    // Mixed alias and physical series with GROUP BY TIME
    String expectedHeader = "Time,AVG(root.view.d1.temperature),AVG(root.db.d1.pressure),";
    String[] retArray =
        new String[] {
          "1509465600000,2.299999952316284,1013.875,", "1509465602000,3.0,1015.75,",
        };

    resultSetEqualTest(
        "SELECT AVG(temperature), AVG(pressure) FROM root.view.d1, root.db.d1 GROUP BY ([1509465600000, 1509465603000), 2s)",
        expectedHeader,
        retArray);
  }

  @Test
  public void testMixedSeriesWithComplexWhere() {
    // Mixed alias and physical series with complex WHERE conditions
    String expectedHeader = "Time,root.view.d1.temperature,root.db.d1.pressure,";
    String[] retArray =
        new String[] {
          "1509465600012,2.1,1013.25,", "1509465601000,2.5,1014.5,", "1509465602000,3.0,1015.75,"
        };

    resultSetEqualTest(
        "SELECT temperature, pressure FROM root.view.d1, root.db.d1 WHERE (root.view.d1.temperature BETWEEN 2.0 AND 3.5) AND (root.db.d1.pressure BETWEEN 1013.0 AND 1016.0)",
        expectedHeader,
        retArray);
  }

  // ============================================================================
  // Test: Create view on renamed series (alias), then query the view
  // ============================================================================

  @Test
  public void testQueryViewOnRenamedSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.rename_complex");
      statement.execute(
          "CREATE TIMESERIES root.rename_complex.phys.no_data.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      // Insert data before rename
      statement.execute(
          "INSERT INTO root.rename_complex.phys.no_data(timestamp, s1) VALUES (1, 10)");
      statement.execute(
          "INSERT INTO root.rename_complex.phys.no_data(timestamp, s1) VALUES (2, 20)");
      statement.execute(
          "INSERT INTO root.rename_complex.phys.no_data(timestamp, s1) VALUES (3, 30)");

      statement.execute(
          "ALTER TIMESERIES root.rename_complex.phys.no_data.s1 RENAME TO root.rename_complex.phys.no_data.s1_r1");
      statement.execute(
          "CREATE VIEW root.rename_complex.view_test.view_1.s1_r1_view AS SELECT s1_r1 FROM root.rename_complex.phys.no_data");
      String expectedHeader = "Time,root.rename_complex.view_test.view_1.s1_r1_view,";
      String[] retArray = new String[] {"1,10,", "2,20,", "3,30,"};
      resultSetEqualTest(
          "SELECT * FROM root.rename_complex.view_test.view_1", expectedHeader, retArray);
    }
  }

  // ============================================================================
  // Test: Create view first, then rename the source series (view binds to source)
  // ============================================================================

  @Test
  public void testViewFirstThenRenameSource() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.rename_view_first");
      statement.execute(
          "CREATE TIMESERIES root.rename_view_first.dev.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      // Insert data before view and rename
      statement.execute(
          "INSERT INTO root.rename_view_first.dev.d1(timestamp, s1) VALUES (100, 100)");
      statement.execute(
          "INSERT INTO root.rename_view_first.dev.d1(timestamp, s1) VALUES (200, 200)");

      statement.execute(
          "CREATE VIEW root.rename_view_first.view_layer.v1.s1_view AS SELECT s1 FROM root.rename_view_first.dev.d1");
      // Rename source series after view is created
      statement.execute(
          "ALTER TIMESERIES root.rename_view_first.dev.d1.s1 RENAME TO root.rename_view_first.dev.d1.s1_renamed");

      // Query view: view resolves to the same data via alias after source renamed
      String expectedHeader = "Time,root.rename_view_first.view_layer.v1.s1_view,";
      String[] retArray = new String[] {"100,100,", "200,200,"};
      resultSetEqualTest(
          "SELECT * FROM root.rename_view_first.view_layer.v1", expectedHeader, retArray);
    }
  }

  @Test
  public void testViewFirstThenRenameWithMoreData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.rename_view_more");
      statement.execute(
          "CREATE TIMESERIES root.rename_view_more.dev.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=GORILLA");
      // Insert part of data before view
      statement.execute("INSERT INTO root.rename_view_more.dev.d1(timestamp, s1) VALUES (1, 1.1)");
      statement.execute("INSERT INTO root.rename_view_more.dev.d1(timestamp, s1) VALUES (2, 2.2)");

      statement.execute(
          "CREATE VIEW root.rename_view_more.views.v.s1_ref AS SELECT s1 FROM root.rename_view_more.dev.d1");
      // Insert more data after view creation, before rename
      statement.execute("INSERT INTO root.rename_view_more.dev.d1(timestamp, s1) VALUES (3, 3.3)");
      statement.execute("INSERT INTO root.rename_view_more.dev.d1(timestamp, s1) VALUES (4, 4.4)");

      statement.execute(
          "ALTER TIMESERIES root.rename_view_more.dev.d1.s1 RENAME TO root.rename_view_more.dev.d1.s1_alias");

      // All 4 rows should be visible via the view (view resolves via alias)
      String expectedHeader = "Time,root.rename_view_more.views.v.s1_ref,";
      String[] retArray = new String[] {"1,1.1,", "2,2.2,", "3,3.3,", "4,4.4,"};
      resultSetEqualTest("SELECT * FROM root.rename_view_more.views.v", expectedHeader, retArray);
    }
  }

  @Test
  public void testViewFirstThenRenameMultiSeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.rename_view_multi");
      statement.execute(
          "CREATE TIMESERIES root.rename_view_multi.dev.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute(
          "CREATE TIMESERIES root.rename_view_multi.dev.d1.s2 WITH DATATYPE=INT32, ENCODING=RLE");
      // Insert data before view and rename
      statement.execute(
          "INSERT INTO root.rename_view_multi.dev.d1(timestamp, s1, s2) VALUES (10, 1, 10)");
      statement.execute(
          "INSERT INTO root.rename_view_multi.dev.d1(timestamp, s1, s2) VALUES (20, 2, 20)");

      statement.execute(
          "CREATE VIEW root.rename_view_multi.views.v1(s1_v, s2_v) AS SELECT s1, s2 FROM root.rename_view_multi.dev.d1");
      // Rename only s1; s2 stays
      statement.execute(
          "ALTER TIMESERIES root.rename_view_multi.dev.d1.s1 RENAME TO root.rename_view_multi.dev.d1.s1_renamed");

      // Query view: both columns work (s1_renamed as alias, s2 unchanged)
      String expectedHeader =
          "Time,root.rename_view_multi.views.v1.s1_v,root.rename_view_multi.views.v1.s2_v,";
      String[] retArray = new String[] {"10,1,10,", "20,2,20,"};
      resultSetEqualTest("SELECT * FROM root.rename_view_multi.views.v1", expectedHeader, retArray);
    }
  }

  // ============================================================================
  // Helper methods
  // ============================================================================

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeader, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeader.split(",");
    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();

    for (int i = 0; i < expectedHeaders.length; i++) {
      String expectedColumnName = expectedHeaders[i].trim();
      if (expectedColumnName.isEmpty()) {
        continue;
      }

      boolean found = false;
      for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
        String actualColumnName = resultSetMetaData.getColumnName(j);
        int actualType = resultSetMetaData.getColumnType(j);

        if (actualColumnName.equals(expectedColumnName)
            && (expectedTypes == null || actualType == expectedTypes[i])) {
          actualIndexToExpectedIndexList.add(i);
          found = true;
          break;
        }
      }

      if (!found) {
        throw new AssertionError(
            "Expected column '" + expectedColumnName + "' not found in result set");
      }
    }

    return actualIndexToExpectedIndexList;
  }
}
