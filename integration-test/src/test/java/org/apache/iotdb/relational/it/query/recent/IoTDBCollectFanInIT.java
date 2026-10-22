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
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqualUnordered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCollectFanInIT {
  private static final String DATABASE = "collect_fan_in";
  private static final int DEVICES = 24;
  private static final int ROWS = 8;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDegreeOfParallelism(1)
        .setMaxTsBlockLineNumber(2)
        .setSortBufferSize(128 * 1024)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDataRegionPerDataNode(3);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + DATABASE);
      statement.execute("USE " + DATABASE);
      for (String table : new String[] {"readings1", "readings2"}) {
        statement.execute(
            "CREATE TABLE " + table + " (device STRING TAG, zone STRING TAG, value INT32 FIELD)");
        for (int device = 0; device < DEVICES; device++) {
          for (int value = 1; value <= ROWS; value++) {
            statement.execute(
                "INSERT INTO "
                    + table
                    + " (time,device,zone,value) VALUES ("
                    + value
                    + ",'"
                    + device(device)
                    + "',"
                    + (zone(device) == null ? "NULL" : "'" + zone(device) + "'")
                    + ","
                    + value
                    + ")");
          }
        }
      }
      statement.execute("FLUSH");
    }
  }

  @Test
  public void testUnorderedUnionPreservesEveryDuplicateOnEachNode() throws Exception {
    List<String> expectedRows = new ArrayList<>();
    for (int device = 0; device < DEVICES; device++) {
      for (int value = 1; value <= ROWS; value++) {
        String row = device(device) + "," + zone(device) + "," + value + ",";
        expectedRows.add(row);
        expectedRows.add(row);
      }
    }
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      String sql =
          "SELECT device, zone, value FROM readings1 UNION ALL "
              + "SELECT device, zone, value FROM readings1";
      try (ResultSet result = statement.executeQuery(sql)) {
        assertResultSetEqualUnordered(result, "device,zone,value,", expectedRows);
      }
    }
  }

  @Test
  public void testPipeVerificationQueryRowsAreCompletePerNode() throws Exception {
    TableModelUtils.createDataBaseAndTable(EnvFactory.getEnv(), "pipe_readings", DATABASE);
    TableModelUtils.insertData(DATABASE, "pipe_readings", 0, DEVICES * ROWS, EnvFactory.getEnv());
    List<String> expected =
        new ArrayList<>(TableModelUtils.generateExpectedResults(0, DEVICES * ROWS));
    Collections.sort(expected);
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      // The pipe verification query has no ORDER BY and fans in through a ready-first Collect,
      // so each node's row set must be checked independently without relying on row order.
      try (ResultSet result =
          statement.executeQuery(TableModelUtils.getQuerySql("pipe_readings"))) {
        assertResultSetEqualUnordered(result, TableModelUtils.generateHeaderResults(), expected);
      }
    }
  }

  @Test
  public void testLegacyTreeAndSharedTreeToTablePaths() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.collect_tree");
      for (int device = 0; device < DEVICES; device++) {
        String path = "root.collect_tree.z" + device % 3 + "." + device(device);
        statement.execute("CREATE ALIGNED TIMESERIES " + path + "(value INT32)");
        for (int value = 1; value <= ROWS; value++) {
          statement.execute(
              "INSERT INTO " + path + "(time,value) ALIGNED VALUES (" + value + "," + value + ")");
        }
      }
      try (ResultSet result =
          statement.executeQuery("SELECT value FROM root.collect_tree.*.* ALIGN BY DEVICE")) {
        int rows = 0;
        Set<String> seenDevices = new HashSet<>();
        String currentDevice = null;
        int deviceRows = 0;
        while (result.next()) {
          String nextDevice = result.getString("Device");
          if (!nextDevice.equals(currentDevice)) {
            if (currentDevice != null) {
              assertEquals(ROWS, deviceRows);
            }
            assertTrue("Tree results must retain contiguous devices", seenDevices.add(nextDevice));
            currentDevice = nextDevice;
            deviceRows = 0;
          }
          assertEquals(++deviceRows, result.getLong("Time"));
          assertEquals(result.getLong("Time"), result.getLong("value"));
          rows++;
        }
        assertEquals(ROWS, deviceRows);
        assertEquals(DEVICES, seenDevices.size());
        assertEquals(DEVICES * ROWS, rows);
      }
    }
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      statement.execute(
          "CREATE VIEW tree_readings(zone STRING TAG, device STRING TAG, value INT32 FIELD) AS root.collect_tree.**");
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, SUM(value) FROM ("
                  + "SELECT device, value FROM tree_readings UNION ALL SELECT device, value FROM tree_readings"
                  + ") u GROUP BY device ORDER BY device")) {
        for (int device = 0; device < DEVICES; device++) {
          assertTrue(result.next());
          assertEquals(device(device), result.getString(1));
          assertEquals(72.0, result.getDouble(2), 0.0);
        }
        assertFalse(result.next());
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static String device(int id) {
    return "d" + (id < 10 ? "0" : "") + id;
  }

  private static String zone(int id) {
    return id % 3 == 0 ? null : "z" + id % 3;
  }

  @Test
  public void testGroupedAggregationAfterGlobalWindow() throws Exception {
    checkGroups(
        "SELECT device, zone, value FROM (SELECT device, zone, value, "
            + "COUNT(*) OVER () AS n FROM readings1) t WHERE n > 0",
        1);
  }

  @Test
  public void testUnionPartialFinalAndDistinctMask() throws Exception {
    String input =
        "SELECT device, zone, value FROM readings1 UNION ALL "
            + "SELECT device, zone, value FROM readings2";
    checkGroups(input, 2);
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      // Without DISTINCT, this query exercises branch PARTIAL and post-Collect FINAL aggregation.
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, zone, SUM(value) FROM ("
                  + input
                  + ") u GROUP BY device, zone ORDER BY device")) {
        for (int device = 0; device < DEVICES; device++) {
          assertTrue(result.next());
          assertEquals(device(device), result.getString(1));
          assertEquals(zone(device), result.getString(2));
          assertEquals(72.0, result.getDouble(3), 0.0);
        }
        assertFalse(result.next());
      }
    }
  }

  @Test
  public void testOrderedFanInAndJoinDependencies() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      for (String limit : new String[] {"", " LIMIT 13"}) {
        try (ResultSet result =
            statement.executeQuery(
                "SELECT device, value FROM (SELECT device, value FROM readings1 UNION ALL "
                    + "SELECT device, value FROM readings2) u ORDER BY device, value"
                    + limit)) {
          int expectedRows = limit.isEmpty() ? DEVICES * ROWS * 2 : 13;
          for (int row = 0; row < expectedRows; row++) {
            assertTrue(result.next());
            assertEquals(device(row / (ROWS * 2)), result.getString(1));
            assertEquals(row / 2 % ROWS + 1, result.getInt(2));
          }
          assertFalse(result.next());
        }
      }
      try (ResultSet result =
          statement.executeQuery(
              "SELECT COUNT(*) FROM readings1 a JOIN readings2 b "
                  + "ON a.device = b.device AND a.time = b.time WHERE a.value = b.value")) {
        assertTrue(result.next());
        assertEquals(DEVICES * ROWS, result.getLong(1));
        assertFalse(result.next());
      }
    }
  }

  private void checkGroups(String input, int copies) throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, zone, SUM(value), "
                  + "COUNT(*), COUNT(DISTINCT value) FROM ("
                  + input
                  + ") u GROUP BY device, zone ORDER BY device")) {
        for (int device = 0; device < DEVICES; device++) {
          assertTrue(result.next());
          assertEquals(device(device), result.getString(1));
          assertEquals(zone(device), result.getString(2));
          assertEquals(36.0 * copies, result.getDouble(3), 0.0);
          assertEquals((long) ROWS * copies, result.getLong(4));
          assertEquals(ROWS, result.getLong(5));
        }
        assertFalse(result.next());
      }
    }
  }

  @Test
  public void testNestedWindowAfterCollectRestoresCompletePartitions() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, zone, value, "
                  + "SUM(value) OVER (PARTITION BY device, zone ORDER BY value) AS running_sum FROM ("
                  + "SELECT device, zone, value, COUNT(*) OVER () AS n FROM readings1) t "
                  + "WHERE n > 0 ORDER BY device, value")) {
        for (int device = 0; device < DEVICES; device++) {
          for (int value = 1; value <= ROWS; value++) {
            assertTrue(result.next());
            assertEquals(device(device), result.getString(1));
            assertEquals(zone(device), result.getString(2));
            assertEquals(value, result.getInt(3));
            assertEquals((double) value * (value + 1) / 2, result.getDouble(4), 0.0);
          }
        }
        assertFalse(result.next());
      }
    }
  }

  @Test
  public void testOrderByAfterGlobalWindowAndConstantFill() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, zone, value, COUNT(*) OVER () AS n "
                  + "FROM readings1 FILL METHOD CONSTANT 0 ORDER BY device, value DESC")) {
        for (int device = 0; device < DEVICES; device++) {
          for (int value = ROWS; value > 0; value--) {
            assertTrue(result.next());
            assertEquals(device(device), result.getString(1));
            assertEquals(value, result.getInt(3));
            assertEquals((long) DEVICES * ROWS, result.getLong(4));
          }
        }
        assertFalse(result.next());
      }
    }
  }

  @Test
  public void testExcludedTopKPlanRetainsQueryResults() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE);
      int limit = DEVICES + 5;
      try (ResultSet result =
          statement.executeQuery(
              "SELECT device, zone, value, COUNT(*) OVER () AS n FROM readings1 "
                  + "ORDER BY value DESC, device LIMIT "
                  + limit)) {
        for (int row = 0; row < limit; row++) {
          assertTrue(result.next());
          assertEquals(device(row % DEVICES), result.getString(1));
          assertEquals(zone(row % DEVICES), result.getString(2));
          assertEquals(ROWS - row / DEVICES, result.getInt(3));
          assertEquals((long) DEVICES * ROWS, result.getLong(4));
        }
        assertFalse(result.next());
      }
    }
  }
}
