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

package org.apache.iotdb.db.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.util.AbstractSchemaIT;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test for counting devices, especially when database path itself is also a device. This tests the
 * bug fix where "count devices root.db.**" incorrectly includes the database path "root.db" as a
 * device when it's both a database and a device.
 */
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCountDeviceIT extends AbstractSchemaIT {

  public IoTDBCountDeviceIT(SchemaTestMode schemaTestMode) {
    super(schemaTestMode);
  }

  @Parameterized.BeforeParam
  public static void before() throws Exception {
    setUpEnvironment();
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Parameterized.AfterParam
  public static void after() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    tearDownEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    clearSchema();
  }

  /**
   * Test the bug fix: when database path is also a device, count devices should not count it.
   * Scenario: root.db is both a database and a device
   */
  @Test
  public void testCountDevicesWhenDatabaseIsAlsoDevice() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.db");

      // Create timeseries where root.db becomes a device
      statement.execute("CREATE TIMESERIES root.db.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN");

      // Create child devices
      statement.execute("CREATE TIMESERIES root.db.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db.d2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db.d3.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Count devices with pattern root.db.**
      long countResult = getCountDevices(statement, "root.db.**");

      // Get actual device count using show devices
      long showDevicesCount = getShowDevicesCount(statement, "root.db.**");

      // They should match (bug fix ensures root.db is not counted)
      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);

      // Expected: root.db.d1, root.db.d2, root.db.d3 = 3 devices
      Assert.assertEquals("Should have exactly 3 devices", 3, countResult);
    }
  }

  /** Test count devices with wildcard pattern root.*.** */
  @Test
  public void testCountDevicesWithWildcardPattern() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create multiple databases
      statement.execute("CREATE DATABASE root.db1");
      statement.execute("CREATE DATABASE root.db2");

      // Create devices in db1
      statement.execute("CREATE TIMESERIES root.db1.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db1.d2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Create devices in db2 where db2 itself is also a device
      statement.execute("CREATE TIMESERIES root.db2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db2.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.db2.d2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Count devices with wildcard pattern
      long countResult = getCountDevices(statement, "root.*.**");

      // Get actual device count using show devices
      long showDevicesCount = getShowDevicesCount(statement, "root.*.**");

      // They should match
      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);
    }
  }

  /** Test normal device counting without database-device conflict */
  @Test
  public void testCountDevicesNormalCase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.test");

      // Create only child devices (database is NOT a device)
      statement.execute("CREATE TIMESERIES root.test.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.test.d2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.test.d3.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.test.d1.s2 WITH DATATYPE=FLOAT, ENCODING=PLAIN");

      // Count devices
      long countResult = getCountDevices(statement, "root.test.**");

      // Get actual device count using show devices
      long showDevicesCount = getShowDevicesCount(statement, "root.test.**");

      // They should match
      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);

      // Expected: root.test.d1, root.test.d2, root.test.d3 = 3 devices
      Assert.assertEquals("Should have exactly 3 devices", 3, countResult);
    }
  }

  /** Test count devices with specific device path (no wildcard) */
  @Test
  public void testCountDevicesWithSpecificPath() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.sg");

      // Create devices
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.g1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.g2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Count devices exactly under root.sg.d1
      long countResult = getCountDevices(statement, "root.sg.d1.*");

      // Get actual device count using show devices
      long showDevicesCount = getShowDevicesCount(statement, "root.sg.d1.*");

      // They should match
      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);

      // Expected: root.sg.d1.g1, root.sg.d1.g2 = 2 devices
      Assert.assertEquals("Should have exactly 2 devices under root.sg.d1", 2, countResult);
    }
  }

  /** Test count devices with nested database-device structure */
  @Test
  public void testCountDevicesNestedStructure() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database
      statement.execute("CREATE DATABASE root.company");

      // Create a complex nested structure where root.company is also a device
      statement.execute("CREATE TIMESERIES root.company.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.company.dept1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.company.dept1.team1.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.company.dept1.team2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.company.dept2.s1 WITH DATATYPE=INT64, ENCODING=PLAIN");

      // Count devices
      long countResult = getCountDevices(statement, "root.company.**");
      long showDevicesCount = getShowDevicesCount(statement, "root.company.**");

      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);

      // Expected: root.company.dept1, root.company.dept1.team1,
      //           root.company.dept1.team2, root.company.dept2 = 4 devices
      Assert.assertEquals("Should have exactly 4 devices", 4, countResult);
    }
  }

  /** Test edge case: empty database (no devices) */
  @Test
  public void testCountDevicesEmptyDatabase() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // Create database without any timeseries
      statement.execute("CREATE DATABASE root.empty");

      // Count devices
      long countResult = getCountDevices(statement, "root.empty.**");
      long showDevicesCount = getShowDevicesCount(statement, "root.empty.**");

      Assert.assertEquals(
          "Count devices should match show devices count", showDevicesCount, countResult);

      // Expected: 0 devices
      Assert.assertEquals("Should have 0 devices", 0, countResult);
    }
  }

  /** Helper method to get count from "count devices" query */
  private long getCountDevices(Statement statement, String pattern) throws SQLException {
    try (ResultSet resultSet = statement.executeQuery("COUNT DEVICES " + pattern)) {
      if (resultSet.next()) {
        return resultSet.getLong(1);
      }
    }
    return 0;
  }

  /** Helper method to get count from "show devices" query */
  private long getShowDevicesCount(Statement statement, String pattern) throws SQLException {
    long count = 0;
    try (ResultSet resultSet = statement.executeQuery("SHOW DEVICES " + pattern)) {
      while (resultSet.next()) {
        count++;
      }
    }
    return count;
  }
}
