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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class IoTDBDeletionIT {

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "SET STORAGE GROUP TO root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      };

  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4" + ") VALUES(%d,%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DELETE FROM root.vehicle.d0 WHERE time <= 10000";
  private long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvironmentUtils.closeStatMonitor();
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareSeries();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
  }

  /**
   * Should delete this case after the deletion value filter feature be implemented
   *
   * @throws SQLException
   */
  @Test
  public void testUnsupportedValueFilter() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.vehicle.d0(time,s0) values (10,310)");
      statement.execute("insert into root.vehicle.d0(time,s3) values (10,'text')");
      statement.execute("insert into root.vehicle.d0(time,s4) values (10,true)");

      String errorMsg =
          "303: Check metadata error: For delete statement, where clause can only"
              + " contain time expressions, value filter is not currently supported.";

      String errorMsg2 =
          "303: Check metadata error: For delete statement, where clause can only contain"
              + " atomic expressions like : time > XXX, time <= XXX,"
              + " or two atomic expressions connected by 'AND'";

      try {
        statement.execute(
            "DELETE FROM root.vehicle.d0.s0  WHERE s0 <= 300 AND time > 0 AND time < 100");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg2, e.getMessage());
      }

      try {
        statement.execute("DELETE FROM root.vehicle.d0.s0  WHERE s0 <= 300 AND s0 > 0");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try {
        statement.execute("DELETE FROM root.vehicle.d0.s3  WHERE s3 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try {
        statement.execute("DELETE FROM root.vehicle.d0.s4  WHERE s4 != true");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s3 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s4 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void test() throws SQLException {
    prepareData();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.s0  WHERE time <= 300");
      statement.execute(
          "DELETE FROM root.vehicle.d0.s1,root.vehicle.d0.s2,root.vehicle.d0.s3"
              + " WHERE time <= 350");
      statement.execute("DELETE FROM root.vehicle.d0.** WHERE time <= 150");

      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(50, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testMerge() throws SQLException {
    prepareMerge();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("merge");
      statement.execute("DELETE FROM root.vehicle.d0.** WHERE time <= 15000");

      // before merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(5000, cnt);
      }

      // after merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(5000, cnt);
      }
      cleanData();
    }
  }

  @Test
  public void testDelAfterFlush() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN," + " ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt01(timestamp,status) " + "values(1509465600000,true)");
      statement.execute("INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false)");

      statement.execute("delete from root.ln.wf01.wt01.status where time <= NOW()");
      statement.execute("flush");
      statement.execute("delete from root.ln.wf01.wt01.status where time <= NOW()");

      try (ResultSet resultSet = statement.executeQuery("select status from root.ln.wf01.wt01")) {
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testRangeDelete() throws SQLException {
    prepareData();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time <= 300");
      statement.execute("DELETE FROM root.vehicle.d0.s1 WHERE time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }

      statement.execute("DELETE FROM root.vehicle.d0.** WHERE time > 50 and time <= 250");
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testFullDeleteWithoutWhereClause() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM root.vehicle.d0.s0");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(0, cnt);
      }
      cleanData();
    }
  }

  @Test
  public void testPartialPathRangeDelete() throws SQLException {
    prepareData();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle.d0.* WHERE time <= 300 and time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      statement.execute("DELETE FROM root.vehicle.*.s0 WHERE time <= 100");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }
    }
    cleanData();
  }

  @Test
  public void testDelFlushingMemtable() throws SQLException {
    long size = IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    // Adjust memstable threshold size to make it flush automatically
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(10000);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 1500 and time <= 9000");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(2500, cnt);
      }
      cleanData();
    }
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(size);
  }

  @Test
  public void testDelMultipleFlushingMemtable() throws SQLException {
    long size = IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    // Adjust memstable threshold size to make it flush automatically
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1000000);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 100000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 15000 and time <= 30000");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 30000 and time <= 40000");
      for (int i = 100001; i <= 200000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 50000 and time <= 80000");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 90000 and time <= 110000");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 150000 and time <= 165000");
      statement.execute("flush");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(110000, cnt);
      }
      cleanData();
    }
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(size);
  }

  @Test
  public void testDelSeriesWithSpecialSymbol() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.ln.d1.\"status,01\" WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.d1(timestamp,\"status,01\") VALUES(300, true)");
      statement.execute("INSERT INTO root.ln.d1(timestamp,\"status,01\") VALUES(500, false)");

      try (ResultSet resultSet = statement.executeQuery("select \"status,01\" from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("DELETE FROM root.ln.d1.\"status,01\" WHERE time <= 400");

      try (ResultSet resultSet = statement.executeQuery("select \"status,01\" from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      statement.execute("DELETE FROM root.ln.d1.\"status,01\"");

      try (ResultSet resultSet = statement.executeQuery("select \"status,01\" from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  private static void prepareSeries() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite file
      for (int i = 201; i <= 300; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("merge");
      // prepare Unseq-File
      for (int i = 1; i <= 100; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("merge");
      // prepare BufferWrite cache
      for (int i = 301; i <= 400; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow cache
      for (int i = 101; i <= 200; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
    }
  }

  private void cleanData() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(deleteAllTemplate);
    }
  }

  public void prepareMerge() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite data
      for (int i = 10001; i <= 20000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow data
      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
    }
  }
}
