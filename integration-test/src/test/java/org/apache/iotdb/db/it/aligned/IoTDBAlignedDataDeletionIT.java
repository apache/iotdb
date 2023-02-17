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

package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedDataDeletionIT {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE ALIGNED TIMESERIES root.vehicle.d0(s0 INT32 ENCODING=RLE, s1 INT64 ENCODING=RLE, s2 FLOAT ENCODING=RLE, s3 TEXT ENCODING=PLAIN, s4 BOOLEAN ENCODING=PLAIN)",
      };

  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4"
          + ") ALIGNED VALUES(%d,%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DELETE FROM root.vehicle.d0.* WHERE time <= 10000";

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        // Adjust memstable threshold size to make it flush automatically
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareSeries();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Should delete this case after the deletion value filter feature be implemented
   *
   * @throws SQLException
   */
  @Test
  public void testUnsupportedValueFilter() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.vehicle.d0(time,s0) aligned values (10,310)");
      statement.execute("insert into root.vehicle.d0(time,s3) aligned values (10,'text')");
      statement.execute("insert into root.vehicle.d0(time,s4) aligned values (10,true)");

      String errorMsg =
          TSStatusCode.SEMANTIC_ERROR.getStatusCode()
              + ": For delete statement, where clause can only"
              + " contain time expressions, value filter is not currently supported.";

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
    try (Connection connection = EnvFactory.getEnv().getConnection();
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

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("merge");
      statement.execute("DELETE FROM root.vehicle.d0.** WHERE time <= 15000");

      // before merge completes
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln.wf01.wt01");
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 10000; i++) {
        statement.addBatch(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
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
  }

  @Test
  public void testDelMultipleFlushingMemtable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 1000; i++) {
        statement.addBatch(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 150 and time <= 300");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 300 and time <= 400");
      for (int i = 1001; i <= 2000; i++) {
        statement.addBatch(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 500 and time <= 800");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 900 and time <= 1100");
      statement.execute("DELETE FROM root.vehicle.d0.s0 WHERE time > 1500 and time <= 1650");
      statement.execute("flush");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1100, cnt);
      }
      cleanData();
    }
  }

  @Test
  public void testDelSeriesWithSpecialSymbol() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.ln.d1.`status,01` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.d1(timestamp,`status,01`) VALUES(300, true)");
      statement.execute("INSERT INTO root.ln.d1(timestamp,`status,01`) VALUES(500, false)");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("DELETE FROM root.ln.d1.`status,01` WHERE time <= 400");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      statement.execute("DELETE FROM root.ln.d1.`status,01`");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void testInsertDuplicatedTimeThenDel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)");
      statement.execute(
          "insert into root.lz.dev.GPS(time, latitude, longitude) aligned values(9,3.2,9.8)");
      statement.execute("insert into root.lz.dev.GPS(time, latitude) aligned values(11,4.5)");
      statement.execute("insert into root.lz.dev.GPS(time, longitude) aligned values(11,6.7)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("delete from root.lz.dev.GPS.latitude");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    }
  }

  @Test
  public void testDeleteAll() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)");
      statement.execute(
          "insert into root.lz.dev.GPS(time, latitude, longitude) aligned values(9,3.2,9.8)");
      statement.execute("insert into root.lz.dev.GPS(time, latitude) aligned values(11,4.5)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("delete from root.lz.**");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
      statement.execute("flush");
    }
  }

  private static void prepareSeries() {
    String sq = null;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        sq = sql;
        statement.execute(sql);
      }
    } catch (Exception e) {
      System.out.println(sq);
      e.printStackTrace();
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(deleteAllTemplate);
    }
  }

  public void prepareMerge() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite data
      for (int i = 10001; i <= 20000; i++) {
        statement.addBatch(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
      // prepare Overflow data
      for (int i = 1; i <= 10000; i++) {
        statement.addBatch(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
    }
  }
}
