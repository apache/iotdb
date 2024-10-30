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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDeletionTableIT {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        "CREATE TABLE IF NOT EXISTS vehicle0(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
      };

  private String insertTemplate =
      "INSERT INTO test.vehicle%d(time, deviceId, s0,s1,s2,s3,s4"
          + ") VALUES(%d,'d0',%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DROP TABLE IF EXISTS vehicle%d";

  @BeforeClass
  public static void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000);
    // Adjust memstable threshold size to make it flush automatically
    EnvFactory.getEnv().initClusterEnvironment();
    prepareSeries();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Should delete this case after the deletion value filter feature be implemented
   *
   * @throws SQLException
   */
  @Test
  public void testUnsupportedValueFilter() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          "CREATE TABLE vehicle1(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)");

      statement.execute("insert into vehicle1(time, deviceId, s0) values (10, 'd0', 310)");
      statement.execute("insert into vehicle1(time, deviceId, s3) values (10, 'd0','text')");
      statement.execute("insert into vehicle1(time, deviceId, s4) values (10, 'd0',true)");

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s0 <= 300 AND s0 > 0");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The column 's0' does not exist or is not an id column", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s3 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The column 's3' does not exist or is not an id column", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE s4 != true");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The column 's4' does not exist or is not an id column", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 or time > 30");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Only support AND operator in deletion", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 or deviceId = 'd0'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Only support AND operator in deletion", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId > 'd0'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The operator of id predicate must be '=' for 'd0'", e.getMessage());
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle1")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s3 FROM vehicle1")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s4 FROM vehicle1")) {
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
    prepareData(2);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      statement.execute("DELETE FROM vehicle2 WHERE time <= 150");

      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle2")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      statement.execute("DELETE FROM vehicle2 WHERE time <= 300");

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle2")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      statement.execute("DELETE FROM vehicle2 WHERE time <= 350");

      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM vehicle2")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(50, cnt);
      }
    }
    cleanData(2);
  }

  @Test
  public void testDelAfterFlush() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE ln3");
      statement.execute("use ln3");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle3(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)"));

      statement.execute(
          "INSERT INTO vehicle3(time, deviceId, s4) " + "values(1509465600000, 'd0', true)");
      statement.execute("INSERT INTO vehicle3(time, deviceId, s4) VALUES(NOW(), 'd0', false)");

      statement.execute("delete from vehicle3 where time <= NOW()");
      statement.execute("flush");
      statement.execute("delete from vehicle3 where time <= NOW()");

      try (ResultSet resultSet = statement.executeQuery("select s4 from vehicle3")) {
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testRangeDelete() throws SQLException {
    prepareData(4);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      statement.execute("DELETE FROM vehicle4 WHERE time >= 300");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(300, cnt);
      }

      statement.execute("DELETE FROM vehicle4 WHERE time <= 150");
      try (ResultSet set = statement.executeQuery("SELECT s1 FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }

      statement.execute("DELETE FROM vehicle4 WHERE time > 50 and time <= 250");
      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }
    }
    cleanData(4);
  }

  @Test
  public void testFullDeleteWithoutWhereClause() throws SQLException {
    prepareData(5);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute("DELETE FROM vehicle5");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle5")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(0, cnt);
      }
      cleanData(5);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDeleteWithSpecificDevice() throws SQLException {
    prepareData(6);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      statement.execute(
          "DELETE FROM vehicle6 WHERE time <= 300 and time > 150 and deviceId = 'd0'");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle6")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      // invalid deletion, d1 not exists
      statement.execute("DELETE FROM vehicle6 WHERE time <= 200 and deviceId = 'd1'");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle6")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }
    }
    cleanData(6);
  }

  @Test
  public void testDelFlushingMemtable() throws SQLException {
    int testNum = 7;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM vehicle7 WHERE time > 1500 and time <= 9000");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle7")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(2500, cnt);
      }
      cleanData(testNum);
    }
  }

  @Test
  public void testDelMultipleFlushingMemtable() throws SQLException {
    int testNum = 8;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      for (int i = 1; i <= 1000; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM vehicle8 WHERE time > 150 and time <= 300");
      statement.execute("DELETE FROM vehicle8 WHERE time > 300 and time <= 400");
      for (int i = 1001; i <= 2000; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM vehicle8 WHERE time > 500 and time <= 800");
      statement.execute("DELETE FROM vehicle8 WHERE time > 900 and time <= 1100");
      statement.execute("DELETE FROM vehicle8 WHERE time > 1500 and time <= 1650");
      statement.execute("flush");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle8")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1100, cnt);
      }
      cleanData(testNum);
    }
  }

  @Test
  public void testDeleteAll() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              9));

      statement.execute("insert into vehicle9(time, deviceId, s2) values(9,'d0',9.8)");
      statement.execute("insert into vehicle9(time, deviceId, s2) values(11, 'd0', 4.5)");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle9")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("delete from vehicle9");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle9")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void testDeleteDataFromEmptyTable() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d_1(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              10));
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d_2(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              10));

      statement.execute(
          "INSERT INTO vehicle10_2(Time, deviceId, s4) VALUES (2022-10-11 10:20:50,'d0', true),(2022-10-11 10:20:51,'d0',true)");
      statement.execute("DELETE FROM vehicle10_1 WHERE time >2022-10-11 10:20:50");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle10_2")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    }
  }

  @Test
  public void testDelSeriesWithSpecialSymbol() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              11));

      statement.execute("INSERT INTO vehicle11(time, deviceId, s4) VALUES(300, 'device,1', true)");
      statement.execute("INSERT INTO vehicle11(time, deviceId, s4) VALUES(500, 'device,2', false)");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle11")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("DELETE FROM vehicle11 WHERE time <= 400 and deviceId = 'device,1'");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle11")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      statement.execute("DELETE FROM vehicle11 WHERE deviceId = 'device,2'");

      try (ResultSet resultSet = statement.executeQuery("select * from vehicle11")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  private static void prepareSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void prepareData(int testNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      // prepare BufferWrite file
      for (int i = 201; i <= 300; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("flush");
      // prepare Unseq-File
      for (int i = 1; i <= 100; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("flush");
      // prepare BufferWrite cache
      for (int i = 301; i <= 400; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow cache
      for (int i = 101; i <= 200; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
    }
  }

  private void cleanData(int testNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(String.format(deleteAllTemplate, testNum));
    }
  }
}
