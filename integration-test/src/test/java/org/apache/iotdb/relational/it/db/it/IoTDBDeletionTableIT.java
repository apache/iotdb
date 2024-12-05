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

import org.apache.tsfile.read.common.TimeRange;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDeletionTableIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDeletionTableIT.class);
  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        "CREATE TABLE IF NOT EXISTS vehicle0(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
      };

  private final String insertTemplate =
      "INSERT INTO test.vehicle%d(time, deviceId, s0,s1,s2,s3,s4"
          + ") VALUES(%d,'d%d',%d,%d,%f,%s,%b)";

  @BeforeClass
  public static void setUp() {
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
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /** Should delete this case after the deletion value filter feature be implemented */
  @Test
  public void testUnsupportedValueFilter() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          "CREATE TABLE vehicle1(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT, attr1 ATTRIBUTE)");

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
        statement.execute("DELETE FROM vehicle1  WHERE s1 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The column 's1' does not exist or is not an id column", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE attr1 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(
            "701: The column 'attr1' does not exist or is not an id column", e.getMessage());
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
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId > 'd0'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: The operator of id predicate must be '=' for 'd0'", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId is not null");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(
            "701: Unsupported expression: (deviceId IS NOT NULL) in ((time < 10) AND (deviceId IS NOT NULL))",
            e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1  WHERE time < 10 and deviceId = null");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(
            "701: The right hand value of id predicate cannot be null with '=' operator, please use 'IS NULL' instead",
            e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicle1 WHERE true");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Unsupported expression: true in true", e.getMessage());
      }

      try {
        statement.execute("DELETE FROM vehicleNonExist");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals("701: Table vehiclenonexist not found", e.getMessage());
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
    int testId = 2;
    prepareData(testId, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      // init [1, 400]

      // remain [151, 400]
      statement.execute("DELETE FROM vehicle" + testId + " WHERE time <= 150");
      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle" + testId)) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      // remain [301, 400]
      statement.execute("DELETE FROM vehicle" + testId + " WHERE time <= 300");

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle" + testId)) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      // remain [351, 400]
      statement.execute("DELETE FROM vehicle" + testId + " WHERE time <= 350");

      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM vehicle" + testId)) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(50, cnt);
      }

      // remain [361, 380]
      statement.execute("DELETE FROM vehicle" + testId + "  WHERE time <= 360 or time > 380");
      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM vehicle" + testId)) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(20, cnt);
      }
    }
    cleanData(testId);
  }

  @Test
  public void testDelAfterFlush() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE ln3");
      statement.execute("use ln3");
      statement.execute(
          "CREATE TABLE vehicle3(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)");

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
    prepareData(4, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      // [1, 400] -> [1, 299]
      statement.execute("DELETE FROM vehicle4 WHERE time >= 300");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(299, cnt);
      }

      // [1, 299] -> [151, 299]
      statement.execute("DELETE FROM vehicle4 WHERE time <= 150");
      try (ResultSet set = statement.executeQuery("SELECT s1 FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(149, cnt);
      }

      // [151, 299] -> [251, 299]
      statement.execute("DELETE FROM vehicle4 WHERE time > 50 and time <= 250");
      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle4")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(49, cnt);
      }
    }
    cleanData(4);
  }

  @Test
  public void testFullDeleteWithoutWhereClause() throws SQLException {
    prepareData(5, 1);
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
      fail(e.getMessage());
    }
  }

  @Test
  public void testDeleteWithSpecificDevice() throws SQLException {
    prepareData(6, 1);
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
  public void testDelFlushingMemTable() throws SQLException {
    int testNum = 7;
    int deviceId = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(
                insertTemplate, testNum, i, deviceId, i, i, (double) i, "'" + i + "'", i % 2 == 0));
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
  public void testDelMultipleFlushingMemTable() throws SQLException {
    int testNum = 8;
    int deviceId = 0;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      for (int i = 1; i <= 1000; i++) {
        statement.execute(
            String.format(
                insertTemplate, testNum, i, deviceId, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM vehicle8 WHERE time > 150 and time <= 300");
      statement.execute("DELETE FROM vehicle8 WHERE time > 300 and time <= 400");
      for (int i = 1001; i <= 2000; i++) {
        statement.execute(
            String.format(
                insertTemplate, testNum, i, deviceId, i, i, (double) i, "'" + i + "'", i % 2 == 0));
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

  @Test
  public void testDeleteTable() throws SQLException {
    int testNum = 12;
    prepareData(testNum, 1);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      statement.execute("DROP TABLE vehicle" + testNum);

      try (ResultSet ignored = statement.executeQuery("SELECT * FROM vehicle" + testNum)) {
        fail("Exception expected");
      } catch (SQLException e) {
        assertEquals("701: Table 'test.vehicle12' does not exist", e.getMessage());
      }

      statement.execute(
          String.format(
              "CREATE TABLE vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle" + testNum)) {
        assertFalse(set.next());
      }

      prepareData(testNum, 1);

      statement.execute("DELETE FROM vehicle" + testNum + " WHERE time <= 150");

      try (ResultSet set = statement.executeQuery("SELECT * FROM vehicle" + testNum)) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }
    }
    cleanData(testNum);
  }

  @Test
  public void testSingleDeviceDeletionMultiExecution() throws SQLException {
    int testNum = 13;
    prepareData(testNum, 5);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      // init d0[1, 400] d1[1, 400] d2[1, 400] d3[1, 400] d4[1, 400]

      // remain d1[10, 400] d2[10, 400] d3[10, 400] d4[10, 400]
      statement.execute("DELETE FROM vehicle" + testNum + "  WHERE time < 10 or deviceId = 'd0'");
      int[] expectedPointNumOfDevice = new int[] {0, 391, 391, 391, 391};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d1[50, 400] d2[10, 400] d3[10, 400] d4[10, 400]
      statement.execute("DELETE FROM vehicle" + testNum + "  WHERE time < 50 and deviceId = 'd1'");
      expectedPointNumOfDevice = new int[] {0, 351, 391, 391, 391};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d1[50, 400] d2[101, 400] d3[10, 400] d4[10, 400]
      statement.execute(
          "DELETE FROM vehicle" + testNum + "  WHERE time <= 100 and deviceId = 'd2'");
      expectedPointNumOfDevice = new int[] {0, 351, 300, 391, 391};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d1[50, 400] d2[101, 400] d3[301, 400] d4[10, 400]
      statement.execute(
          "DELETE FROM vehicle" + testNum + "  WHERE time <= 300 and deviceId = 'd3'");
      expectedPointNumOfDevice = new int[] {0, 351, 300, 100, 391};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d1[50, 400] d2[101, 400] d3[301, 400] d4[10, 100]
      statement.execute("DELETE FROM vehicle" + testNum + "  WHERE time > 100 and deviceId = 'd4'");
      expectedPointNumOfDevice = new int[] {0, 351, 300, 100, 91};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);
    }
    cleanData(testNum);
  }

  private void checkDevicePoint(int[] expectedPointNumOfDevice, Statement statement, int testNum)
      throws SQLException {
    for (int i = 0; i < expectedPointNumOfDevice.length; i++) {
      try (ResultSet set =
          statement.executeQuery(
              "SELECT * FROM vehicle" + testNum + " where deviceId = 'd" + i + "'")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(expectedPointNumOfDevice[i], cnt);
      }
    }
  }

  @Test
  public void testDeviceIdWithNull() throws SQLException {
    int testNum = 14;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          "create table t" + testNum + " (id1 string id, id2 string id, s1 int32 measurement)");
      // id1 is null for this record
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (1, '1', 1)");
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (2, '', 2)");
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (3, NULL, 3)");
      statement.execute("flush");

      statement.execute("delete from t" + testNum + " where id1 is NULL and time <= 1");
      try (ResultSet set = statement.executeQuery("SELECT * FROM t" + testNum + " order by time")) {
        assertTrue(set.next());
        assertEquals(2, set.getLong("time"));
        assertTrue(set.next());
        assertEquals(3, set.getLong("time"));
        assertFalse(set.next());
      }

      statement.execute("delete from t" + testNum + " where id2 is NULL");
      try (ResultSet set = statement.executeQuery("SELECT * FROM t" + testNum + " order by time")) {
        assertTrue(set.next());
        assertEquals(2, set.getLong("time"));
        assertFalse(set.next());
      }

      statement.execute("delete from t" + testNum);
      try (ResultSet set = statement.executeQuery("SELECT * FROM t" + testNum + " order by time")) {
        assertFalse(set.next());
      }

      statement.execute("drop table t" + testNum);
    }
  }

  @Test
  public void testEmptyString() throws SQLException {
    int testNum = 15;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          "create table t" + testNum + " (id1 string id, id2 string id, s1 int32 measurement)");
      // id1 is null for this record
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (1, '1', 1)");
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (2, '', 2)");
      statement.execute("insert into t" + testNum + " (time, id2, s1) values (3, NULL, 3)");
      statement.execute("flush");

      statement.execute("delete from t" + testNum + " where id2 = ''");
      try (ResultSet set = statement.executeQuery("SELECT * FROM t" + testNum + " order by time")) {
        assertTrue(set.next());
        assertEquals(1, set.getLong("time"));
        assertTrue(set.next());
        assertEquals(3, set.getLong("time"));
        assertFalse(set.next());
      }

      statement.execute("drop table t" + testNum);
    }
  }

  @Test
  public void testIllegalRange() throws SQLException {
    int testNum = 16;
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          "create table t" + testNum + " (id1 string id, id2 string id, s1 int32 measurement)");

      try {
        statement.execute("delete from t" + testNum + " where time > 10 and time <= 1");
        fail("Exception expected");
      } catch (SQLException e) {
        assertEquals("701: Start time 11 is greater than end time 1", e.getMessage());
      }
    }
  }

  @Test
  public void testMultiDevicePartialDeletionMultiExecution() throws SQLException {
    int testNum = 17;
    prepareData(testNum, 5);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      // init d0[1, 400] d1[1, 400] d2[1, 400] d3[1, 400] d4[1, 400]

      // remain d0[10, 400] d1[10, 400] d2[1, 400] d3[1, 400] d4[1, 400]
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE time < 10 and (deviceId = 'd0' or deviceId = 'd1')");
      int[] expectedPointNumOfDevice = new int[] {391, 391, 400, 400, 400};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d0[10, 400] d1[50, 400] d2[50, 400] d3[50, 400] d4[1, 400]
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE time < 50 and (deviceId = 'd1' or deviceId = 'd2' or deviceId = 'd3')");
      expectedPointNumOfDevice = new int[] {391, 351, 351, 351, 400};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d0[101, 400] d1[50, 400] d2[101, 400] d3[101, 400] d4[101, 400]
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE time <= 100 and (deviceId = 'd2' or deviceId = 'd3' or deviceId = 'd4' or deviceId = 'd0')");
      expectedPointNumOfDevice = new int[] {300, 351, 300, 300, 300};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d0[101, 150] d1[50, 150] d2[101, 150] d3[101, 150] d4[101, 150]
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE time > 150 and (deviceId = 'd2' or deviceId = 'd3' or deviceId = 'd4' or deviceId = 'd0' or deviceId = 'd1')");
      expectedPointNumOfDevice = new int[] {50, 101, 50, 50, 50};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);
    }
    cleanData(testNum);
  }

  @Test
  public void testMultiDeviceFullDeletionMultiExecution() throws SQLException {
    int testNum = 18;
    prepareData(testNum, 5);
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");

      // init d0[1, 400] d1[1, 400] d2[1, 400] d3[1, 400] d4[1, 400]

      // remain  d2[1, 400] d3[1, 400] d4[1, 400]
      statement.execute(
          "DELETE FROM vehicle" + testNum + " WHERE (deviceId = 'd0' or deviceId = 'd1')");
      int[] expectedPointNumOfDevice = new int[] {0, 0, 400, 400, 400};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain d4[1, 400]
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE (deviceId = 'd1' or deviceId = 'd2' or deviceId = 'd3')");
      expectedPointNumOfDevice = new int[] {0, 0, 0, 0, 400};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      // remain nothing
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE (deviceId = 'd2' or deviceId = 'd3' or deviceId = 'd4' or deviceId = 'd0')");
      expectedPointNumOfDevice = new int[] {0, 0, 0, 0, 0};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);

      /// remain nothing
      statement.execute(
          "DELETE FROM vehicle"
              + testNum
              + " WHERE (deviceId = 'd2' or deviceId = 'd3' or deviceId = 'd4' or deviceId = 'd0' or deviceId = 'd1')");
      expectedPointNumOfDevice = new int[] {0, 0, 0, 0, 0};
      checkDevicePoint(expectedPointNumOfDevice, statement, testNum);
    }
    cleanData(testNum);
  }

  @Test
  public void testMergeDeletion() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database db1");
      statement.execute("use db1");
      statement.execute(
          "create table t1(country id,region id, city id, device id, ab1 ATTRIBUTE, s1 int32, s2 float, s3 boolean, s4 string)");
      statement.execute(
          "INSERT INTO t1(time,country,region,city,device,ab1,s1,s2,s3,s4) values (100,'china','hebei','shijiazhuang','d1','ab1',1,1,1,1),(200,null,'hebei','shijiazhuang','d2','ab2',1,1,1,1),(300,'china','beijing','beijing','d1','ab3',1,1,1,1),(400,'china','tianjin','tianjin','d1','ab4',1,1,1,1),(500,'china','sichuan','chengdu','d1',null,1,1,1,1),(600,'china','zhejiang','hangzhou','d1','ab6',1,1,1,1),(700,'japan','dao','tokyo','d1','ab7',1,1,1,1),(800,'canada','tronto','shijiazhuang','d1','ab8',null,1,1,1),(900,'usa','ca','oldmountain','d1','ab9',1,1,1,1),(1000,'tailand',null,'mangu','d1','ab10',1,1,1,1),(1100,'china','hebei','','d1','ab11',1,1,1,1),(1200,'','hebei','','d1','ab12',1,1,1,1),(1300,'china','','','d1','ab13',1,1,1,1)");
      statement.execute("flush");
      int cnt = 0;
      try (ResultSet set =
          statement.executeQuery(
              "select time,country,region,city,device,ab1,s1,s2,s3,s4 from t1 order by time")) {
        while (set.next()) {
          cnt++;
        }
        assertEquals(13, cnt);
      }
      cnt = 0;
      statement.execute("delete from t1 where country='japan'");
      try (ResultSet set =
          statement.executeQuery(
              "select time,country,region,city,device,ab1,s1,s2,s3,s4 from t1 order by time")) {
        while (set.next()) {
          cnt++;
        }
        assertEquals(12, cnt);
      }
      cnt = 0;
      statement.execute("delete from t1 where country='china' and region='beijing'");
      try (ResultSet set =
          statement.executeQuery(
              "select time,country,region,city,device,ab1,s1,s2,s3,s4 from t1 order by time")) {
        while (set.next()) {
          cnt++;
        }
        assertEquals(11, cnt);
      }
    }
  }

  @Ignore("long test")
  @Test
  public void testConcurrentFlushAndSequentialDeletion()
      throws InterruptedException, ExecutionException {
    AtomicLong writtenPointCounter = new AtomicLong(-1);
    ExecutorService threadPool = Executors.newCachedThreadPool();
    Future<Void> writeThread =
        threadPool.submit(() -> concurrentWrite(writtenPointCounter, threadPool));
    Future<Void> deletionThread =
        threadPool.submit(() -> concurrentSequentialDeletion(writtenPointCounter, threadPool));
    writeThread.get();
    deletionThread.get();
    threadPool.shutdown();
    boolean success = threadPool.awaitTermination(1, TimeUnit.MINUTES);
    assertTrue(success);
  }

  // @Ignore("long test")
  @Test
  public void testConcurrentFlushAndRandomDeletion()
      throws InterruptedException, ExecutionException {
    AtomicLong writtenPointCounter = new AtomicLong(-1);
    ExecutorService threadPool = Executors.newCachedThreadPool();
    Future<Void> writeThread =
        threadPool.submit(() -> concurrentWrite(writtenPointCounter, threadPool));
    Future<Void> deletionThread =
        threadPool.submit(() -> concurrentRandomDeletion(writtenPointCounter, threadPool));
    writeThread.get();
    deletionThread.get();
    threadPool.shutdown();
    boolean success = threadPool.awaitTermination(1, TimeUnit.MINUTES);
    assertTrue(success);
  }

  private Void concurrentWrite(AtomicLong writtenPointCounter, ExecutorService allThreads)
      throws SQLException {
    int fileNumMax = 10000;
    int pointPerFile = 100;

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("create database if not exists test");
      statement.execute("use test");

      statement.execute("create table table1(deviceId STRING ID, s0 INT32 MEASUREMENT)");

      for (int i = 1; i <= fileNumMax; i++) {
        for (int j = 0; j < pointPerFile; j++) {
          statement.execute(
              String.format(
                  "INSERT INTO test.table1(time, deviceId, s0) VALUES(%d,'d0',%d)",
                  writtenPointCounter.get() + 1, writtenPointCounter.get() + 1));
          writtenPointCounter.incrementAndGet();
          if (Thread.interrupted()) {
            return null;
          }
        }
        statement.execute("FLUSH");
      }
    } catch (Throwable e) {
      allThreads.shutdownNow();
      throw e;
    }
    return null;
  }

  private Void concurrentSequentialDeletion(
      AtomicLong writtenPointCounter, ExecutorService allThreads)
      throws SQLException, InterruptedException {
    // delete every 10 points in 100 points
    int deletionOffset = 0;
    int deletionInterval = 100;
    int deletionRange = 10;
    long nextPointNumToDelete = deletionInterval;
    // pointPerFile * fileNumMax
    long deletionEnd = 100 * 10000;

    long deletedCnt = 0;

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("create database if not exists test");
      statement.execute("use test");
      while (deletionOffset < deletionEnd && !Thread.interrupted()) {
        if (writtenPointCounter.get() >= nextPointNumToDelete) {
          statement.execute(
              "delete from test.table1 where time >= "
                  + deletionOffset
                  + " and time < "
                  + (deletionOffset + deletionRange));
          deletedCnt += deletionRange;
          LOGGER.info("{} points deleted", deletedCnt);

          try (ResultSet set =
              statement.executeQuery(
                  "select count(*) from table1 where time < " + nextPointNumToDelete)) {
            assertTrue(set.next());
            assertEquals(nextPointNumToDelete * 9 / 10, set.getLong(1));
          }
          deletionOffset += deletionInterval;
          nextPointNumToDelete += deletionInterval;

        } else {
          Thread.sleep(10);
        }
      }
    } catch (Throwable e) {
      allThreads.shutdownNow();
      throw e;
    }
    return null;
  }

  private Void concurrentRandomDeletion(AtomicLong writtenPointCounter, ExecutorService allThreads)
      throws SQLException, InterruptedException {
    // delete random 10 points each time
    int deletionRange = 10;
    int minIntervalToRecord = 100;
    List<TimeRange> undeletedRanges = new ArrayList<>();
    // pointPerFile * fileNumMax
    long deletionEnd = 100 * 10000;
    long deletedCnt = 0;
    long nextRangeStart = 0;
    Random random = new Random();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("create database if not exists test");
      statement.execute("use test");
      while ((writtenPointCounter.get() < deletionEnd || !undeletedRanges.isEmpty())
          && !Thread.interrupted()) {
        // record the newly inserted interval if it is long enough
        long currentWrittenTime = writtenPointCounter.get();
        if (currentWrittenTime - nextRangeStart >= minIntervalToRecord) {
          undeletedRanges.add(new TimeRange(nextRangeStart, currentWrittenTime));
          nextRangeStart = currentWrittenTime + 1;
        }
        if (undeletedRanges.isEmpty()) {
          Thread.sleep(10);
          continue;
        }
        // pick up a random range
        int rangeIndex = random.nextInt(undeletedRanges.size());
        TimeRange timeRange = undeletedRanges.get(rangeIndex);
        // delete a random part in the range
        LOGGER.info("Pick up a range [{}, {}]", timeRange.getMin(), timeRange.getMax());
        long rangeDeletionStart;
        long timeRangeLength = timeRange.getMax() - timeRange.getMin() + 1;
        if (timeRangeLength == 1) {
          rangeDeletionStart = timeRange.getMin();
        } else {
          rangeDeletionStart = random.nextInt((int) (timeRangeLength - 1)) + timeRange.getMin();
        }
        long rangeDeletionEnd = Math.min(rangeDeletionStart + deletionRange, timeRange.getMax());
        LOGGER.info("Deletion range [{}, {}]", rangeDeletionStart, rangeDeletionEnd);

        statement.execute(
            "delete from test.table1 where time >= "
                + rangeDeletionStart
                + " and time <= "
                + rangeDeletionEnd);
        deletedCnt += rangeDeletionEnd - rangeDeletionStart + 1;
        LOGGER.info(
            "Deleted range [{}, {}], written points: {}, deleted points: {}",
            timeRange.getMin(),
            timeRange.getMax(),
            currentWrittenTime + 1,
            deletedCnt);

        // update the range
        if (rangeDeletionStart == timeRange.getMin() && rangeDeletionEnd == timeRange.getMax()) {
          // range fully deleted
          undeletedRanges.remove(rangeIndex);
        } else if (rangeDeletionStart == timeRange.getMin()) {
          // prefix deleted
          timeRange.setMin(rangeDeletionEnd + 1);
        } else if (rangeDeletionEnd == timeRange.getMax()) {
          // suffix deleted
          timeRange.setMax(rangeDeletionStart - 1);
        } else {
          // split into two ranges
          undeletedRanges.add(new TimeRange(rangeDeletionEnd + 1, timeRange.getMax()));
          timeRange.setMax(rangeDeletionStart - 1);
        }

        // check the point count
        try (ResultSet set =
            statement.executeQuery(
                "select count(*) from table1 where time <= " + currentWrittenTime)) {
          assertTrue(set.next());
          long expectedCnt = currentWrittenTime + 1 - deletedCnt;
          if (expectedCnt != set.getLong(1)) {
            undeletedRanges = mergeRanges(undeletedRanges);
            List<TimeRange> remainingRanges = collectDataRanges(statement, currentWrittenTime);
            LOGGER.info("Expected ranges: {}", undeletedRanges);
            LOGGER.info("Remaining ranges: {}", remainingRanges);
            fail(
                String.format(
                    "Inconsistent number of points %d - %d", expectedCnt, set.getLong(1)));
          }
        }

        Thread.sleep(10);
      }
    } catch (Throwable e) {
      allThreads.shutdownNow();
      throw e;
    }
    return null;
  }

  private List<TimeRange> mergeRanges(List<TimeRange> timeRanges) {
    timeRanges.sort(null);
    List<TimeRange> result = new ArrayList<>();
    TimeRange current = null;
    for (TimeRange timeRange : timeRanges) {
      if (current == null) {
        current = timeRange;
      } else {
        if (current.getMax() == timeRange.getMin() - 1) {
          current.setMax(timeRange.getMax());
        } else {
          result.add(current);
          current = timeRange;
        }
      }
    }
    result.add(current);
    return result;
  }

  private List<TimeRange> collectDataRanges(Statement statement, long timeUpperBound)
      throws SQLException {
    List<TimeRange> ranges = new ArrayList<>();
    try (ResultSet set =
        statement.executeQuery("select time from table1 where time <= " + timeUpperBound)) {
      while (set.next()) {
        long time = set.getLong(1);
        if (ranges.isEmpty()) {
          ranges.add(new TimeRange(time, time));
        } else {
          TimeRange lastRange = ranges.get(ranges.size() - 1);
          if (lastRange.getMax() == time - 1) {
            lastRange.setMax(time);
          } else {
            ranges.add(new TimeRange(time, time));
          }
        }
      }
    }
    return ranges;
  }

  @Ignore("performance")
  @Test
  public void testDeletionWritePerformance() throws SQLException, IOException {
    int fileNumMax = 10000;
    int fileNumStep = 100;
    int deletionRepetitions = 10;
    List<Integer> fileNumsRecorded = new ArrayList<>();
    List<Long> timeConsumptionNsRecorded = new ArrayList<>();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION enable_seq_space_compaction='false'");

      statement.execute("create database if not exists test");
      statement.execute("use test");

      statement.execute("create table table1(deviceId STRING ID, s0 INT32 MEASUREMENT)");

      for (int i = 1; i <= fileNumMax; i++) {
        statement.execute(
            String.format("INSERT INTO test.table1(time, deviceId, s0) VALUES(%d,'d0',%d)", i, i));
        statement.execute("FLUSH");

        if (i % fileNumStep == 0) {
          long start = System.nanoTime();
          for (int j = 0; j < deletionRepetitions; j++) {
            statement.execute("DELETE FROM test.table1 WHERE deviceId = 'd0'");
          }
          long end = System.nanoTime();
          fileNumsRecorded.add(i);
          long timeConsumption = (end - start) / deletionRepetitions;
          timeConsumptionNsRecorded.add(timeConsumption);

          System.out.println(i + "," + timeConsumption);
        }
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter("test.txt"))) {
      writer.write(
          fileNumsRecorded.stream().map(i -> Integer.toString(i)).collect(Collectors.joining(",")));
      writer.write("\n");
      writer.write(
          timeConsumptionNsRecorded.stream()
              .map(i -> Long.toString(i))
              .collect(Collectors.joining(",")));
      writer.flush();
    }
  }

  @Ignore("performance")
  @Test
  public void testDeletionReadPerformance() throws SQLException, IOException {
    int fileNumMax = 100;
    int pointNumPerFile = 100;
    int deletionNumStep = 100;
    int maxDeletionNum = 10000;
    int readRepetitions = 5;
    List<Integer> deletionNumsRecorded = new ArrayList<>();
    List<Long> timeConsumptionNsRecorded = new ArrayList<>();

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("SET CONFIGURATION enable_seq_space_compaction='false'");

      statement.execute("create database if not exists test");
      statement.execute("use test");

      statement.execute("create table table1(deviceId STRING ID, s0 INT32 MEASUREMENT)");

      for (int i = 1; i <= fileNumMax; i++) {
        for (int j = 0; j < pointNumPerFile; j++) {
          long timestamp = (long) (i - 1) * pointNumPerFile + j;
          statement.execute(
              String.format(
                  "INSERT INTO test.table1(time, deviceId, s0) VALUES(%d,'d0',%d)",
                  timestamp, timestamp));
        }
        statement.execute("FLUSH");
      }

      for (int i = 1; i <= maxDeletionNum; i++) {
        statement.execute("DELETE FROM test.table1 WHERE deviceId = 'd0'");
        if (i % deletionNumStep == 0) {
          long start = System.nanoTime();
          for (int j = 0; j < readRepetitions; j++) {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM test.table1");
            //noinspection StatementWithEmptyBody
            while (resultSet.next()) {
              // just iterate the set
            }
          }
          long end = System.nanoTime();
          long timeConsumption = (end - start) / readRepetitions;
          timeConsumptionNsRecorded.add(timeConsumption);
          deletionNumsRecorded.add(i);
          System.out.println(i + "," + timeConsumption);
        }
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter("test.txt"))) {
      writer.write(
          deletionNumsRecorded.stream()
              .map(i -> Integer.toString(i))
              .collect(Collectors.joining(",")));
      writer.write("\n");
      writer.write(
          timeConsumptionNsRecorded.stream()
              .map(i -> Long.toString(i))
              .collect(Collectors.joining(",")));
      writer.flush();
    }
  }

  private static void prepareSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void prepareData(int testNum, int deviceNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      statement.execute(
          String.format(
              "CREATE TABLE IF NOT EXISTS vehicle%d(deviceId STRING ID, s0 INT32 MEASUREMENT, s1 INT64 MEASUREMENT, s2 FLOAT MEASUREMENT, s3 TEXT MEASUREMENT, s4 BOOLEAN MEASUREMENT)",
              testNum));

      for (int d = 0; d < deviceNum; d++) {
        // prepare seq file
        for (int i = 201; i <= 300; i++) {
          statement.execute(
              String.format(
                  insertTemplate, testNum, i, d, i, i, (double) i, "'" + i + "'", i % 2 == 0));
        }
      }

      statement.execute("flush");

      for (int d = 0; d < deviceNum; d++) {
        // prepare unseq File
        for (int i = 1; i <= 100; i++) {
          statement.execute(
              String.format(
                  insertTemplate, testNum, i, d, i, i, (double) i, "'" + i + "'", i % 2 == 0));
        }
      }
      statement.execute("flush");

      for (int d = 0; d < deviceNum; d++) {
        // prepare BufferWrite cache
        for (int i = 301; i <= 400; i++) {
          statement.execute(
              String.format(
                  insertTemplate, testNum, i, d, i, i, (double) i, "'" + i + "'", i % 2 == 0));
        }
        // prepare Overflow cache
        for (int i = 101; i <= 200; i++) {
          statement.execute(
              String.format(
                  insertTemplate, testNum, i, d, i, i, (double) i, "'" + i + "'", i % 2 == 0));
        }
      }
    }
  }

  private void cleanData(int testNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("use test");
      String deleteAllTemplate = "DROP TABLE IF EXISTS vehicle%d";
      statement.execute(String.format(deleteAllTemplate, testNum));
    }
  }
}
