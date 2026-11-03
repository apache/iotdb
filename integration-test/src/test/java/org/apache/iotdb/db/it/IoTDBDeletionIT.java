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

package org.apache.iotdb.db.it;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.rpc.TSStatusCode;

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
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeletionIT {

  private static String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      };

  private String insertTemplate =
      "INSERT INTO root.vehicle%d.d0(timestamp,s0,s1,s2,s3,s4" + ") VALUES(%d,%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DELETE FROM root.vehicle%d.d0.* WHERE time <= 10000";

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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.vehicle1.d0(time,s0) values (10,310)");
      statement.execute("insert into root.vehicle1.d0(time,s3) values (10,'text')");
      statement.execute("insert into root.vehicle1.d0(time,s4) values (10,true)");

      String errorMsg =
          TSStatusCode.SEMANTIC_ERROR.getStatusCode()
              + ": For delete statement, where clause can only contain time expressions, value filter is not currently supported.";

      try {
        statement.execute("DELETE FROM root.vehicle1.d0.s0  WHERE s0 <= 300 AND s0 > 0");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try {
        statement.execute("DELETE FROM root.vehicle1.d0.s3  WHERE s3 = 'text'");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try {
        statement.execute("DELETE FROM root.vehicle1.d0.s4  WHERE s4 != true");
        fail("should not reach here!");
      } catch (SQLException e) {
        assertEquals(errorMsg, e.getMessage());
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle1.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s3 FROM root.vehicle1.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s4 FROM root.vehicle1.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(1, cnt);
      }
      try (ResultSet rows = statement.executeQuery("SELECT s0,s3,s4 FROM root.vehicle1.d0")) {
        TestUtils.assertResultSetEqual(
            rows,
            "Time,root.vehicle1.d0.s0,root.vehicle1.d0.s3,root.vehicle1.d0.s4,",
            new String[] {"10,310.0,text,true,"});
      }
      statement.execute("DELETE FROM root.vehicle1.d0.s0 WHERE time=10");
      statement.execute("FLUSH");
      try (ResultSet rows = statement.executeQuery("SELECT s0,s3,s4 FROM root.vehicle1.d0")) {
        TestUtils.assertResultSetEqual(
            rows,
            "Time,root.vehicle1.d0.s0,root.vehicle1.d0.s3,root.vehicle1.d0.s4,",
            new String[] {"10,null,text,true,"});
      }
    }
  }

  @Test
  public void test() throws SQLException {
    prepareData(2);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle2.d0.s0  WHERE time <= 300");
      statement.execute(
          "DELETE FROM root.vehicle2.d0.s1,root.vehicle2.d0.s2,root.vehicle2.d0.s3"
              + " WHERE time <= 350");
      statement.execute("DELETE FROM root.vehicle2.d0.** WHERE time <= 150");

      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle2.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle2.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1,s2,s3 FROM root.vehicle2.d0")) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln3.wf01.wt01");
      statement.execute(
          "CREATE TIMESERIES root.ln3.wf01.wt01.status WITH DATATYPE=BOOLEAN," + " ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.ln3.wf01.wt01(timestamp,status) " + "values(1509465600000,true)");
      statement.execute("INSERT INTO root.ln3.wf01.wt01(timestamp,status) VALUES(NOW(), false)");

      statement.execute("delete from root.ln3.wf01.wt01.status where time <= NOW()");
      statement.execute("flush");
      statement.execute("delete from root.ln3.wf01.wt01.status where time <= NOW()");

      try (ResultSet resultSet = statement.executeQuery("select status from root.ln3.wf01.wt01")) {
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testRangeDelete() throws SQLException {
    prepareData(4);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle4.d0.s0 WHERE time <= 300");
      statement.execute("DELETE FROM root.vehicle4.d0.s1 WHERE time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle4.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(100, cnt);
      }

      try (ResultSet set = statement.executeQuery("SELECT s1 FROM root.vehicle4.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }

      statement.execute("DELETE FROM root.vehicle4.d0.** WHERE time > 50 and time <= 250");
      try (ResultSet set = statement.executeQuery("SELECT * FROM root.vehicle4.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(200, cnt);
      }
    }
    cleanData(4);
  }

  @Test
  public void testFullDeleteWithoutWhereClause() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM root.vehicle5.d0.s0");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle5.d0")) {
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
  public void testPartialPathRangeDelete() throws SQLException {
    prepareData(6);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("DELETE FROM root.vehicle6.d0.* WHERE time <= 300 and time > 150");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle6.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(250, cnt);
      }

      statement.execute("DELETE FROM root.vehicle6.*.s0 WHERE time <= 100");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle6.d0")) {
        int cnt = 0;
        while (set.next()) {
          cnt++;
        }
        assertEquals(150, cnt);
      }
    }
    cleanData(6);
  }

  @Test
  public void testDelFlushingMemtable() throws SQLException {
    int testNum = 7;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 10000; i++) {
        statement.execute(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }

      statement.execute("DELETE FROM root.vehicle7.d0.s0 WHERE time > 1500 and time <= 9000");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle7.d0")) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (int i = 1; i <= 1000; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
      statement.clearBatch();

      statement.execute("DELETE FROM root.vehicle8.d0.s0 WHERE time > 150 and time <= 300");
      statement.execute("DELETE FROM root.vehicle8.d0.s0 WHERE time > 300 and time <= 400");
      for (int i = 1001; i <= 2000; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("DELETE FROM root.vehicle8.d0.s0 WHERE time > 500 and time <= 800");
      statement.execute("DELETE FROM root.vehicle8.d0.s0 WHERE time > 900 and time <= 1100");
      statement.execute("DELETE FROM root.vehicle8.d0.s0 WHERE time > 1500 and time <= 1650");
      statement.execute("flush");
      try (ResultSet set = statement.executeQuery("SELECT s0 FROM root.vehicle8.d0")) {
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.lz9.dev.GPS(time, latitude, longitude) values(9,3.2,9.8)");
      statement.execute("insert into root.lz9.dev.GPS(time, latitude) values(11,4.5)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz9.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("delete from root.lz9.**");

      try (ResultSet resultSet = statement.executeQuery("select * from root.lz9.dev.GPS")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void testDeleteDataFromEmptySeries() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.`0`.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN;");
      statement.execute(
          "INSERT INTO root.ln10.wf01.wt01(Time,status) VALUES (2022-10-11 10:20:50,true),(2022-10-11 10:20:51,true);");
      statement.execute(
          "create timeseries root.sg10.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN;");

      statement.execute(
          "DELETE FROM root.ln10.wf01.wt01.status,root.sg.wf01.wt01.status WHERE time >2022-10-11 10:20:50;");

      try (ResultSet resultSet = statement.executeQuery("select ** from root.ln10")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testDeleteDataAfterInsertFailed() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.dd.wf01.wt01.status with datatype=BOOLEAN;");
      statement.execute(
          "INSERT INTO root.dd.wf01.wt01(Time,status) VALUES (2022-10-11 10:20:50,'rr');");
    } catch (SQLException e) {
      assertEquals(507, e.getErrorCode());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM root.dd.wf01.wt01.status");

      try (ResultSet resultSet = statement.executeQuery("select ** from root.dd")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void testDelSeriesWithSpecialSymbol() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.ln11.d1.`status,01` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln11.d1(timestamp,`status,01`) VALUES(300, true)");
      statement.execute("INSERT INTO root.ln11.d1(timestamp,`status,01`) VALUES(500, false)");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln11.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      statement.execute("DELETE FROM root.ln11.d1.`status,01` WHERE time <= 400");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln11.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      statement.execute("DELETE FROM root.ln11.d1.`status,01`");

      try (ResultSet resultSet = statement.executeQuery("select `status,01` from root.ln11.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void testDelAfterUpdate() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE ALIGNED TIMESERIES root.ln12.d1 (status int32)");
      statement.execute("INSERT INTO root.ln12.d1(timestamp, status) VALUES(1, 1)");
      statement.execute("INSERT INTO root.ln12.d1(timestamp, status) VALUES(2, 2)");
      statement.execute("INSERT INTO root.ln12.d1(timestamp, status) VALUES(3, 3)");
      statement.execute("INSERT INTO root.ln12.d1(timestamp, status) VALUES(2, 2)");

      try (ResultSet resultSet = statement.executeQuery("select status from root.ln12.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      statement.execute("DELETE FROM root.ln12.d1.* WHERE time <= 2");

      try (ResultSet resultSet = statement.executeQuery("select status from root.ln12.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void testDeleteByRangeComparison() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.test");
      statement.execute("CREATE ALIGNED TIMESERIES root.test.g_0.d2(s_0 int32)");
      statement.execute("INSERT INTO root.test.g_0.d_2(time,s_0) VALUES(1, 1)");

      try {
        statement.execute("DELETE FROM root.test.g_0.d_2.s_0 WHERE time > 4 AND time < 0");
      } catch (IoTDBSQLException e) {
        Assert.assertEquals(
            e.getErrorCode() + ": " + ASTVisitor.DELETE_RANGE_COMPARISON_ERROR_MSG, e.getMessage());
      }

      statement.execute("DROP DATABASE root.test");
    }
  }

  @Test
  public void testOverlappingDeleteRangesAtTimePartitionBoundaries() throws SQLException {
    final String database = "root.delete_partition_boundary";
    final long[] timestamps = {
      -1, 0, 1, 998, 999, 1000, 1001, 1002, 1003, 1998, 1999, 2000, 2001, 4102444800000L
    };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + database);
      statement.execute("CREATE TIMESERIES " + database + ".d1.s1 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES " + database + ".d1.s2 WITH DATATYPE=INT64");
      statement.execute("CREATE TIMESERIES " + database + ".d2.s1 WITH DATATYPE=INT64");

      for (long timestamp : timestamps) {
        statement.execute(
            "INSERT INTO "
                + database
                + ".d1(time,s1,s2) VALUES("
                + timestamp
                + ","
                + (timestamp + 100)
                + ","
                + (timestamp + 10000)
                + ")");
        statement.execute(
            "INSERT INTO "
                + database
                + ".d2(time,s1) VALUES("
                + timestamp
                + ","
                + (timestamp + 30000)
                + ")");
      }
      statement.execute("FLUSH");

      // Leave unflushed values on both sides of the boundary as well as the closed-file data.
      statement.execute("INSERT INTO " + database + ".d1(time,s1) VALUES(999,9999),(1001,10001)");
      // The interval is 1000. Overlapping, adjacent and disjoint deletions must preserve the gap
      // at 1003..1998, the unselected measurement and the other device.
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time >= 999 AND time <= 1000");
      try (ResultSet rows =
          statement.executeQuery("SELECT s1 FROM " + database + ".d1 WHERE time=1000")) {
        assertFalse(rows.next());
      }
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time >= 1000 AND time <= 1001");
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time = 1002");
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time > 1998 AND time < 2001");
      statement.execute("INSERT INTO " + database + ".d1(time,s1) VALUES(1500,15000)");
      // No writes within this range occur between retries. The disjoint new point must survive.
      statement.execute("DELETE FROM " + database + ".d1.s1 WHERE time >= 999 AND time <= 1000");
      statement.execute("INSERT INTO " + database + ".d1(time,s1) VALUES(1000,900)");
      statement.execute("FLUSH");
    }

    TestUtils.resultSetEqualTest(
        "SELECT s1,s2 FROM " + database + ".d1 ORDER BY time",
        new String[] {"Time", database + ".d1.s1", database + ".d1.s2"},
        new String[] {
          "-1,99,9999,",
          "0,100,10000,",
          "1,101,10001,",
          "998,1098,10998,",
          "999,null,10999,",
          "1000,900,11000,",
          "1001,null,11001,",
          "1002,null,11002,",
          "1003,1103,11003,",
          "1500,15000,null,",
          "1998,2098,11998,",
          "1999,null,11999,",
          "2000,null,12000,",
          "2001,2101,12001,",
          "4102444800000,4102444800100,4102444810000,"
        });
    TestUtils.resultSetEqualTest(
        "SELECT s1 FROM " + database + ".d2 ORDER BY time",
        new String[] {"Time", database + ".d2.s1"},
        new String[] {
          "-1,29999,",
          "0,30000,",
          "1,30001,",
          "998,30998,",
          "999,30999,",
          "1000,31000,",
          "1001,31001,",
          "1002,31002,",
          "1003,31003,",
          "1998,31998,",
          "1999,31999,",
          "2000,32000,",
          "2001,32001,",
          "4102444800000,4102444830000,"
        });
  }

  private static void prepareSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void prepareData(int testNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite file
      for (int i = 201; i <= 300; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.addBatch("flush");
      // prepare Unseq-File
      for (int i = 1; i <= 100; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.addBatch("flush");
      // prepare BufferWrite cache
      for (int i = 301; i <= 400; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      // prepare Overflow cache
      for (int i = 101; i <= 200; i++) {
        statement.addBatch(
            String.format(insertTemplate, testNum, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.executeBatch();
    }
  }

  private void cleanData(int testNum) throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(String.format(deleteAllTemplate, testNum));
    }
  }
}
