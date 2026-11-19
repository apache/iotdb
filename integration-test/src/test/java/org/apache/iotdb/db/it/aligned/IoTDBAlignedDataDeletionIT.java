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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
public class IoTDBAlignedDataDeletionIT {

  // Missing paths make DELETE TIMESERIES fail, so cleanup tracks which non-fixture paths each test
  // creates.
  private boolean nestedDatabaseCreated;
  private boolean lnSeriesCreated;
  private boolean lzSeriesCreated;

  private static String[] creationSqls =
      new String[] {
        "CREATE ALIGNED TIMESERIES root.vehicle.d0(s0 INT32 ENCODING=RLE, s1 INT64 ENCODING=RLE, s2 FLOAT ENCODING=RLE, s3 TEXT ENCODING=PLAIN, s4 BOOLEAN ENCODING=PLAIN)",
      };

  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4"
          + ") ALIGNED VALUES(%d,%d,%d,%f,%s,%b)";
  private String deleteAllTemplate = "DELETE FROM root.vehicle.d0.* WHERE time <= 10000";

  // Reuse the shared test cluster; recreate the per-method schema and remove each method's paths.
  @BeforeClass
  public static void setUpClass() throws Exception {
    Locale.setDefault(Locale.ENGLISH);
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        // Adjust memstable threshold size to make it flush automatically
        .setMemtableSizeThreshold(10000);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.vehicle");
    }
  }

  @Before
  public void setUp() {
    nestedDatabaseCreated = false;
    lnSeriesCreated = false;
    lzSeriesCreated = false;
    prepareSeries();
  }

  @After
  public void tearDown() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      if (nestedDatabaseCreated) {
        statement.execute("DROP DATABASE root.ln.wf01.wt01");
      }
      statement.execute("DELETE TIMESERIES root.vehicle.**");
      if (lnSeriesCreated) {
        statement.execute("DELETE TIMESERIES root.ln.d1.**");
      }
      if (lzSeriesCreated) {
        statement.execute("DELETE TIMESERIES root.lz.dev.GPS.**");
      }
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Should delete this case after the deletion value filter feature be implemented
   *
   * @throws SQLException
   */
  // Reject value-filter predicates for three data types and preserve the matching rows.
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

  // Delete inclusive and overlapping time ranges across aligned columns; verify 250, 100, and 50
  // rows remain.
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

  // Delete through NOW before and after flush; the query must return no remaining rows.
  @Test
  public void testDelAfterFlush() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.ln.wf01.wt01");
      nestedDatabaseCreated = true;
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

  // Combine single-column and aligned-range deletes with overlapping bounds; verify each resulting
  // row count.
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

  // Delete an entire extended column from sparse aligned rows without returning an error.
  @Test
  public void testDeleteAfterColumnExtended() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.vehicle.d0(time,s0) aligned values (10,310)");
      statement.execute("insert into root.vehicle.d0(time,s3) aligned values (10,'text')");
      statement.execute("insert into root.vehicle.d0(time,s4) aligned values (10,true)");

      try {
        statement.execute("DELETE FROM root.vehicle.d0.s3");
      } catch (SQLException e) {
        fail(e.getMessage());
      }
    }
  }

  // Delete all values from one aligned column without a time predicate; its query must be empty.
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

  // Delete through partial measurement and device paths; verify the remaining counts are 250 and
  // 150.
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

  // Insert 10,000 aligned rows and delete (1500,9000]; 2,500 values must remain in the target
  // column.
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

  // Delete several ranges across two batches and a flush; 1,100 values must remain in the target
  // column.
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

  // Delete a quoted measurement containing a comma by range and then fully; counts must go from two
  // to zero.
  @Test
  public void testDelSeriesWithSpecialSymbol() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.ln.d1.`status,01` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      lnSeriesCreated = true;
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

  // Insert partial aligned rows at a duplicate timestamp, then delete latitude; both timestamps
  // must remain.
  @Test
  public void testInsertDuplicatedTimeThenDel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)");
      lzSeriesCreated = true;
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

  // Delete every series under root.lz after aligned inserts; the subsequent query must return no
  // rows.
  @Test
  public void testDeleteAll() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE ALIGNED TIMESERIES root.lz.dev.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY)");
      lzSeriesCreated = true;
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
      statement.execute("flush");
      // prepare Unseq-File
      for (int i = 1; i <= 100; i++) {
        statement.execute(
            String.format(insertTemplate, i, i, i, (double) i, "'" + i + "'", i % 2 == 0));
      }
      statement.execute("flush");
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
}
