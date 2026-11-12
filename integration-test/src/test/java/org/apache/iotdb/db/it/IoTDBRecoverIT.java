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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.utils.constant.TestConstant.count;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.minTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.minValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRecoverIT {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBRecoverIT.class);

  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";
  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE root.vehicle.d0",
        "CREATE DATABASE root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };
  private static final String[] dataSet2 =
      new String[] {
        "CREATE DATABASE root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";

  @BeforeClass
  public static void setUpCluster() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Before
  public void setUp() throws SQLException {
    // Recreate the same file layout before each recovery on the shared cluster.
    prepareData();
  }

  @After
  public void tearDown() throws SQLException {
    // Remove this class's databases so neither recovered data nor schemas reach the next test.
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE DATABASE root.vehicle.d0, root.vehicle.d1, root.ln.wf01.wt01");
    }
  }

  /**
   * Restart all nodes and verify filtered aggregates over recovered sequence and unsequence data.
   */
  @Test
  public void recoverTest1() {
    // stop cluster
    EnvFactory.getEnv().shutdownAllDataNodes();
    logger.info("All DataNodes are shut down");
    EnvFactory.getEnv().shutdownAllConfigNodes();
    logger.info("All ConfigNodes are shut down");
    EnvFactory.getEnv().startAllConfigNodes();
    logger.info("All ConfigNodes are started");
    EnvFactory.getEnv().startAllDataNodes();
    logger.info("All DataNodes are started");
    // check cluster whether restart
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();
    String[] retArray = new String[] {"2", "4", "3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String selectSql = "select count(temperature) from root.ln.wf01.wt01 where time > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans = resultSet.getString(count(TEMPERATURE_STR));
        Assert.assertEquals(retArray[0], ans);
      }

      selectSql = "select min_time(temperature) from root.ln.wf01.wt01 where time > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans = resultSet.getString(minTime(TEMPERATURE_STR));
        Assert.assertEquals(retArray[1], ans);
      }

      selectSql = "select min_time(temperature) from root.ln.wf01.wt01 where temperature > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans = resultSet.getString(minTime(TEMPERATURE_STR));
        Assert.assertEquals(retArray[2], ans);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // max min ValueTest
    retArray = new String[] {"8499,500.0", "2499,500.0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String selectSql =
          "select max_value(s0),min_value(s2) "
              + "from root.vehicle.d0 where time >= 100 and time < 9000";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString(maxValue(d0s0)) + "," + resultSet.getString(minValue(d0s2));
        Assert.assertEquals(retArray[0], ans);
      }

      selectSql = "select max_value(s0),min_value(s2) from root.vehicle.d0 where time < 2500";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString(maxValue(d0s0)) + "," + resultSet.getString(minValue(d0s2));
        Assert.assertEquals(retArray[1], ans);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Restart all nodes and verify recovered counts for overlapping writes and a time range. */
  @Test
  public void recoverTest2() {
    // stop cluster
    EnvFactory.getEnv().shutdownAllDataNodes();
    logger.info("All DataNodes are shut down");
    EnvFactory.getEnv().shutdownAllConfigNodes();
    logger.info("All ConfigNodes are shut down");
    EnvFactory.getEnv().startAllConfigNodes();
    logger.info("All ConfigNodes are started");
    EnvFactory.getEnv().startAllDataNodes();
    logger.info("All DataNodes are started");
    // wait for cluster to start and check
    ((AbstractEnv) EnvFactory.getEnv()).checkClusterStatusWithoutUnknown();
    // count test
    String[] retArray = new String[] {"2001,2001,2001,2001", "7500,7500,7500,7500"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String selectSql =
          "select count(s0),count(s1),count(s2),count(s3) "
              + "from root.vehicle.d0 where time >= 6000 and time <= 9000";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString(count(d0s0))
                + ","
                + resultSet.getString(count(d0s1))
                + ","
                + resultSet.getString(count(d0s2))
                + ","
                + resultSet.getString(count(d0s3));
        Assert.assertEquals(retArray[0], ans);
      }

      selectSql = "select count(s0),count(s1),count(s2),count(s3) from root.vehicle.d0";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString(count(d0s0))
                + ","
                + resultSet.getString(count(d0s1))
                + ","
                + resultSet.getString(count(d0s2))
                + ","
                + resultSet.getString(count(d0s3));
        Assert.assertEquals(retArray[1], ans);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      insertRange(statement, 5000, 7000, true);
      statement.execute("flush");
      insertRange(statement, 7500, 8500, false);
      statement.execute("flush");
      // prepare Unseq-File
      insertRange(statement, 500, 1500, true);
      statement.execute("flush");
      insertRange(statement, 3000, 6500, false);
      statement.execute("flush");

      // prepare BufferWrite cache
      insertRange(statement, 9000, 10000, true);
      // prepare Overflow cache
      insertRange(statement, 2000, 2500, false);
      statement.execute("flush");
    }
  }

  private static void insertRange(
      Statement statement, int startTime, int endTime, boolean booleanValue) throws SQLException {
    // Reduce SQL parsing and execution overhead without changing rows, write order or flush
    // boundaries.
    final int rowsPerInsert = 500;
    for (int batchStart = startTime; batchStart < endTime; batchStart += rowsPerInsert) {
      StringBuilder sql =
          new StringBuilder("INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4) VALUES");
      int batchEnd = Math.min(batchStart + rowsPerInsert, endTime);
      for (int time = batchStart; time < batchEnd; time++) {
        if (time > batchStart) {
          sql.append(',');
        }
        sql.append('(')
            .append(time)
            .append(',')
            .append(time)
            .append(',')
            .append(time)
            .append(',')
            .append((double) time)
            .append(",'")
            .append(time)
            .append("',")
            .append(booleanValue)
            .append(')');
      }
      statement.execute(sql.toString());
    }
  }
}
