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
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.utils.constant.TestConstant.count;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.minTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.minValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
@Ignore // aggregation
public class IoTDBRecoverUnclosedTableIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBRecoverUnclosedTableIT.class);
  private static final String TIMESTAMP_STR = "time";
  private static final String TEMPERATURE_STR = "temperature";
  private static final String[] creationSqls =
      new String[] {
        "CREATE DATABASE test",
        "USE \"test\"",
        "CREATE TABLE vehicle (id1 string id, s0 int32 measurement, s1 int64 measurement, s2 float measurement, s3 text measurement, s4 boolean measurement)"
      };
  private static final String[] dataSet2 =
      new String[] {
        "CREATE DATABASE ln",
        "USE \"ln\"",
        "CREATE TABLE wf01 (id1 string id, status boolean measurement, temperature float measurement, hardware int32 measurement)",
        "INSERT INTO wf01(id1, time,temperature,status, hardware) "
            + "values('wt01', 1, 1.1, false, 11)",
        "INSERT INTO wf01(id1, time,temperature,status, hardware) "
            + "values('wt01', 2, 2.2, true, 22)",
        "INSERT INTO wf01(id1, time,temperature,status, hardware) "
            + "values('wt01', 3, 3.3, false, 33 )",
        "INSERT INTO wf01(id1, time,temperature,status, hardware) "
            + "values('wt01', 4, 4.4, false, 44)",
        "INSERT INTO wf01(id1, time,temperature,status, hardware) "
            + "values('wt01',5, 5.5, false, 55)"
      };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setWalMode("SYNC");
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws SQLException, IOException {
    String[] retArray = new String[] {"0,2", "0,4", "0,3"};
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.execute("use \"ln\"");
      String selectSql = "select count(temperature) from wf01 where time > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString("time") + "," + resultSet.getString(count(TEMPERATURE_STR));
        assertEquals(retArray[0], ans);
      }

      selectSql = "select min_time(temperature) from wf01 where time > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString("time") + "," + resultSet.getString(minTime(TEMPERATURE_STR));
        assertEquals(retArray[1], ans);
      }

      selectSql = "select min_time(temperature) from wf01 where temperature > 3";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString("time") + "," + resultSet.getString(minTime(TEMPERATURE_STR));
        assertEquals(retArray[2], ans);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    insertMoreData();

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

    // test count,
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String selectSql = "select count(*) from vehicle";
      ResultSet tempResultSet = statement.executeQuery(selectSql);
      assertNotNull(tempResultSet);
      tempResultSet.next();
      String d0s0 = "s0";
      String d0s1 = "s1";
      String d0s2 = "s2";
      assertEquals(7500, tempResultSet.getInt("count(" + d0s0 + ")"));

      // test max, min value
      retArray = new String[] {"0,8499,500.0", "0,2499,500.0"};
      selectSql =
          "select max_value(s0),min_value(s2) " + "from vehicle where time >= 100 and time < 9000";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(maxValue(d0s0))
                + ","
                + resultSet.getString(minValue(d0s2));
        assertEquals(retArray[0], ans);
      }

      selectSql = "select max_value(s1),min_value(s2) from vehicle where time < 2500";
      try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        assertNotNull(resultSet);
        resultSet.next();
        String ans =
            resultSet.getString("time")
                + ","
                + resultSet.getString(maxValue(d0s1))
                + ","
                + resultSet.getString(minValue(d0s2));
        assertEquals(retArray[1], ans);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void insertMoreData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // prepare BufferWrite file
      String insertTemplate =
          "INSERT INTO vehicle(id1,timestamp,s0,s1,s2,s3,s4)" + " VALUES('d0',%d,%d,%d,%f,%s,%s)";
      for (int i = 5000; i < 7000; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.executeBatch();
      for (int i = 7500; i < 8500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      for (int i = 3000; i < 6500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.executeBatch();
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.addBatch(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
