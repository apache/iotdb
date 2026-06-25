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

package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.db.query.udf.example.relational.iotdblocal.IoTDBLocalLogFunction;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link org.apache.iotdb.udf.api.IoTDBLocal} in table-model UDF, covering
 * compatibility, embedded query, logging, permission inheritance and auto resource cleanup.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBLocalIT {

  private static final String DATABASE_NAME = "iotdb_local_it";
  private static final String LIMITED_USER = "iotdb_local_user";
  private static final String LIMITED_PASSWORD = "iotdbLocalPw123456";

  private static final String PKG = "org.apache.iotdb.db.query.udf.example.relational";
  private static final String IOTDB_LOCAL_PKG =
      "org.apache.iotdb.db.query.udf.example.relational.iotdblocal";

  private static final String[] SETUP_SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE readings (device_id STRING TAG, temperature DOUBLE FIELD)",
        "CREATE TABLE device_info (device_id STRING TAG, device_name STRING FIELD)",
        "CREATE TABLE device_limits (device_id STRING TAG, max_temp DOUBLE FIELD)",
        "CREATE TABLE secret_table (device_id STRING TAG, secret STRING FIELD)",
        "CREATE TABLE vehicle (device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD)",
        "INSERT INTO device_info(time, device_id, device_name) VALUES (1, 'd1', '一号车间温度传感器'), (1, 'd2', '二号车间温度传感器')",
        "INSERT INTO device_limits(time, device_id, max_temp) VALUES (1, 'd1', 30.0), (1, 'd2', 35.0)",
        "INSERT INTO readings(time, device_id, temperature) VALUES (1000, 'd1', 25.5), (1001, 'd2', 32.0), (1002, 'd3', 20.0)",
        "INSERT INTO secret_table(time, device_id, secret) VALUES (1, 'd1', 'top-secret')",
        "INSERT INTO vehicle(time, device_id, s1, s2) VALUES (1, 'd0', 1, 1)",
        "INSERT INTO vehicle(time, device_id, s1, s2) VALUES (2, 'd0', null, 2)",
        "INSERT INTO vehicle(time, device_id, s1, s2) VALUES (3, 'd0', 3, 3)",
        "INSERT INTO vehicle(time, device_id, s1) VALUES (5, 'd0', 4)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  public static void main(String[] args) {
    for (String sql : SETUP_SQLS) {
      System.out.println(sql + ";");
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    executeAsRoot(SETUP_SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @After
  public void dropFunctions() {
    SQLFunctionUtils.dropAllUDF();
  }

  private static void executeAsRoot(String... sqls) {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("executeAsRoot failed: " + e.getMessage());
    }
  }

  // ── compatibility: legacy UDF without IoTDBLocal parameter ─────────────────

  @Test
  public void testLegacyScalarUdf() {
    SQLFunctionUtils.createUDF("contain_null", PKG + ".ContainNull");
    SQLFunctionUtils.createUDF("all_sum", PKG + ".AllSum");
    tableResultSetEqualTest(
        "SELECT time, contain_null(s1, s2) AS contain_null, contain_null(s1) AS s1_null FROM vehicle",
        new String[] {"time", "contain_null", "s1_null"},
        new String[] {
          "1970-01-01T00:00:00.001Z,false,false,",
          "1970-01-01T00:00:00.002Z,true,true,",
          "1970-01-01T00:00:00.003Z,true,false,",
          "1970-01-01T00:00:00.005Z,true,true,"
        },
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT time, all_sum(s1, s2) AS s12 FROM vehicle",
        new String[] {"time", "s12"},
        new String[] {
          "1970-01-01T00:00:00.001Z,2,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
          "5,4,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testLegacyAggregateUdf() {
    SQLFunctionUtils.createUDF("my_avg", PKG + ".MyAvg");
    tableResultSetEqualTest(
        "SELECT device_id, my_avg(s1) AS avg_s1 FROM vehicle GROUP BY device_id",
        new String[] {"device_id", "avg_s1"},
        new String[] {"d0,2.0,"},
        DATABASE_NAME);
  }

  @Test
  public void testLegacyTableFunctionUdf() {
    SQLFunctionUtils.createUDF("my_split", PKG + ".MySplit");
    SQLFunctionUtils.createUDF("my_repeat", PKG + ".MyRepeatWithoutIndex");
    tableResultSetEqualTest(
        "SELECT * FROM TABLE(my_split('a,b,c'))",
        new String[] {"output"},
        new String[] {"a,", "b,", "c,"},
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT * FROM my_repeat(vehicle, 2) ORDER BY time",
        new String[] {"time", "device_id", "s1"},
        new String[] {
          "1970-01-01T00:00:00.001Z,d0,1,",
          "1970-01-01T00:00:00.001Z,d0,1,",
          "1970-01-01T00:00:00.002Z,d0,null,",
          "1970-01-01T00:00:00.002Z,d0,null,",
          "1970-01-01T00:00:00.003Z,d0,3,",
          "1970-01-01T00:00:00.003Z,d0,3,",
          "1970-01-01T00:00:00.005Z,d0,4,",
          "1970-01-01T00:00:00.005Z,d0,4,",
        },
        DATABASE_NAME);
  }

  // ── IoTDBLocal embedded query ───────────────────────────────────────────────

  @Test
  public void testDeviceNameWithSingleQuery() {
    SQLFunctionUtils.createUDF("device_name", IOTDB_LOCAL_PKG + ".DeviceNameFunction");
    tableResultSetEqualTest(
        "SELECT time, device_id, device_name(device_id) AS name, temperature FROM readings ORDER BY time",
        new String[] {"time", "device_id", "name", "temperature"},
        new String[] {
          "1000,d1,一号车间温度传感器,25.5,", "1001,d2,二号车间温度传感器,32.0,", "1002,d3,未知设备,20.0,",
        },
        DATABASE_NAME);
  }

  @Test
  public void testDeviceSummaryWithMultipleQueries() {
    SQLFunctionUtils.createUDF("device_summary", IOTDB_LOCAL_PKG + ".DeviceSummaryFunction");
    tableResultSetEqualTest(
        "SELECT time, device_id, temperature, device_summary(device_id) AS summary FROM readings ORDER BY time",
        new String[] {"time", "device_id", "temperature", "summary"},
        new String[] {
          "1000,d1,25.5,一号车间温度传感器(上限:30.0),",
          "1001,d2,32.0,二号车间温度传感器(上限:35.0),",
          "1002,d3,20.0,未知设备(上限:未知),",
        },
        DATABASE_NAME);
  }

  @Test
  public void testDeviceSummaryWithoutManualClose() {
    SQLFunctionUtils.createUDF(
        "device_summary_no_close", IOTDB_LOCAL_PKG + ".DeviceSummaryNoCloseFunction");
    tableResultSetEqualTest(
        "SELECT device_summary_no_close(device_id) AS summary FROM readings WHERE device_id = 'd1'",
        new String[] {"summary"},
        new String[] {"一号车间温度传感器(上限:30.0),"},
        DATABASE_NAME);
    tableResultSetEqualTest(
        "SELECT device_summary_no_close(device_id) AS summary FROM readings WHERE device_id = 'd2'",
        new String[] {"summary"},
        new String[] {"二号车间温度传感器(上限:35.0),"},
        DATABASE_NAME);
  }

  // ── IoTDBLocal logging ──────────────────────────────────────────────────────

  @Test
  public void testIoTDBLocalLogApis() throws IOException {
    SQLFunctionUtils.createUDF("iotdb_local_log", IOTDB_LOCAL_PKG + ".IoTDBLocalLogFunction");
    EnvFactory.getEnv().getDataNodeWrapper(0).clearLogContent();
    tableResultSetEqualTest(
        "SELECT iotdb_local_log(device_id) AS log_ok FROM readings WHERE device_id = 'd1'",
        new String[] {"log_ok"},
        new String[] {"ok,"},
        DATABASE_NAME);
    assertLogContains(IoTDBLocalLogFunction.INFO_PLAIN);
    assertLogContains(IoTDBLocalLogFunction.INFO_FORMAT);
    assertLogContains(IoTDBLocalLogFunction.INFO_CAUSE);
    assertLogContains(IoTDBLocalLogFunction.WARN_PLAIN);
    assertLogContains(IoTDBLocalLogFunction.WARN_FORMAT);
    assertLogContains(IoTDBLocalLogFunction.WARN_CAUSE);
    assertLogContains(IoTDBLocalLogFunction.ERROR_PLAIN);
    assertLogContains(IoTDBLocalLogFunction.ERROR_FORMAT);
    assertLogContains(IoTDBLocalLogFunction.ERROR_CAUSE);
    assertLogContains("iotdb-local-it-log-cause");
  }

  private static void assertLogContains(String content) throws IOException {
    Assert.assertTrue(
        "Expected log to contain: " + content,
        EnvFactory.getEnv().getDataNodeWrapper(0).logContains(content));
  }

  // ── permission inheritance ──────────────────────────────────────────────────

  @Test
  public void testIoTDBLocalInheritsSelectPermission() {
    setupLimitedUserWithTableGrants("readings", "device_info", "device_limits");
    SQLFunctionUtils.createUDF("device_name", IOTDB_LOCAL_PKG + ".DeviceNameFunction");
    tableResultSetEqualTest(
        "SELECT device_name(device_id) AS name FROM readings WHERE device_id = 'd1'",
        new String[] {"name"},
        new String[] {"一号车间温度传感器,"},
        DATABASE_NAME,
        LIMITED_USER,
        LIMITED_PASSWORD);
    dropLimitedUser();
  }

  @Test
  public void testIoTDBLocalDeniedWithoutTablePermission() {
    setupLimitedUserWithTableGrants("readings");
    SQLFunctionUtils.createUDF("secret_query", IOTDB_LOCAL_PKG + ".SecretTableQueryFunction");
    tableAssertTestFail(
        "SELECT secret_query(device_id) FROM readings WHERE device_id = 'd1'",
        "Access Denied",
        LIMITED_USER,
        LIMITED_PASSWORD,
        DATABASE_NAME);
    dropLimitedUser();
  }

  private static void setupLimitedUserWithTableGrants(String... tables) {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DROP USER " + LIMITED_USER);
      } catch (Exception ignored) {
        // user may not exist
      }
      statement.execute("CREATE USER " + LIMITED_USER + " '" + LIMITED_PASSWORD + "'");
      for (String table : tables) {
        statement.execute(
            "GRANT SELECT ON " + DATABASE_NAME + "." + table + " TO USER " + LIMITED_USER);
      }
    } catch (Exception e) {
      fail("setupLimitedUserWithTableGrants failed: " + e.getMessage());
    }
  }

  private static void dropLimitedUser() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("DROP USER " + LIMITED_USER);
      } catch (Exception ignored) {
        // user may not exist
      }
    } catch (Exception e) {
      fail("dropLimitedUser failed: " + e.getMessage());
    }
  }
}
