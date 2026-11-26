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

import org.apache.iotdb.db.query.udf.example.relational.iotdblocal.IoTDBLocalLogHelper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
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

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

/**
 * Verifies {@link org.apache.iotdb.udf.api.IoTDBLocal} logging APIs on DataNode log output. Runs
 * only in 1C1D ({@link TableLocalStandaloneIT}) because log inspection uses {@code
 * DataNodeWrapper(0)}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class})
public class IoTDBLocalLogIT {

  private static final String DATABASE_NAME = "iotdb_local_log_it";
  private static final String IOTDB_LOCAL_PKG =
      "org.apache.iotdb.db.query.udf.example.relational.iotdblocal";

  private static DataNodeWrapper dataNodeWrapper;

  private static final String[] SETUP_SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE readings (device_id STRING TAG, temperature DOUBLE FIELD)",
        "CREATE TABLE vehicle (device_id STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO readings(time, device_id, temperature) VALUES (1000, 'd1', 25.5)",
        "INSERT INTO vehicle(time, device_id, s1) VALUES (1, 'd0', 1), (2, 'd0', 2), (3, 'd0', 3)",
        "FLUSH",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
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

  @Test
  public void testScalarFunctionLogApis() throws IOException {
    SQLFunctionUtils.createUDF(
        "iotdb_local_log_scalar", IOTDB_LOCAL_PKG + ".IoTDBLocalLogFunction");
    dataNodeWrapper.clearLogContent();
    tableResultSetEqualTest(
        "SELECT iotdb_local_log_scalar(device_id) AS log_ok FROM readings WHERE device_id = 'd1'",
        new String[] {"log_ok"},
        new String[] {"ok,"},
        DATABASE_NAME);
    assertPhaseLogs(IoTDBLocalLogHelper.SCALAR_BEFORE_START);
    assertPhaseLogs(IoTDBLocalLogHelper.SCALAR_EVALUATE);
    assertPhaseLogs(IoTDBLocalLogHelper.SCALAR_BEFORE_DESTROY);
  }

  @Test
  public void testAggregateFunctionLogApis() throws IOException {
    SQLFunctionUtils.createUDF(
        "iotdb_local_log_udaf", IOTDB_LOCAL_PKG + ".IoTDBLocalLogAggregateFunction");
    dataNodeWrapper.clearLogContent();
    tableResultSetEqualTest(
        "SELECT iotdb_local_log_udaf(s1) AS row_count FROM vehicle",
        new String[] {"row_count"},
        new String[] {"3,"},
        DATABASE_NAME);
    assertPhaseLogs(IoTDBLocalLogHelper.UDAF_BEFORE_START);
    assertPhaseLogs(IoTDBLocalLogHelper.UDAF_ADD_INPUT);
    assertPhaseLogs(IoTDBLocalLogHelper.UDAF_OUTPUT_FINAL);
    assertPhaseLogs(IoTDBLocalLogHelper.UDAF_BEFORE_DESTROY);
  }

  @Test
  public void testTableFunctionLogApis() throws IOException {
    SQLFunctionUtils.createUDF(
        "iotdb_local_log_split", IOTDB_LOCAL_PKG + ".IoTDBLocalLogTableFunction");
    dataNodeWrapper.clearLogContent();
    tableResultSetEqualTest(
        "SELECT * FROM iotdb_local_log_split('a,b')",
        new String[] {"output"},
        new String[] {"a,", "b,"},
        DATABASE_NAME);
    assertPhaseLogs(IoTDBLocalLogHelper.TVF_BEFORE_START);
    assertPhaseLogs(IoTDBLocalLogHelper.TVF_PROCESS);
    assertPhaseLogs(IoTDBLocalLogHelper.TVF_BEFORE_DESTROY);
  }

  private static void assertPhaseLogs(String phase) throws IOException {
    assertLogContains(IoTDBLocalLogHelper.infoPlain(phase));
    assertLogContains(IoTDBLocalLogHelper.infoFormat(phase));
    assertLogContains(IoTDBLocalLogHelper.infoCause(phase));
    assertLogContains(IoTDBLocalLogHelper.warnPlain(phase));
    assertLogContains(IoTDBLocalLogHelper.warnFormat(phase));
    assertLogContains(IoTDBLocalLogHelper.warnCause(phase));
    assertLogContains(IoTDBLocalLogHelper.errorPlain(phase));
    assertLogContains(IoTDBLocalLogHelper.errorFormat(phase));
    assertLogContains(IoTDBLocalLogHelper.errorCause(phase));
    assertLogContains(IoTDBLocalLogHelper.CAUSE_MESSAGE);
  }

  private static void assertLogContains(String content) throws IOException {
    Assert.assertTrue("Expected log to contain: " + content, dataNodeWrapper.logContains(content));
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
}
