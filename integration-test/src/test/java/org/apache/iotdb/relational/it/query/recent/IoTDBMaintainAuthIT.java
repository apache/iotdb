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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableQueryNoVerifyResultTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBMaintainAuthIT {
  private static final String DATABASE_NAME = "test";
  private static final String CREATE_USER_FORMAT = "create user %s '%s'";
  private static final String USER_1 = "user1";
  private static final String USER_2 = "user2";
  private static final String PASSWORD = "password123456";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO table1(time,device_id,s1) values(1, 'd1', 1)",
        String.format(CREATE_USER_FORMAT, USER_1, PASSWORD),
        "GRANT SELECT ON TABLE table1 TO USER " + USER_1,
        String.format(CREATE_USER_FORMAT, USER_2, PASSWORD)
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void maintainAuthTest() {
    // case 1: explain
    // user1 with select on table1
    String[] expectedHeader = new String[] {"distribution plan"};
    tableQueryNoVerifyResultTest(
        "explain select * from test.table1", expectedHeader, USER_1, PASSWORD);
    // user2 without select on table1
    tableAssertTestFail(
        "explain select * from test.table1",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table1",
        USER_2,
        PASSWORD);

    // case 2: explain analyze [verbose]
    // user1 with select on table1
    expectedHeader = new String[] {"Explain Analyze"};
    tableQueryNoVerifyResultTest(
        "explain analyze select * from test.table1", expectedHeader, USER_1, PASSWORD);
    tableQueryNoVerifyResultTest(
        "explain analyze verbose select * from test.table1", expectedHeader, USER_1, PASSWORD);
    // user2 without select on table1
    tableAssertTestFail(
        "explain analyze select * from test.table1",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table1",
        USER_2,
        PASSWORD);
    tableAssertTestFail(
        "explain analyze verbose select * from test.table1",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table1",
        USER_2,
        PASSWORD);

    // case 3: show current_sql_dialect
    expectedHeader = new String[] {"CurrentSqlDialect"};
    tableQueryNoVerifyResultTest("SHOW CURRENT_SQL_DIALECT", expectedHeader, USER_2, PASSWORD);

    // case 4: show current_user
    expectedHeader = new String[] {"CurrentUser"};
    tableQueryNoVerifyResultTest("SHOW CURRENT_USER", expectedHeader, USER_2, PASSWORD);

    // case 5: show version
    expectedHeader = new String[] {"Version", "BuildInfo"};
    tableQueryNoVerifyResultTest("SHOW VERSION", expectedHeader, USER_2, PASSWORD);

    // case 6: show current_timestamp
    expectedHeader = new String[] {"CurrentTimestamp"};
    tableQueryNoVerifyResultTest("SHOW CURRENT_TIMESTAMP", expectedHeader, USER_2, PASSWORD);

    // case 7: show variables
    // user2
    tableAssertTestFail(
        "SHOW VARIABLES",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 8: show cluster_id
    // user2
    tableAssertTestFail(
        "SHOW CLUSTER_ID",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 9: flush
    // user2
    tableAssertTestFail(
        "FLUSH",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 10: clear cache
    // user2
    tableAssertTestFail(
        "CLEAR CACHE",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 11: set configuration
    // user2
    tableAssertTestFail(
        "SET CONFIGURATION query_timeout_threshold='100000'",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 12: show queries
    // non-root users can access its own queries
    expectedHeader =
        new String[] {"query_id", "start_time", "datanode_id", "elapsed_time", "statement", "user"};
    tableQueryNoVerifyResultTest("show queries", expectedHeader, USER_2, PASSWORD);

    // case 13: kill query
    // user2
    tableAssertTestFail(
        "kill query '20250206_093300_00001_1'",
        TSStatusCode.NO_SUCH_QUERY.getStatusCode() + ": No such query",
        USER_2,
        PASSWORD);

    // case 14: load configuration
    // user2
    tableAssertTestFail(
        "LOAD CONFIGURATION",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, only root user is allowed",
        USER_2,
        PASSWORD);

    // case 15: set system status
    // user2
    tableAssertTestFail(
        "SET SYSTEM TO RUNNING",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 16: start repair data
    // user2
    tableAssertTestFail(
        "START REPAIR DATA",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 17: stop repair data
    // user2
    tableAssertTestFail(
        "STOP REPAIR DATA",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 18: create function
    // user1
    tableAssertTestFail(
        "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_1,
        PASSWORD);
    // user2
    tableAssertTestFail(
        "create function udsf as 'org.apache.iotdb.db.query.udf.example.relational.ContainNull'",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);

    // case 19: show functions
    // user1
    expectedHeader = new String[] {"FunctionName", "FunctionType", "ClassName(UDF)", "State"};
    tableQueryNoVerifyResultTest("SHOW FUNCTIONS", expectedHeader, USER_1, PASSWORD);
    // user2
    tableQueryNoVerifyResultTest("SHOW FUNCTIONS", expectedHeader, USER_2, PASSWORD);

    // case 20: create function
    // user1
    tableAssertTestFail(
        "drop function udsf",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_1,
        PASSWORD);
    // user2
    tableAssertTestFail(
        "drop function udsf",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SYSTEM",
        USER_2,
        PASSWORD);
  }
}
