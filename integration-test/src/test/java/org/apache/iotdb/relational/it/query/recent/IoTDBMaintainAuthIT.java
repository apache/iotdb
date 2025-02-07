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
import static org.apache.iotdb.db.it.utils.TestUtils.tableExecuteTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableQueryNoVerifyResultTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBMaintainAuthIT {
  private static final String DATABASE_NAME = "test";
  private static final String CREATE_USER_FORMAT = "create user %s '%s'";
  private static final String USER_1 = "user1";
  private static final String USER_2 = "user2";
  private static final String PASSWORD = "password";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO table1(time,device_id,s1) values(1, 'd1', 1)",
        String.format(CREATE_USER_FORMAT, USER_1, PASSWORD),
        "GRANT MAINTAIN TO USER " + USER_1,
        "GRANT SELECT ON TABLE table1 TO USER " + USER_1,
        "GRANT SELECT ON information_schema.queries TO USER " + USER_1,
        String.format(CREATE_USER_FORMAT, USER_2, PASSWORD)
      };

  @BeforeClass
  public static void setUp() throws Exception {
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
        "explain select * from table1", expectedHeader, USER_1, PASSWORD, DATABASE_NAME);
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
        "explain analyze select * from table1", expectedHeader, USER_1, PASSWORD, DATABASE_NAME);
    tableQueryNoVerifyResultTest(
        "explain analyze verbose select * from table1",
        expectedHeader,
        USER_1,
        PASSWORD,
        DATABASE_NAME);
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
    tableQueryNoVerifyResultTest(
        "SHOW CURRENT_SQL_DIALECT", expectedHeader, USER_2, PASSWORD, DATABASE_NAME);

    // case 4: show current_user
    expectedHeader = new String[] {"CurrentUser"};
    tableQueryNoVerifyResultTest(
        "SHOW CURRENT_USER", expectedHeader, USER_2, PASSWORD, DATABASE_NAME);

    // case 5: show version
    expectedHeader = new String[] {"Version", "BuildInfo"};
    tableQueryNoVerifyResultTest("SHOW VERSION", expectedHeader, USER_2, PASSWORD, DATABASE_NAME);

    // case 6: show current_timestamp
    expectedHeader = new String[] {"CurrentTimestamp"};
    tableQueryNoVerifyResultTest(
        "SHOW CURRENT_TIMESTAMP", expectedHeader, USER_2, PASSWORD, DATABASE_NAME);

    // case 7: show variables
    expectedHeader = new String[] {"Variable", "Value"};
    // user1 with MAINTAIN
    tableQueryNoVerifyResultTest("SHOW VARIABLES", expectedHeader, USER_1, PASSWORD, DATABASE_NAME);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "SHOW VARIABLES",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 8: show cluster_id
    expectedHeader = new String[] {"ClusterId"};
    // user1 with MAINTAIN
    tableQueryNoVerifyResultTest(
        "SHOW CLUSTER_ID", expectedHeader, USER_1, PASSWORD, DATABASE_NAME);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "SHOW CLUSTER_ID",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 9: flush
    // user1 with MAINTAIN
    tableExecuteTest("FLUSH", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "FLUSH",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 10: clear cache
    // user1 with MAINTAIN
    tableExecuteTest("CLEAR CACHE", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "CLEAR CACHE",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 11: set configuration
    // user1 with MAINTAIN
    tableExecuteTest("SET CONFIGURATION query_timeout_threshold='100000'", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "SET CONFIGURATION query_timeout_threshold='100000'",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 12: show queries
    // user1 with select on information_schema.queries
    expectedHeader =
        new String[] {"query_id", "start_time", "datanode_id", "elapsed_time", "statement"};
    tableQueryNoVerifyResultTest("SHOW QUERIES", expectedHeader, USER_1, PASSWORD, DATABASE_NAME);
    // user2 without select on information_schema.queries
    tableAssertTestFail(
        "SHOW QUERIES",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON information_schema.queries",
        USER_2,
        PASSWORD);

    // case 13: kill query
    // user1 with MAINTAIN
    tableAssertTestFail(
        "kill query '20250206_093300_00001_100'",
        TSStatusCode.NO_SUCH_QUERY.getStatusCode() + ": No such query",
        USER_1,
        PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "kill query '20250206_093300_00001_100'",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 14: load configuration
    // user1 with MAINTAIN
    tableExecuteTest("LOAD CONFIGURATION", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "LOAD CONFIGURATION",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 15: set system status
    // user1 with MAINTAIN
    tableExecuteTest("SET SYSTEM TO RUNNING", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "SET SYSTEM TO RUNNING",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 16: start repair data
    // user1 with MAINTAIN
    tableExecuteTest("START REPAIR DATA", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "START REPAIR DATA",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);

    // case 17: stop repair data
    // user1 with MAINTAIN
    tableExecuteTest("STOP REPAIR DATA", USER_1, PASSWORD);
    // user2 without MAINTAIN
    tableAssertTestFail(
        "STOP REPAIR DATA",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege MAINTAIN",
        USER_2,
        PASSWORD);
  }
}
