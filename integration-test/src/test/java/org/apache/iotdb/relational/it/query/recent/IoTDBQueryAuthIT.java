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
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBQueryAuthIT {

  private static final String DATABASE_NAME = "test";
  private static final String CREATE_USER_FORMAT = "create user %s '%s'";
  private static final String USER_1 = "user1";
  private static final String USER_2 = "user2";
  private static final String USER_3 = "user3";
  private static final String USER_4 = "user4";
  private static final String USER_5 = "user5";
  private static final String USER_6 = "user6";
  private static final String USER_7 = "user7";
  private static final String USER_8 = "user8";
  private static final String USER_9 = "user9";
  private static final String PASSWORD = "password123456";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE table1(device_id STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO table1(time,device_id,s1) values(1, 'd1', 1)",
        "CREATE TABLE table2(device_id STRING TAG, s1 INT32 FIELD)",
        "INSERT INTO table2(time,device_id,s1) values(1, 'd1', 1)",
        String.format(CREATE_USER_FORMAT, USER_1, PASSWORD),
        "GRANT SELECT ON ANY TO USER " + USER_1,
        String.format(CREATE_USER_FORMAT, USER_2, PASSWORD),
        "GRANT SELECT ON DATABASE " + DATABASE_NAME + " TO USER " + USER_2,
        String.format(CREATE_USER_FORMAT, USER_3, PASSWORD),
        "GRANT SELECT ON TABLE table1 TO USER " + USER_3,
        String.format(CREATE_USER_FORMAT, USER_4, PASSWORD),
        "GRANT DROP ON TABLE table2 TO USER " + USER_4,
        String.format(CREATE_USER_FORMAT, USER_5, PASSWORD),
        "GRANT INSERT ON TABLE table2 TO USER " + USER_5,
        String.format(CREATE_USER_FORMAT, USER_6, PASSWORD),
        "GRANT CREATE ON TABLE table2 TO USER " + USER_6,
        String.format(CREATE_USER_FORMAT, USER_7, PASSWORD),
        "GRANT ALTER ON TABLE table2 TO USER " + USER_7,
        String.format(CREATE_USER_FORMAT, USER_8, PASSWORD),
        "GRANT DELETE ON TABLE table2 TO USER " + USER_8,
        String.format(CREATE_USER_FORMAT, USER_9, PASSWORD),
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
  public void queryAuthTest() {
    // case 1: user1 with SELECT ON ANY
    String[] expectedHeader1 = new String[] {"time", "device_id", "s1"};
    String[] retArray1 =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,",
        };
    String[] expectedHeader2 = new String[] {"device_id"};
    String[] retArray2 = new String[] {"d1,"};
    String[] expectedHeader3 = new String[] {"count(devices)"};
    String[] retArray3 = new String[] {"1,"};

    tableResultSetEqualTest(
        "select * from table1", expectedHeader1, retArray1, USER_1, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "show devices from table1", expectedHeader2, retArray2, USER_1, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "count devices from table1", expectedHeader3, retArray3, USER_1, PASSWORD, DATABASE_NAME);

    // case 2: user2 with SELECT ON database
    tableResultSetEqualTest(
        "select * from table1", expectedHeader1, retArray1, USER_2, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "show devices from table1", expectedHeader2, retArray2, USER_2, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "count devices from table1", expectedHeader3, retArray3, USER_2, PASSWORD, DATABASE_NAME);

    // case 3: user3 with SELECT ON table1
    tableResultSetEqualTest(
        "select * from table1", expectedHeader1, retArray1, USER_3, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "show devices from table1", expectedHeader2, retArray2, USER_3, PASSWORD, DATABASE_NAME);
    tableResultSetEqualTest(
        "count devices from table1", expectedHeader3, retArray3, USER_3, PASSWORD, DATABASE_NAME);

    // case 4: user3 with SELECT ON table1, without SELECT ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_3,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_3,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_3,
        PASSWORD,
        DATABASE_NAME);

    // case 5: user4 with only DROP ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_4,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_4,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_4,
        PASSWORD,
        DATABASE_NAME);

    // case 6: user5 with only INSERT ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_5,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_5,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_5,
        PASSWORD,
        DATABASE_NAME);

    // case 7: user6 with only CREATE ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_6,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_6,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_6,
        PASSWORD,
        DATABASE_NAME);

    // case 8: user7 with only ALTER ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_7,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_7,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_7,
        PASSWORD,
        DATABASE_NAME);

    // case 9: user8 with only DELETE ON table2
    tableAssertTestFail(
        "select * from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_8,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "show devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_8,
        PASSWORD,
        DATABASE_NAME);
    tableAssertTestFail(
        "count devices from table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_8,
        PASSWORD,
        DATABASE_NAME);

    // case 10: user9 with nothing
    tableAssertTestFail(
        "select * from test.table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_9,
        PASSWORD);
    tableAssertTestFail(
        "show devices from test.table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_9,
        PASSWORD);
    tableAssertTestFail(
        "count devices from test.table2",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_9,
        PASSWORD);

    // case 11: user1 with SELECT ON ANY
    String[] expectedHeader4 = new String[] {"time", "device_id", "table1_s1", "table2_s1"};
    String[] retArray4 =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,1,",
        };

    tableResultSetEqualTest(
        "select table1.time as time, table1.device_id as device_id, table1.s1 as table1_s1, table2.s1 as table2_s1 from table1 inner join table2 on table1.time=table2.time and table1.device_id=table2.device_id",
        expectedHeader4,
        retArray4,
        USER_1,
        PASSWORD,
        DATABASE_NAME);

    // case 12: user2 with SELECT ON database
    tableResultSetEqualTest(
        "select table1.time as time, table1.device_id as device_id, table1.s1 as table1_s1, table2.s1 as table2_s1 from table1 inner join table2 on table1.time=table2.time and table1.device_id=table2.device_id",
        expectedHeader4,
        retArray4,
        USER_2,
        PASSWORD,
        DATABASE_NAME);

    // case 3: user3 with SELECT ON just table1
    tableAssertTestFail(
        "select table1.time as time, table1.device_id as device_id, table1.s1 as table1_s1, table2.s1 as table2_s1 from table1 inner join table2 on table1.time=table2.time and table1.device_id=table2.device_id",
        TSStatusCode.NO_PERMISSION.getStatusCode()
            + ": Access Denied: No permissions for this operation, please add privilege SELECT ON test.table2",
        USER_3,
        PASSWORD,
        DATABASE_NAME);
  }
}
