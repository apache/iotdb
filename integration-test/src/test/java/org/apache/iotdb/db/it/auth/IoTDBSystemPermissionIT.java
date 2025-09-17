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

package org.apache.iotdb.db.it.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.trigger.IoTDBTriggerManagementIT.TRIGGER_FILE_TIMES_COUNTER;
import static org.apache.iotdb.db.it.trigger.IoTDBTriggerManagementIT.TRIGGER_JAR_PREFIX;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.executeQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;

/**
 * This Class contains integration tests for SystemPermissions but {@link PrivilegeType#MANAGE_USER}
 * and {@link PrivilegeType#MANAGE_ROLE}, you can see tests of them in {@link IoTDBAuthIT}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSystemPermissionIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test1", "test123123456");
    createUser("test2", "test123123456");
    createUser("test3", "test123123456");
    createUser("test4", "test123123456");
    createUser("test5", "test123123456");
    createUser("test6", "test123123456");
    createUser("test7", "test123123456");
    executeNonQuery("create database root.test1");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showVersionTest() {
    assertNonQueryTestFail(
        "show version",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test4",
        "test123123456");
    grantUserSystemPrivileges("test4", PrivilegeType.SYSTEM);
    executeQuery("show version", "test4", "test123123456");
  }

  @Test
  public void manageDataBaseTest() {
    assertNonQueryTestFail(
        "create database root.test2",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test1",
        "test123123456");
    assertNonQueryTestFail(
        "drop database root.test1",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test1",
        "test123123456");

    grantUserSystemPrivileges("test1", PrivilegeType.SYSTEM);

    executeNonQuery("create database root.test2", "test1", "test123123456");
    executeNonQuery("drop database root.test1, root.test2", "test1", "test123123456");
  }

  @Test
  public void manageTriggerTest() {
    assertNonQueryTestFail(
        String.format(
            "create stateless trigger testTrigger before insert on root.test.stateless.* as '%s' using URI '%s'",
            TRIGGER_FILE_TIMES_COUNTER, TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar"),
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test2",
        "test123123456");
    assertNonQueryTestFail(
        "drop trigger testTrigger",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test2",
        "test123123456");
    assertTestFail(
        "show triggers",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test2",
        "test123123456");

    grantUserSystemPrivileges("test2", PrivilegeType.SYSTEM);

    executeNonQuery(
        String.format(
            "create stateless trigger testTrigger before insert on root.test.stateless.* as '%s' using URI '%s'",
            TRIGGER_FILE_TIMES_COUNTER, TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar"),
        "test2",
        "test123123456");
    executeNonQuery("drop trigger testTrigger", "test2", "test123123456");
    executeQuery("show triggers", "test2", "test123123456");
  }

  @Test
  public void manageUdfTest() {
    assertNonQueryTestFail(
        "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test3",
        "test123123456");
    assertNonQueryTestFail(
        "drop function counter",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test3",
        "test123123456");
    assertTestFail(
        "show functions",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test3",
        "test123123456");

    grantUserSystemPrivileges("test3", PrivilegeType.SYSTEM);

    executeNonQuery(
        "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'",
        "test3",
        "test123123456");
    executeNonQuery("drop function counter", "test3", "test123123456");
    executeQuery("show functions", "test3", "test123123456");
  }

  @Test
  public void manageCQTest() {
    assertNonQueryTestFail(
        "CREATE CQ testCQ\n"
            + "RESAMPLE RANGE 30m, 0m\n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test5",
        "test123123456");
    assertNonQueryTestFail(
        "drop CQ testCQ",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test5",
        "test123123456");
    assertTestFail(
        "show CQs",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test5",
        "test123123456");

    grantUserSystemPrivileges("test5", PrivilegeType.SYSTEM);

    executeNonQuery(
        "CREATE CQ testCQ\n"
            + "RESAMPLE RANGE 30m, 0m\n"
            + "TIMEOUT POLICY DISCARD\n"
            + "BEGIN \n"
            + "  SELECT count(s1)  \n"
            + "    INTO root.sg_count.d(count_s1)\n"
            + "    FROM root.sg.d\n"
            + "    GROUP BY(10m)\n"
            + "END",
        "test5",
        "test123123456");
    executeNonQuery("drop CQ testCQ", "test5", "test123123456");
    executeQuery("show CQs", "test5", "test123123456");
  }

  // We test pipe permission in IoTDBPipeLifeCycleIT because a fake or self receiver
  // will surely lead to premature failure

  @Test
  public void maintainOperationsTest() {
    assertNonQueryTestFail(
        "show queries",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test6",
        "test123123456");
    assertNonQueryTestFail(
        "kill query 'test'",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test6",
        "test123123456");
    assertNonQueryTestFail(
        "show cluster",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test6",
        "test123123456");
    assertNonQueryTestFail(
        "show cluster details",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test6",
        "test123123456");

    grantUserSystemPrivileges("test6", PrivilegeType.SYSTEM);

    executeNonQuery("show queries", "test6", "test123123456");
    assertNonQueryTestFail(
        "kill query 'test'",
        "701: Please ensure your input <queryId> is correct",
        "test6",
        "test123123456");
    executeNonQuery("show cluster", "test6", "test123123456");
    executeNonQuery("show cluster details", "test6", "test123123456");
  }

  @Test
  public void adminOperationsTest() {
    assertNonQueryTestFail(
        "flush",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
    assertNonQueryTestFail(
        "clear cache",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
    assertNonQueryTestFail(
        "set system to readonly",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
    assertNonQueryTestFail(
        "set system to running",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
    assertNonQueryTestFail(
        "load configuration",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
  }
}
