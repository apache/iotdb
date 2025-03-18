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
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123");
    executeNonQuery("create database root.test1");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showVersionTest() {
    executeQuery("show version", "test", "test123");
  }

  @Test
  public void manageDataBaseTest() {
    assertNonQueryTestFail(
        "create database root.test2",
        "803: No permissions for this operation, please add privilege MANAGE_DATABASE",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop database root.test1",
        "803: No permissions for this operation, please add privilege MANAGE_DATABASE",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.MANAGE_DATABASE);

    executeNonQuery("create database root.test2", "test", "test123");
    executeNonQuery("drop database root.test1, root.test2", "test", "test123");
  }

  @Test
  public void manageTriggerTest() {
    assertNonQueryTestFail(
        String.format(
            "create stateless trigger testTrigger before insert on root.test.stateless.* as '%s' using URI '%s'",
            TRIGGER_FILE_TIMES_COUNTER, TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar"),
        "803: No permissions for this operation, please add privilege USE_TRIGGER",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop trigger testTrigger",
        "803: No permissions for this operation, please add privilege USE_TRIGGER",
        "test",
        "test123");
    assertTestFail(
        "show triggers",
        "803: No permissions for this operation, please add privilege USE_TRIGGER",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.USE_TRIGGER);

    executeNonQuery(
        String.format(
            "create stateless trigger testTrigger before insert on root.test.stateless.* as '%s' using URI '%s'",
            TRIGGER_FILE_TIMES_COUNTER, TRIGGER_JAR_PREFIX + "TriggerFireTimesCounter.jar"),
        "test",
        "test123");
    executeNonQuery("drop trigger testTrigger", "test", "test123");
    executeQuery("show triggers", "test", "test123");
  }

  @Test
  public void manageUdfTest() {
    assertNonQueryTestFail(
        "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'",
        "803: No permissions for this operation, please add privilege USE_UDF",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop function counter",
        "803: No permissions for this operation, please add privilege USE_UDF",
        "test",
        "test123");
    assertTestFail(
        "show functions",
        "803: No permissions for this operation, please add privilege USE_UDF",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.USE_UDF);

    executeNonQuery(
        "create function counter as 'org.apache.iotdb.db.query.udf.example.Counter'",
        "test",
        "test123");
    executeNonQuery("drop function counter", "test", "test123");
    executeQuery("show functions", "test", "test123");
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
        "803: No permissions for this operation, please add privilege USE_CQ",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop CQ testCQ",
        "803: No permissions for this operation, please add privilege USE_CQ",
        "test",
        "test123");
    assertTestFail(
        "show CQs",
        "803: No permissions for this operation, please add privilege USE_CQ",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.USE_CQ);

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
        "test",
        "test123");
    executeNonQuery("drop CQ testCQ", "test", "test123");
    executeQuery("show CQs", "test", "test123");
  }

  // We test pipe permission in IoTDBPipeLifeCycleIT because a fake or self receiver
  // will surely lead to premature failure

  @Test
  public void maintainOperationsTest() {
    assertNonQueryTestFail(
        "show queries",
        "803: No permissions for this operation, please add privilege MAINTAIN",
        "test",
        "test123");
    assertNonQueryTestFail(
        "kill query 'test'",
        "803: No permissions for this operation, please add privilege MAINTAIN",
        "test",
        "test123");
    assertNonQueryTestFail(
        "show cluster",
        "803: No permissions for this operation, please add privilege MAINTAIN",
        "test",
        "test123");
    assertNonQueryTestFail(
        "show cluster details",
        "803: No permissions for this operation, please add privilege MAINTAIN",
        "test",
        "test123");

    grantUserSystemPrivileges("test", PrivilegeType.MAINTAIN);

    executeNonQuery("show queries", "test", "test123");
    assertNonQueryTestFail(
        "kill query 'test'",
        "701: Please ensure your input <queryId> is correct",
        "test",
        "test123");
    executeNonQuery("show cluster", "test", "test123");
    executeNonQuery("show cluster details", "test", "test123");
  }

  @Test
  public void adminOperationsTest() {
    assertNonQueryTestFail(
        "flush", "803: Only the admin user can perform this operation", "test", "test123");
    assertNonQueryTestFail(
        "clear cache", "803: Only the admin user can perform this operation", "test", "test123");
    assertNonQueryTestFail(
        "set system to readonly",
        "803: Only the admin user can perform this operation",
        "test",
        "test123");
    assertNonQueryTestFail(
        "set system to running",
        "803: Only the admin user can perform this operation",
        "test",
        "test123");
    assertNonQueryTestFail(
        "load configuration",
        "803: Only the admin user can perform this operation",
        "test",
        "test123");
  }
}
