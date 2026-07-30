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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.executeTableNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.executeTableQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSystemPermissionRelationalIT {

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
    createUser("test8", "test123123456");
    executeNonQuery("create database root.test1");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void maintainOperationsTest() {
    executeTableQuery("show queries", "test6", "test123123456");
    executeTableNonQuery("show version", "test6", "test123123456");
    assertTableNonQueryTestFail(
        "kill query '20250918_015728_00003_1'", "714: No such query", "test6", "test123123456");
    assertTableNonQueriesTestFail(
        new String[] {
          "show variables",
          "flush",
          "clear cache",
          "set system to readonly",
          "set system to running",
          "set configuration enable_seq_space_compaction='true'",
          "start repair data",
          "stop repair data",
        },
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test6",
        "test123123456");
    grantUserSystemPrivileges("test6", PrivilegeType.SYSTEM);
    executeTableNonQuery("flush", "test6", "test123123456");
    executeTableNonQuery("clear cache", "test6", "test123123456");
    executeTableNonQuery("set system to readonly", "test6", "test123123456");
    executeTableNonQuery("set system to running", "test6", "test123123456");
    executeTableNonQuery(
        "set configuration enable_seq_space_compaction='true'", "test6", "test123123456");
    executeTableNonQuery("start repair data", "test6", "test123123456");
    executeTableNonQuery("stop repair data", "test6", "test123123456");
    executeTableQuery("show queries", "test6", "test123123456");
  }

  @Test
  public void clusterManagemantOperationsTest() {
    assertTableNonQueriesTestFail(
        new String[] {
          "show confignodes",
          "show datanodes",
          "show ainodes",
          "show regions",
          "remove datanode 1",
          "remove confignode 0",
          "reconstruct region 0 on 1",
          "extend region 0 to 1",
          "migrate region 1 from 1 to 2",
          "remove region 1 from 1",
          "show cluster",
          "show cluster details",
        },
        "803: Access Denied: No permissions for this operation, please add privilege SYSTEM",
        "test7",
        "test123123456");
    assertTableNonQueryTestFail(
        "load configuration",
        "803: Access Denied: No permissions for this operation, only root user is allowed",
        "test7",
        "test123123456");
    grantUserSystemPrivileges("test7", PrivilegeType.SYSTEM);

    executeTableQuery("show confignodes", "test7", "test123123456");
    executeTableQuery("show datanodes", "test7", "test123123456");
    executeTableQuery("show ainodes", "test7", "test123123456");
    executeTableQuery("show regions", "test7", "test123123456");
    executeTableQuery("show cluster", "test7", "test123123456");
    executeTableQuery("show cluster details", "test7", "test123123456");
    assertTableNonQueryTestFail(
        "load configuration",
        "803: Access Denied: No permissions for this operation, only root user is allowed",
        "test7",
        "test123123456");
  }

  private void assertTableNonQueriesTestFail(
      String[] sqls, String msg, String username, String password) {
    for (String sql : sqls) {
      assertTableNonQueryTestFail(sql, msg, username, password);
    }
  }
}
