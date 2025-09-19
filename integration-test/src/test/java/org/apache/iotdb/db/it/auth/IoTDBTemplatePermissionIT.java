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
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showPathsUsingTemplateHeaders;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.revokeUserSeriesPrivilege;

/**
 * This Class contains integration tests for SystemPermissions but {@link PrivilegeType#MANAGE_USER}
 * and {@link PrivilegeType#MANAGE_ROLE}, you can see tests of them in {@link IoTDBAuthIT}.
 */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTemplatePermissionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123123456");
    executeNonQuery("create database root.test1");
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void adminOperationsTest() {
    assertNonQueryTestFail(
        "create device template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "drop device template t1",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "alter device template t1 add (speed FLOAT encoding=RLE, FLOAT TEXT encoding=PLAIN compression=SNAPPY)",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    executeNonQuery("show device templates");
    assertNonQueryTestFail(
        "show nodes in device template t1",
        "507: Template t1 does not exist",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "set device template t1 to root.sg1",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "unset device template t1 from root.sg1",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "show paths set device template t1",
        "305: org.apache.iotdb.commons.exception.IoTDBException: Template t1 does not exist",
        "test",
        "test123123456");
  }

  @Test
  public void otherTest() {
    executeNonQuery(
        "create device template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)");
    executeNonQuery("create database root.sg1");
    executeNonQuery("set device template t1 to root.sg1.d1");

    // active
    assertNonQueryTestFail(
        "create timeseries using device template on root.sg1.d1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.temperature, root.sg1.d1.status]",
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
    executeNonQuery(
        "create timeseries using device template on root.sg1.d1", "test", "test123123456");

    // insert
    assertNonQueryTestFail(
        "insert into root.sg1.d1(time, s1) values(1,1)",
        "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg1.d1.s1]",
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_DATA, "root.sg1.**");
    executeNonQuery(
        "insert into root.sg1.d1(time, temperature) values(1,1)", "test", "test123123456");
    assertNonQueryTestFail(
        "insert into root.sg1.d1(time, s1) values(1,1)",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.SYSTEM, "root.**");
    executeNonQuery("insert into root.sg1.d1(time, s1) values(1,1)", "test", "test123123456");

    // show
    executeNonQuery("create database root.sg2");
    executeNonQuery("set device template t1 to root.sg2.d1");
    resultSetEqualTest(
        "show paths using device template t1",
        showPathsUsingTemplateHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"root.sg1.d1,"},
        "test",
        "test123123456");

    // deActive
    revokeUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
    assertNonQueryTestFail(
        "deactivate device template t1 from root.sg1.d1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg1.d1.temperature, root.sg1.d1.s1, root.sg1.d1.status]",
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.sg1.d1.**");
    executeNonQuery("deactivate device template t1 from root.sg1.d1", "test", "test123123456");
  }
}
