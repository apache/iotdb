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
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.countDevicesColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.countNodesColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.countTimeSeriesColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showChildNodesColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showChildPathsColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showDevicesColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showStorageGroupsColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showTTLColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showTimeSeriesColumnHeaders;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSeriesPermissionIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123");
    createUser("test1", "test123");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSchema() {
    testWriteSchema();

    testReadSchema();
  }

  private void testWriteSchema() {
    assertNonQueryTestFail(
        "create timeseries root.test.d1.s1 with dataType = int32",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123");
    assertNonQueryTestFail(
        "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop timeseries root.test.d1.s1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.d1.s1]",
        "test",
        "test123");
    assertNonQueryTestFail(
        "drop timeseries root.test.d1.s1, root.test.**",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.d1.s1, root.test.**]",
        "test",
        "test123");
    assertNonQueryTestFail(
        "set TTL to root.test.** 10000",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123");
    assertNonQueryTestFail(
        "unset TTL to root.test.**",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123");

    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.test.**");

    assertNonQueryTestFail(
        "create timeseries root.test.d1.s1 with dataType = int32",
        "803: No permissions for this operation, please add privilege MANAGE_DATABASE",
        "test",
        "test123");

    grantUserSeriesPrivilege("test", PrivilegeType.MANAGE_DATABASE, "root.**");

    executeNonQuery("create timeseries root.test.d1.s1 with dataType = int32", "test", "test123");
    executeNonQuery("ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3", "test", "test123");
    executeNonQuery("drop timeseries root.test.d1.s1", "test", "test123");
    executeNonQuery("set TTL to root.test.** 10000", "test", "test123");
    executeNonQuery("unset TTL to root.test.**", "test", "test123");
  }

  private void testReadSchema() {
    executeNonQuery("create timeseries root.test.d1.s1 with dataType = int32");
    executeNonQuery("create timeseries root.test1.d1.s1 with dataType = int32");
    executeNonQuery("create timeseries root.test1.d2.s1 with dataType = int32");
    executeNonQuery("create timeseries root.test3.d1.s1 with dataType = int32");
    executeNonQuery("set TTL to root.** 10000");

    // show/count timeseries
    resultSetEqualTest(
        "show timeseries",
        showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {},
        "test1",
        "test123");
    grantUserSeriesPrivilege("test1", PrivilegeType.READ_SCHEMA, "root.test.**");
    resultSetEqualTest(
        "show timeseries",
        showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"root.test.d1.s1,null,root.test,INT32,RLE,LZ4,null,null,null,null,BASE,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count timeseries",
        countTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"1,"},
        "test1",
        "test123");

    // show/count databases
    resultSetEqualTest(
        "show databases",
        showStorageGroupsColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"root.test,10000,1,1,604800000,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count databases", new String[] {"count"}, new String[] {"1,"}, "test1", "test123");

    // show/count devices
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,"},
        "test1",
        "test123");
    grantUserSeriesPrivilege("test1", PrivilegeType.READ_SCHEMA, "root.test1.d1.**");
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,", "root.test1.d1,false,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count devices",
        countDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count devices root.test1.**",
        countDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"1,"},
        "test1",
        "test123");

    // show child paths
    resultSetEqualTest(
        "show child paths",
        showChildPathsColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {
          "root.test.d1,DEVICE,",
          "root.test.d1.s1,TIMESERIES,",
          "root.test1.d1,DEVICE,",
          "root.test1.d1.s1,TIMESERIES,"
        },
        "test1",
        "test123");

    // show child nodes
    resultSetEqualTest(
        "show child nodes root",
        showChildNodesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"test,", "test1,"},
        "test1",
        "test123");

    // count nodes level
    resultSetEqualTest(
        "count nodes root.** level=1",
        countNodesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count nodes root.** level=2",
        countNodesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123");

    // TTL
    resultSetEqualTest(
        "show ttl on root.**",
        showTTLColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test1,10000,", "root.test,10000,"},
        "test1",
        "test123");
  }

  @Test
  public void testData() {
    // TODO
  }
}
