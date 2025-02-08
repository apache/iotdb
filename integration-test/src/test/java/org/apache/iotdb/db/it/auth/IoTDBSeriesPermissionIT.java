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

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countDevicesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countNodesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countTimeSeriesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showChildNodesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showChildPathsColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDevicesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showStorageGroupsColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTTLColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTimeSeriesColumnHeaders;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSeriesPermissionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123");
    createUser("test1", "test123");
  }

  @After
  public void tearDown() throws Exception {
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
    executeNonQuery("set TTL to root.test 10000");

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
        new String[] {
          "root.test.d1.s1,null,root.test,INT32,TS_2DIFF,LZ4,null,null,null,null,BASE,"
        },
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
        new String[] {"root.test,1,1,0,604800000,"},
        "test1",
        "test123");
    resultSetEqualTest(
        "count databases", new String[] {"count"}, new String[] {"1,"}, "test1", "test123");

    // show/count devices
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,null,10000,"},
        "test1",
        "test123");
    grantUserSeriesPrivilege("test1", PrivilegeType.READ_SCHEMA, "root.test1.d1.**");
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,null,10000,", "root.test1.d1,false,null,INF,"},
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
        "show all ttl",
        showTTLColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.**,INF,", "root.test,10000,", "root.test.**,10000,"},
        "test1",
        "test123");
  }

  @Test
  public void testData() {
    testWriteData();

    testReadData();
  }

  private void testWriteData() {
    grantUserSeriesPrivilege("test1", PrivilegeType.WRITE_DATA, "root.sg.d1.s1");
    assertNonQueryTestFail(
        "insert into root.sg.d1(time,s1,s2) values(1,1,1)",
        "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg.d1.s2]",
        "test1",
        "test123");
    assertNonQueryTestFail(
        "delete from root.sg.d1.s1, root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg.d1.s2]",
        "test1",
        "test123");
    grantUserSeriesPrivilege("test1", PrivilegeType.WRITE_DATA, "root.sg.d1.s2");
    assertNonQueryTestFail(
        "insert into root.sg.d1(time,s1,s2) values(1,1,1)",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.d1.s1, root.sg.d1.s2]",
        "test1",
        "test123");
    executeNonQuery("delete from root.sg.d1.s1, root.sg.d1.s2", "test1", "test123");
    grantUserSeriesPrivilege("test1", PrivilegeType.WRITE_SCHEMA, "root.sg.d1.**");
    assertNonQueryTestFail(
        "insert into root.sg.d1(time,s1,s2) values(1,1,1)",
        "803: No permissions for this operation, please add privilege MANAGE_DATABASE",
        "test1",
        "test123");
    grantUserSystemPrivileges("test1", PrivilegeType.MANAGE_DATABASE);
    executeNonQuery("insert into root.sg.d1(time,s1,s2) values(1,1,1)", "test1", "test123");
  }

  private void testReadData() {
    executeNonQuery("insert into  root.test.d1(time,s1) values(1,1)");
    executeNonQuery("insert into root.test1.d1(time,s1) values(2,2)");
    executeNonQuery("insert into root.test1.d2(time,s1) values(2,2)");

    resultSetEqualTest(
        "select * from root.**", new String[] {TIME}, new String[] {}, "test", "test123");
    grantUserSeriesPrivilege("test", PrivilegeType.READ_DATA, "root.test.**");
    resultSetEqualTest(
        "select * from root.**",
        new String[] {TIME, "root.test.d1.s1"},
        new String[] {"1,1.0,"},
        "test",
        "test123");
    grantUserSeriesPrivilege("test", PrivilegeType.READ_DATA, "root.test1.d1.**");
    resultSetEqualTest(
        "select * from root.**",
        new String[] {TIME, "root.test.d1.s1", "root.test1.d1.s1"},
        new String[] {"1,1.0,null,", "2,null,2.0,"},
        "test",
        "test123");
  }
}
