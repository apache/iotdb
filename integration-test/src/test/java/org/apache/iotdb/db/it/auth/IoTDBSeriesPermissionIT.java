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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.TIME;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countDevicesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countNodesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.countTimeSeriesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showChildNodesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showChildPathsColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDatabasesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDevicesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTTLColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTimeSeriesColumnHeaders;
import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.executeNonQuery;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSeriesPermissionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    createUser("test", "test123123456");
    createUser("test1", "test123123456");
    createUser("test2", "test123123456");
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
        "test123123456");
    assertNonQueryTestFail(
        "ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "drop timeseries root.test.d1.s1",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.d1.s1]",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "drop timeseries root.test.d1.s1, root.test.**",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.d1.s1, root.test.**]",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "set TTL to root.test.** 10000",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123123456");
    assertNonQueryTestFail(
        "unset TTL to root.test.**",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA",
        "test",
        "test123123456");

    grantUserSeriesPrivilege("test", PrivilegeType.WRITE_SCHEMA, "root.test.**");

    assertNonQueryTestFail(
        "create timeseries root.test.d1.s1 with dataType = int32",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test",
        "test123123456");

    grantUserSeriesPrivilege("test", PrivilegeType.SYSTEM, "root.**");

    executeNonQuery(
        "create timeseries root.test.d1.s1 with dataType = int32", "test", "test123123456");
    executeNonQuery("ALTER timeseries root.test.d1.s1 ADD TAGS tag3=v3", "test", "test123123456");
    executeNonQuery("drop timeseries root.test.d1.s1", "test", "test123123456");
    executeNonQuery("set TTL to root.test.** 10000", "test", "test123123456");
    executeNonQuery("unset TTL to root.test.**", "test", "test123123456");
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
        "test123123456");
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
        "test123123456");
    resultSetEqualTest(
        "count timeseries root.test.**",
        countTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"1,"},
        "test1",
        "test123123456");

    // show/count databases
    resultSetEqualTest(
        "show databases",
        showDatabasesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test,1,1,0,604800000,"},
        "test1",
        "test123123456");
    resultSetEqualTest(
        "count databases", new String[] {"count"}, new String[] {"1,"}, "test1", "test123123456");

    // show/count devices
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,null,10000,"},
        "test1",
        "test123123456");
    grantUserSeriesPrivilege("test1", PrivilegeType.READ_SCHEMA, "root.test1.d1.**");
    resultSetEqualTest(
        "show devices",
        showDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test.d1,false,null,10000,", "root.test1.d1,false,null,INF,"},
        "test1",
        "test123123456");
    resultSetEqualTest(
        "count devices",
        countDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123123456");
    resultSetEqualTest(
        "count devices root.test1.**",
        countDevicesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"1,"},
        "test1",
        "test123123456");

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
        "test123123456");

    // show child nodes
    resultSetEqualTest(
        "show child nodes root",
        showChildNodesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .toArray(String[]::new),
        new String[] {"test,", "test1,"},
        "test1",
        "test123123456");

    // count nodes level
    resultSetEqualTest(
        "count nodes root.** level=1",
        countNodesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123123456");
    resultSetEqualTest(
        "count nodes root.** level=2",
        countNodesColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"2,"},
        "test1",
        "test123123456");

    // TTL
    resultSetEqualTest(
        "show all ttl",
        showTTLColumnHeaders.stream().map(ColumnHeader::getColumnName).toArray(String[]::new),
        new String[] {"root.test,10000,", "root.test.**,10000,"},
        "test1",
        "test123123456");
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
        "test123123456");
    assertNonQueryTestFail(
        "delete from root.sg.d1.s1, root.sg.d1.s2",
        "803: No permissions for this operation, please add privilege WRITE_DATA on [root.sg.d1.s2]",
        "test1",
        "test123123456");
    grantUserSeriesPrivilege("test1", PrivilegeType.WRITE_DATA, "root.sg.d1.s2");
    assertNonQueryTestFail(
        "insert into root.sg.d1(time,s1,s2) values(1,1,1)",
        "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.sg.d1.s1, root.sg.d1.s2]",
        "test1",
        "test123123456");
    executeNonQuery("delete from root.sg.d1.s1, root.sg.d1.s2", "test1", "test123123456");
    grantUserSeriesPrivilege("test1", PrivilegeType.WRITE_SCHEMA, "root.sg.d1.**");
    assertNonQueryTestFail(
        "insert into root.sg.d1(time,s1,s2) values(1,1,1)",
        "803: No permissions for this operation, please add privilege SYSTEM",
        "test1",
        "test123123456");
    grantUserSystemPrivileges("test1", PrivilegeType.SYSTEM);
    executeNonQuery("insert into root.sg.d1(time,s1,s2) values(1,1,1)", "test1", "test123123456");
  }

  private void testReadData() {
    executeNonQuery("insert into  root.test.d1(time,s1) values(1,1)");
    executeNonQuery("insert into root.test1.d1(time,s1) values(2,2)");
    executeNonQuery("insert into root.test1.d2(time,s1) values(2,2)");

    resultSetEqualTest(
        "select * from root.test1.**",
        new String[] {TIME},
        new String[] {},
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.READ_DATA, "root.test.**");
    resultSetEqualTest(
        "select * from root.test.**",
        new String[] {TIME, "root.test.d1.s1"},
        new String[] {"1,1.0,"},
        "test",
        "test123123456");
    grantUserSeriesPrivilege("test", PrivilegeType.READ_DATA, "root.test1.d1.**");
    resultSetEqualTest(
        "select * from root.test.**,root.test1.**",
        new String[] {TIME, "root.test.d1.s1", "root.test1.d1.s1"},
        new String[] {"1,1.0,null,", "2,null,2.0,"},
        "test",
        "test123123456");
  }

  @Test
  public void ttlOperationsTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection("test2", "test123123456");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("show all ttl");
      Assert.assertFalse(resultSet.next());
      assertNonQueryTestFail(
          statement,
          "set ttl to root.test.** 1",
          "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.**]");
      assertNonQueryTestFail(
          statement,
          "unset ttl from root.test.**",
          "803: No permissions for this operation, please add privilege WRITE_SCHEMA on [root.test.**]");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("show all ttl");
      Assert.assertTrue(resultSet.next());
      Assert.assertFalse(resultSet.next());
      statement.execute("grant WRITE_SCHEMA on root.test.** to user test2");

      assertNonQueryTestFail(
          statement,
          "set ttl to root.__audit.** 1",
          "803: The database 'root.__audit' is read-only.");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection("test2", "test123123456");
        Statement statement = connection.createStatement()) {
      statement.execute("set ttl to root.test.** 1");
      ResultSet resultSet = statement.executeQuery("show all ttl");
      Assert.assertTrue(resultSet.next());
      Assert.assertFalse(resultSet.next());
      statement.execute("unset ttl from root.test.**");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
