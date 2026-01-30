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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableWithDefinedTimeIT {

  private static final String TABLE_DATABASE = "user_defined_time";
  private static final String VIEW_DATABASE = "user_defined_time_for_view";
  private static final String[] SQLS =
      new String[] {"CREATE DATABASE " + TABLE_DATABASE, "CREATE DATABASE " + VIEW_DATABASE};
  private final String header = "ColumnName,DataType,Category,";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateTable() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + TABLE_DATABASE);

      // create table and do not assign the time column name
      try {
        statement.execute(
            "create table default_not_assign_time(device string tag, s1 int32 field)");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe default_not_assign_time"),
            header,
            Arrays.asList("time,TIMESTAMP,TIME,", "device,STRING,TAG,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("create table without time info fails, the specific message:  " + e.getMessage());
      }

      // create table and assign the time column name
      try {
        statement.execute(
            "create table time_in_first(date_time timestamp time, device string tag, s1 int32 field)");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe time_in_first"),
            header,
            Arrays.asList("date_time,TIMESTAMP,TIME,", "device,STRING,TAG,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("assign the name of time column fails, the specific message:  " + e.getMessage());
      }

      // create table which of the time column not at the first column
      try {
        statement.execute(
            "create table time_not_in_first(device string tag, date_time timestamp time,  s1 int32 field)");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe time_not_in_first"),
            header,
            Arrays.asList("device,STRING,TAG,", "date_time,TIMESTAMP,TIME,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("assign the name of time column fails, the specific message:  " + e.getMessage());
      }

      // create table with multi time-column
      try {
        statement.execute(
            "create table with_multi_time(time_type timestamp time, device string tag, date_time timestamp time,  s1 int32 field)");
        fail("Creating table is not be allowed to assign two time columns");
      } catch (SQLException e) {
        assertEquals("701: A table cannot have more than one time column", e.getMessage());
      }

      // create table with time column that is not timestamp data type
      try {
        statement.execute(
            "create table time_other_type(device string tag, date_time int64 time,  s1 int32 field)");
        fail("The time column has to be assigned a timestamp data type when creating table");
      } catch (SQLException e) {
        assertEquals("701: The time column's type shall be 'timestamp'.", e.getMessage());
      }

      // Columns in table shall not share the same name time when creating table
      try {
        statement.execute(
            "create table shared_time_name(device string tag, time int64 field,  s1 int32 field)");
        fail("Columns in table shall not share the same name time when creating table");
      } catch (SQLException e) {
        assertEquals(
            "701: Columns in table shall not share the same name: 'time'.", e.getMessage());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareTreeData() {
    try (final Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.tt.device.s1 with datatype=int32");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateView() {
    prepareTreeData();

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use " + VIEW_DATABASE);

      // create view and do not assign the time column name
      try {
        statement.execute(
            "create view default_not_assign_time(device string tag, s1 int32 field) as root.tt.**");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe default_not_assign_time"),
            header,
            Arrays.asList("time,TIMESTAMP,TIME,", "device,STRING,TAG,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("create table without time info fails, the specific message:  " + e.getMessage());
      }

      // create view which of the time column at the first column
      try {
        statement.execute(
            "create view time_in_first(date_time timestamp time, device string tag, s1 int32 field) as root.tt.**");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe time_in_first"),
            header,
            Arrays.asList("date_time,TIMESTAMP,TIME,", "device,STRING,TAG,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("assign the name of time column fails, the specific message:  " + e.getMessage());
      }

      // create view which of the time column not at the first column
      try {
        statement.execute(
            "create view time_not_in_first(device string tag, date_time timestamp time,  s1 int32 field) as root.tt.**");
        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe time_not_in_first"),
            header,
            Arrays.asList("device,STRING,TAG,", "date_time,TIMESTAMP,TIME,", "s1,INT32,FIELD,"));
      } catch (SQLException e) {
        fail("assign the name of time column fails, the specific message:  " + e.getMessage());
      }

      // create view with multi time-column
      try {
        statement.execute(
            "create view with_multi_time(time_type timestamp time, device string tag, date_time timestamp time,  s1 int32 field) as root.tt.**");
        fail("Creating view is not be allowed to assign two time columns");
      } catch (SQLException e) {
        assertEquals("701: A table cannot have more than one time column", e.getMessage());
      }

      // create table with time column that is not timestamp data type
      try {
        statement.execute(
            "create view time_other_type(device string tag, date_time int64 time,  s1 int32 field) as root.tt.**");
        fail("The time column has to be assigned a timestamp data type when creating view");
      } catch (SQLException e) {
        assertEquals("701: The time column's type shall be 'timestamp'.", e.getMessage());
      }

      // Columns in table shall not share the same name time when creating table
      try {
        statement.execute(
            "create view shared_time_time(device string tag, time int64 field,  s1 int32 field) as root.tt.**");
        fail("Columns in view shall not share the same name time when creating table");
      } catch (SQLException e) {
        assertEquals(
            "701: Columns in table shall not share the same name: 'time'.", e.getMessage());
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
