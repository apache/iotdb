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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.describeTableColumnHeaders;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testManageTable() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {

      statement.execute("create database test1");
      statement.execute("create database test2");

      // should specify database before create table
      try {
        statement.execute(
            "create table table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: database is not specified", e.getMessage());
      }

      // Show tables shall succeed in a newly created database with no tables
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables from test1")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        assertFalse(resultSet.next());
      }

      // or use full qualified table name
      // test "TTL=INF"
      // "MEASUREMENT" can be omitted when type is specified
      // "STRING" can be omitted when id/attribute is specified
      statement.execute(
          "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE) with (TTL='INF')");

      try {
        statement.execute(
            "create table test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");
        fail();
      } catch (final SQLException e) {
        assertEquals("551: Table 'test1.table1' already exists.", e.getMessage());
      }

      String[] tableNames = new String[] {"table1"};
      String[] ttls = new String[] {"INF"};

      statement.execute("use test2");

      // show tables by specifying another database
      // Check duplicate create table won't affect table state
      // using SHOW tables in
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables in test1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // Alter table properties
      statement.execute("alter table test1.table1 set properties ttl=1");
      ttls = new String[] {"1"};

      // Alter non-exist table
      try {
        statement.execute("alter table test1.nonExist set properties ttl=1");
      } catch (final SQLException e) {
        assertEquals("550: Table 'test1.nonexist' does not exist", e.getMessage());
      }

      // If exists
      statement.execute("alter table if exists test1.nonExist set properties ttl=1");

      // Alter non-supported properties
      try {
        statement.execute("alter table test1.table1 set properties nonSupport=1");
      } catch (final SQLException e) {
        assertEquals("701: Table property 'nonsupport' is currently not allowed.", e.getMessage());
      }

      // using SHOW tables from
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables from test1")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // Set back to default
      statement.execute("alter table test1.table1 set properties ttl=DEFAULT");
      ttls = new String[] {"INF"};

      try (final ResultSet resultSet = statement.executeQuery("SHOW tables from test1")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // Create if not exist
      statement.execute(
          "create table if not exists test1.table1(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT)");

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT, time2 INT64 TIME) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: Create table statement shall not specify column category TIME", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (UNKNOWN=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: Table property 'unknown' is currently not allowed.", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=null)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: ttl value must be a LongLiteral, but now is NullLiteral, value: null",
            e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=-1)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: ttl value must be equal to or greater than 0, but now is: -1", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id TEXT ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ID Column should only be STRING, current is TEXT", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id INT32 ID, plant_id STRING ID, device_id STRING ID, model STRING ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ID Column should only be STRING, current is INT32", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model TEXT ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is TEXT",
            e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING ID, plant_id STRING ID, device_id STRING ID, model DOUBLE ATTRIBUTE, temperature FLOAT MEASUREMENT, humidity DOUBLE MEASUREMENT) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is DOUBLE",
            e.getMessage());
      }

      statement.execute(
          "create table table2(region_id STRING ID, plant_id STRING ID, color STRING ATTRIBUTE, temperature FLOAT MEASUREMENT) with (TTL=6600000)");

      statement.execute("alter table table2 add column speed DOUBLE MEASUREMENT");

      try {
        statement.execute("alter table table2 add column speed DOUBLE MEASUREMENT");
      } catch (final SQLException e) {
        assertEquals("552: Column 'speed' already exist", e.getMessage());
      }

      statement.execute("alter table table2 add column if not exists speed DOUBLE MEASUREMENT");

      try {
        statement.execute("alter table table3 add column speed DOUBLE MEASUREMENT");
      } catch (final SQLException e) {
        assertEquals("550: Table 'test2.table3' does not exist", e.getMessage());
      }

      statement.execute("alter table if exists table3 add column speed DOUBLE MEASUREMENT");

      tableNames = new String[] {"table2"};
      ttls = new String[] {"6600000"};

      // show tables from current database
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesColumnHeaders.size(); i++) {
          assertEquals(
              showTablesColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // Test create table with only time column
      statement.execute("create table table3()");

      // show tables from a non-exist database
      try {
        statement.executeQuery("SHOW tables from test3");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test3", e.getMessage());
      }

      // describe
      try {
        statement.executeQuery("describe table1");
        fail();
      } catch (final SQLException e) {
        assertEquals("550: Table 'test2.table1' does not exist.", e.getMessage());
      }

      String[] columnNames =
          new String[] {
            "time", "region_id", "plant_id", "device_id", "model", "temperature", "humidity"
          };
      String[] dataTypes =
          new String[] {"TIMESTAMP", "STRING", "STRING", "STRING", "STRING", "FLOAT", "DOUBLE"};
      String[] categories =
          new String[] {"TIME", "ID", "ID", "ID", "ATTRIBUTE", "MEASUREMENT", "MEASUREMENT"};

      try (final ResultSet resultSet = statement.executeQuery("describe test1.table1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableColumnHeaders.size(); i++) {
          assertEquals(
              describeTableColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      columnNames = new String[] {"time", "region_id", "plant_id", "color", "temperature", "speed"};
      dataTypes = new String[] {"TIMESTAMP", "STRING", "STRING", "STRING", "FLOAT", "DOUBLE"};
      categories = new String[] {"TIME", "ID", "ID", "ATTRIBUTE", "MEASUREMENT", "MEASUREMENT"};

      try (final ResultSet resultSet = statement.executeQuery("desc table2")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableColumnHeaders.size(); i++) {
          assertEquals(
              describeTableColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      try {
        statement.executeQuery("describe test3.table3");
        fail();
      } catch (final SQLException e) {
        assertEquals("550: Table 'test3.table3' does not exist.", e.getMessage());
      }

      statement.execute("drop database test1");

      try {
        statement.executeQuery("SHOW tables from test1");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
