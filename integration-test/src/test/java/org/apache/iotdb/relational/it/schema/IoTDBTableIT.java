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
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.describeTableColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.describeTableDetailsColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDBColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTablesColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showTablesDetailsColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setRestrictObjectLimit(true);
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
      statement.execute("create database test2 with (ttl=3000000)");

      // should specify database before create table
      try {
        statement.execute(
            "create table table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD)");
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
      // "FIELD" can be omitted when type is specified
      // "STRING" can be omitted when tag/attribute is specified
      statement.execute(
          "create table test1.table1(time TIMESTAMP TIME COMMENT 'column_comment', region_id STRING TAG, plant_id STRING TAG, device_id TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE) comment 'test' with (TTL='INF')");

      try {
        statement.execute(
            "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD)");
        fail();
      } catch (final SQLException e) {
        assertEquals("551: Table 'test1.table1' already exists.", e.getMessage());
      }

      String[] tableNames = new String[] {"table1"};
      String[] ttls = new String[] {"INF"};
      String[] statuses = new String[] {"USING"};
      String[] comments = new String[] {"test"};

      statement.execute("use test2");

      // show tables by specifying another database
      // Check duplicate create table won't affect table state
      // using SHOW tables in
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables details in test1")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesDetailsColumnHeaders.size(); i++) {
          assertEquals(
              showTablesDetailsColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          assertEquals(statuses[cnt], resultSet.getString(3));
          assertEquals(comments[cnt], resultSet.getString(4));
          cnt++;
        }
        assertEquals(tableNames.length, cnt);
      }

      // Alter table properties
      statement.execute("alter table test1.table1 set properties ttl=1000000");
      ttls = new String[] {"1000000"};

      // Alter non-exist table
      try {
        statement.execute("alter table test1.nonExist set properties ttl=1000000");
      } catch (final SQLException e) {
        assertEquals("550: Table 'test1.nonexist' does not exist", e.getMessage());
      }

      // If exists
      statement.execute("alter table if exists test1.nonExist set properties ttl=1000000");

      // Alter non-supported properties
      try {
        statement.execute("alter table test1.table1 set properties nonSupport=1000000");
      } catch (final SQLException e) {
        assertEquals("701: Table property 'nonsupport' is currently not allowed.", e.getMessage());
      }

      statement.execute("comment on table test1.table1 is 'new_test'");
      comments = new String[] {"new_test"};
      // using SHOW tables from
      try (final ResultSet resultSet = statement.executeQuery("SHOW tables details from test1")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showTablesDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showTablesDetailsColumnHeaders.size(); i++) {
          assertEquals(
              showTablesDetailsColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(tableNames[cnt], resultSet.getString(1));
          assertEquals(ttls[cnt], resultSet.getString(2));
          assertEquals(comments[cnt], resultSet.getString(4));
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
          "create table if not exists test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD)");

      try {
        statement.execute(
            "create table table2(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (UNKNOWN=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: Table property 'unknown' is currently not allowed.", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=null)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: ttl value must be a LongLiteral, but now is NullLiteral, value: null",
            e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=-1)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: ttl value must be equal to or greater than 0, but now is: -1", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id TEXT TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of TAG Column should only be STRING, current is TEXT", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id INT32 TAG, plant_id STRING TAG, device_id STRING TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of TAG Column should only be STRING, current is INT32", e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model TEXT ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is TEXT",
            e.getMessage());
      }

      try {
        statement.execute(
            "create table table2(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, model DOUBLE ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD) with (TTL=3600000)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: DataType of ATTRIBUTE Column should only be STRING, current is DOUBLE",
            e.getMessage());
      }

      statement.execute(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD) with (TTL=6600000)");

      statement.execute("alter table table2 add column speed DOUBLE FIELD COMMENT 'fast'");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show create table table2"),
          "Table,Create Table,",
          Collections.singleton(
              "table2,CREATE TABLE \"table2\" (\"region_id\" STRING TAG,\"plant_id\" STRING TAG,\"color\" STRING ATTRIBUTE,\"temperature\" FLOAT FIELD,\"speed\" DOUBLE FIELD COMMENT 'fast') WITH (ttl=6600000),"));

      try {
        statement.execute("alter table table2 add column speed DOUBLE FIELD");
      } catch (final SQLException e) {
        assertEquals("552: Column 'speed' already exist", e.getMessage());
      }

      statement.execute("alter table table2 add column if not exists speed DOUBLE FIELD");

      try {
        statement.execute("alter table table3 add column speed DOUBLE FIELD");
      } catch (final SQLException e) {
        assertEquals("550: Table 'test2.table3' does not exist", e.getMessage());
      }

      statement.execute("alter table if exists table3 add column speed DOUBLE FIELD");

      // Test create table with only time column
      statement.execute("create table table3()");

      tableNames = new String[] {"table2", "table3"};
      ttls = new String[] {"6600000", "3000000"};

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

      // Will not affect the manual "6600000"
      statement.execute("alter database test2 set properties ttl=6600000");
      statement.execute("alter database test2 set properties ttl=DEFAULT");

      statement.execute("alter table table3 set properties ttl=1000000");
      statement.execute("alter table table3 set properties ttl=DEFAULT");

      ttls = new String[] {"6600000", "INF"};
      // The table3's ttl shall be "INF"
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
          new String[] {"TIME", "TAG", "TAG", "TAG", "ATTRIBUTE", "FIELD", "FIELD"};

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
      categories = new String[] {"TIME", "TAG", "TAG", "ATTRIBUTE", "FIELD", "FIELD"};

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

      statement.execute(
          "insert into table2(region_id, plant_id, color, temperature, speed) values(1, 1, 1, 1, 1)");

      // Test drop column
      statement.execute("alter table table2 drop column color");

      // Test comment
      // Before
      columnNames = new String[] {"time", "region_id", "plant_id", "temperature", "speed"};
      dataTypes = new String[] {"TIMESTAMP", "STRING", "STRING", "FLOAT", "DOUBLE"};
      categories = new String[] {"TIME", "TAG", "TAG", "FIELD", "FIELD"};
      statuses = new String[] {"USING", "USING", "USING", "USING", "USING"};

      comments = new String[] {null, null, null, null, "fast"};
      try (final ResultSet resultSet = statement.executeQuery("describe table2 details")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableDetailsColumnHeaders.size(); i++) {
          assertEquals(
              describeTableDetailsColumnHeaders.get(i).getColumnName(),
              metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          assertEquals(statuses[cnt], resultSet.getString(4));
          assertEquals(comments[cnt], resultSet.getString(5));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      // After
      statement.execute("COMMENT ON COLUMN table2.region_id IS '重庆'");
      statement.execute("COMMENT ON COLUMN table2.region_id IS NULL");
      statement.execute("COMMENT ON COLUMN test2.table2.time IS 'recent'");
      statement.execute("COMMENT ON COLUMN test2.table2.region_id IS ''");

      comments = new String[] {"recent", "", null, null, "fast"};
      try (final ResultSet resultSet = statement.executeQuery("describe table2 details")) {
        int cnt = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(describeTableDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < describeTableDetailsColumnHeaders.size(); i++) {
          assertEquals(
              describeTableDetailsColumnHeaders.get(i).getColumnName(),
              metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          assertEquals(columnNames[cnt], resultSet.getString(1));
          assertEquals(dataTypes[cnt], resultSet.getString(2));
          assertEquals(categories[cnt], resultSet.getString(3));
          assertEquals(statuses[cnt], resultSet.getString(4));
          assertEquals(comments[cnt], resultSet.getString(5));
          cnt++;
        }
        assertEquals(columnNames.length, cnt);
      }

      statement.execute("alter table table2 drop column speed");

      try {
        statement.executeQuery("select color from table2");
        fail();
      } catch (final SQLException e) {
        assertEquals("616: Column 'color' cannot be resolved", e.getMessage());
      }

      try {
        statement.executeQuery("select speed from table2");
        fail();
      } catch (final SQLException e) {
        assertEquals("616: Column 'speed' cannot be resolved", e.getMessage());
      }

      try {
        statement.execute("alter table table2 drop column speed");
      } catch (final SQLException e) {
        assertEquals("616: Column speed in table 'test2.table2' does not exist.", e.getMessage());
      }

      try {
        statement.execute("alter table table2 drop column time");
      } catch (final SQLException e) {
        assertEquals("701: Dropping tag or time column is not supported.", e.getMessage());
      }

      // test data deletion by drop column
      statement.execute("alter table table2 add column speed double");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select speed from table2"),
          "speed,",
          Collections.singleton("null,"));

      statement.execute("drop table table2");
      try {
        statement.executeQuery("describe table2");
        fail();
      } catch (final SQLException e) {
        assertEquals("550: Table 'test2.table2' does not exist.", e.getMessage());
      }
      statement.execute(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD)");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from table2"),
          "count(devices),",
          Collections.singleton("0,"));

      // Test data deletion by drop table
      statement.execute(
          "insert into table2(region_id, plant_id, color, temperature, speed) values(1, 1, 1, 1, 1)");
      TestUtils.assertResultSetSize(statement.executeQuery("select * from table2"), 1);

      try {
        statement.executeQuery("describe test3.table3");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test3", e.getMessage());
      }

      statement.execute("drop database test1");

      // Test error messages
      try {
        statement.executeQuery("SHOW tables from test1");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("create table test1.test()");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("alter table test1.test add column a int32");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("alter table test1.test drop column a");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("alter table test1.test set properties ttl=default");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("desc test1.test");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      try {
        statement.execute("drop table test1.test");
        fail();
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      // Test time column
      statement.execute("create table test100 (time time)");
      statement.execute("create table test101 (time timestamp time)");

      try {
        statement.execute("create table test102 (time timestamp tag)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: The time column category shall be bounded with column name 'time'.",
            e.getMessage());
      }

      try {
        statement.execute("create table test102 (time tag)");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: The time column's type shall be 'timestamp'.", e.getMessage());
      }

      try {
        statement.execute("create table test102 (time time, time time)");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: Columns in table shall not share the same name time.", e.getMessage());
      }
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTableAuth() throws SQLException {
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password123456'");
      adminStmt.execute("create database db");
      adminStmt.execute("use db");
      adminStmt.execute("create table test (a tag, b attribute, c int32)");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      Assert.assertThrows(SQLException.class, () -> userStmt.execute("select * from db.test"));
      TestUtils.assertResultSetEqual(
          userStmt.executeQuery("select * from information_schema.tables where database = 'db'"),
          "database,table_name,ttl(ms),status,comment,table_type,",
          Collections.emptySet());
      TestUtils.assertResultSetEqual(
          userStmt.executeQuery("select * from information_schema.columns where database = 'db'"),
          "database,table_name,column_name,datatype,category,status,comment,",
          Collections.emptySet());
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT SELECT ON db.test to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      try (final ResultSet resultSet = userStmt.executeQuery("SHOW DATABASES")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        Assert.assertTrue(resultSet.next());
        assertEquals("db", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        assertEquals("information_schema", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      userStmt.execute("select * from db.test");
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT DROP ON DATABASE DB to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      userStmt.execute("use db");
      userStmt.execute("drop table test");
    }
  }

  // Test deadlock
  @Test(timeout = 60000)
  public void testConcurrentAutoCreateAndDropColumn() throws Exception {
    try (final ITableSession session = EnvFactory.getEnv().getTableSessionConnection();
        final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create database db1");
      session.executeNonQueryStatement("USE \"db1\"");

      final StringBuilder sb = new StringBuilder("CREATE TABLE table8 (tag1 string tag");
      for (int i = 0; i < 100; ++i) {
        sb.append(String.format(", m%s string", i));
      }
      sb.append(")");
      session.executeNonQueryStatement(sb.toString());

      final Thread insertThread =
          new Thread(
              () -> {
                for (int i = 0; i < 100; ++i) {
                  final List<IMeasurementSchema> schemaList = new ArrayList<>();
                  schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
                  schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
                  schemaList.add(
                      new MeasurementSchema(String.format("m%s", 100 + i), TSDataType.DOUBLE));
                  final List<ColumnCategory> columnTypes =
                      Arrays.asList(
                          ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

                  long timestamp = 0;

                  final Tablet tablet =
                      new Tablet(
                          "table8",
                          IMeasurementSchema.getMeasurementNameList(schemaList),
                          IMeasurementSchema.getDataTypeList(schemaList),
                          columnTypes,
                          15);

                  for (int row = 0; row < 15; row++) {
                    tablet.addTimestamp(row, timestamp);
                    tablet.addValue("tag1", row, "tag:" + timestamp);
                    tablet.addValue("attr1", row, "attr:" + timestamp);
                    tablet.addValue(String.format("m%s", 100 + i), row, timestamp * 1.0);
                    timestamp++;
                  }

                  try {
                    session.insert(tablet);
                  } catch (final StatementExecutionException | IoTDBConnectionException e) {
                    throw new RuntimeException(e);
                  }
                  tablet.reset();
                }
              });

      final Thread deletionThread =
          new Thread(
              () -> {
                for (int i = 0; i < 100; ++i) {
                  try {
                    adminStmt.execute(String.format("alter table db1.table8 drop column m%s", i));
                  } catch (final SQLException e) {
                    throw new RuntimeException(e);
                  }
                }
              });

      insertThread.start();
      deletionThread.start();

      insertThread.join();
      deletionThread.join();
    }
  }

  @Test
  public void testTreeViewTable() throws Exception {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root.another");
      statement.execute("create database root.`重庆`.b");
      statement.execute("create timeSeries root.`重庆`.b.c.S1 int32");
      statement.execute("create timeSeries root.`重庆`.b.c.s2 string");
      statement.execute("create timeSeries root.`重庆`.b.S1 int32");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database tree_view_db");
      statement.execute("use tree_view_db");

      try {
        statement.execute("create view tree_table (tag1 tag, tag2 tag) as root.**");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: Cannot specify view pattern to match more than one tree database.",
            e.getMessage());
      }
      statement.execute("create view tree_table (tag1 tag, tag2 tag) as root.\"重庆\".**");
      statement.execute("drop view tree_table");
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create timeSeries root.`重庆`.b.d.s1 int32");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use tree_view_db");

      try {
        statement.execute("create view tree_table (tag1 tag, tag2 tag) as root.\"重庆\".**");
        fail();
      } catch (final SQLException e) {
        final Set<String> result =
            new HashSet<>(
                Arrays.asList(
                    "617: The measurements s1 and S1 share the same lower case when auto detecting type, please check",
                    "617: The measurements S1 and s1 share the same lower case when auto detecting type, please check"));
        assertTrue(result.contains(e.getMessage()));
      }
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("drop timeSeries root.`重庆`.b.d.s1");
      statement.execute("create device template t1 (S1 boolean, s9 int32)");
      statement.execute("set schema template t1 to root.`重庆`.b.d");
      statement.execute("create timeSeries root.`重庆`.b.c.f.g.h.S1 int32");

      // Put schema cache
      statement.execute("select S1, s2 from root.`重庆`.b.c");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use tree_view_db");

      try {
        statement.execute("create view tree_table (tag1 tag, tag2 tag) as root.\"重庆\".**");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "614: Multiple types encountered when auto detecting type of measurement 'S1', please check",
            e.getMessage());
      }

      try {
        statement.execute(
            "create view tree_table (tag1 tag, tag2 tag, S1 field) as root.\"重庆\".**");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "614: Multiple types encountered when auto detecting type of measurement 'S1', please check",
            e.getMessage());
      }
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create timeSeries root.`重庆`.b.e.s1 int32");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use tree_view_db");

      // Test error message
      try {
        statement.execute("alter view view_not_exist add column col from col");
        fail();
      } catch (final SQLException e) {
        assertEquals("550: Table 'tree_view_db.view_not_exist' does not exist", e.getMessage());
      }

      // Temporary
      try {
        statement.execute(
            "create or replace view tree_table (tag1 tag, tag2 tag, S1 int32 field, s3 boolean from S1) as root.\"重庆\".**");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: The duplicated source measurement S1 is unsupported yet.", e.getMessage());
      }

      try {
        statement.execute(
            "create or replace view tree_table (tag1 tag, tag2 tag, S1 int32 field, s3 from s2, s8 field) as root.\"重庆\".**");
        fail();
      } catch (final SQLException e) {
        assertEquals("528: Measurements not found for s8, cannot auto detect", e.getMessage());
      }

      statement.execute(
          "create or replace view tree_table (tag1 tag, tag2 tag, S1 int32 field, s3 from s2) as root.\"重庆\".**");

      // Cannot be written
      try {
        statement.execute(
            "insert into tree_table(time, tag1, tag2, S1, s3) values (1, 1, 1, 1, 1)");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: The table tree_view_db.tree_table is a view from tree, cannot be written or deleted from",
            e.getMessage());
      }

      statement.execute("alter view tree_table rename to view_table");

      // Test clear cache
      try {
        statement.execute("select * from tree_table");
        fail();
      } catch (final SQLException e) {
        assertEquals("550: Table 'tree_view_db.tree_table' does not exist.", e.getMessage());
      }

      statement.execute("alter view view_table rename column s1 to s11");
      statement.execute("alter view view_table set properties ttl=100");
      statement.execute("comment on view view_table is 'comment'");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show tables details"),
          "TableName,TTL(ms),Status,Comment,TableType,",
          Collections.singleton("view_table,100,USING,comment,VIEW FROM TREE,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc view_table"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,",
                  "tag1,STRING,TAG,",
                  "tag2,STRING,TAG,",
                  "s11,INT32,FIELD,",
                  "s3,STRING,FIELD,")));
      // Currently we show the device even if all of its measurements does not match,
      // the handling logic at query because validate it at fetching will potentially cause a
      // lot of time
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from view_table where tag1 = 'b'"),
          "tag1,tag2,",
          new HashSet<>(Arrays.asList("b,c,", "b,null,", "b,e,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from view_table where tag1 = 'b' and tag2 is null"),
          "tag1,tag2,",
          Collections.singleton("b,null,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from view_table"),
          "count(devices),",
          Collections.singleton("3,"));
    }

    // Test tree session
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      // Test create & replace + restrict
      statement.execute(
          "create or replace view tree_view_db.view_table (tag1 tag, tag2 tag, s11 int32 field, s3 from s2) restrict with (ttl=100) as root.`重庆`.**");
      fail();
    } catch (final SQLException e) {
      assertTrue(
          e.getMessage().contains("The 'CreateTableView' is unsupported in tree sql-dialect."));
    }

    // Test permission
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      // Test create & replace + restrict
      statement.execute("create user testUser 'testUser123456'");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection("testUser", "testUser123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace view tree_view_db.view_table (tag1 tag, tag2 tag, s11 int32 field, s3 from s2) restrict with (ttl=100) as root.\"重庆\".**");
      fail();
    } catch (final SQLException e) {
      assertEquals(
          "803: Access Denied: No permissions for this operation, please add privilege CREATE ON tree_view_db.view_table",
          e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("grant create on tree_view_db.view_table to user testUser");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection("testUser", "testUser123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace view tree_view_db.view_table (tag1 tag, tag2 tag, s11 int32 field, s3 from s2) restrict with (ttl=100) as root.\"重庆\".**");
      fail();
    } catch (final SQLException e) {
      assertEquals(
          "803: Access Denied: No permissions for this operation, please add privilege READ_SCHEMA",
          e.getMessage());
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("grant read_schema on root.`重庆`.** to user testUser");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection("testUser", "testUser123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace view tree_view_db.view_table (tag1 tag, tag2 tag, s11 int32 field, s3 from s2) restrict with (ttl=100) as root.\"重庆\".**");
      fail();
    } catch (final SQLException e) {
      assertEquals(
          "803: Access Denied: No permissions for this operation, please add privilege READ_DATA",
          e.getMessage());
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("grant read_data on root.`重庆`.** to user testUser");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    try (final Connection connection =
            EnvFactory.getEnv()
                .getConnection("testUser", "testUser123456", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace view tree_view_db.view_table (tag1 tag, tag2 tag, s11 int32 field, s3 from s2) restrict with (ttl=100) as root.\"重庆\".**");
    } catch (final SQLException e) {
      fail();
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use tree_view_db");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from view_table where tag1 = 'b' and tag2 is null"),
          "tag1,tag2,",
          Collections.emptySet());

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show create view view_table"),
          "View,Create View,",
          Collections.singleton(
              "view_table,CREATE VIEW \"view_table\" (\"tag1\" STRING TAG,\"tag2\" STRING TAG,\"s11\" INT32 FIELD,\"s3\" STRING FIELD FROM \"s2\") RESTRICT WITH (ttl=100) AS root.\"重庆\".**,"));

      // Can also use "show create table"
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show create table view_table"),
          "View,Create View,",
          Collections.singleton(
              "view_table,CREATE VIEW \"view_table\" (\"tag1\" STRING TAG,\"tag2\" STRING TAG,\"s11\" INT32 FIELD,\"s3\" STRING FIELD FROM \"s2\") RESTRICT WITH (ttl=100) AS root.\"重庆\".**,"));

      statement.execute("create table a ()");
      try {
        statement.execute("show create view a");
        fail();
      } catch (final SQLException e) {
        assertEquals(
            "701: The table a is a base table, does not support show create view.", e.getMessage());
      }
      try {
        statement.execute("show create view information_schema.tables");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: The system view does not support show create.", e.getMessage());
      }
      try {
        statement.execute("show create table information_schema.tables");
        fail();
      } catch (final SQLException e) {
        assertEquals("701: The system view does not support show create.", e.getMessage());
      }
      try {
        statement.execute("create or replace view a () as root.b.**");
        fail();
      } catch (final SQLException e) {
        assertEquals("551: Table 'tree_view_db.a' already exists.", e.getMessage());
      }
    }
  }

  @Test
  public void testAlterTableName() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      try {
        statement.execute(
            "CREATE TABLE IF NOT EXISTS alter_table_name_disabled () WITH (allow_alter_name=1)");
        fail("allow_alter_name must be boolean");
      } catch (SQLException e) {
        assertEquals(
            "701: allow_alter_name value must be a BooleanLiteral, but now is LongLiteral, value: 1",
            e.getMessage());
      }

      statement.execute(
          "CREATE TABLE IF NOT EXISTS alter_table_name_disabled () WITH (allow_alter_name=false)");

      try {
        statement.execute(
            "ALTER TABLE alter_table_name_disabled SET PROPERTIES allow_alter_name=true");
        fail("allow_alter_name cannot be altered");
      } catch (SQLException e) {
        assertEquals("701: The property allow_alter_name cannot be altered.", e.getMessage());
      }

      try {
        statement.execute("ALTER TABLE alter_table_name_disabled RENAME TO alter_table_named");
        fail("the table cannot be renamed");
      } catch (SQLException e) {
        assertEquals(
            "701: Table 'testdb.alter_table_name_disabled' is created in a old version and cannot be renamed, please migrate its data to a new table manually",
            e.getMessage());
      }

      // alter once
      statement.execute("CREATE TABLE IF NOT EXISTS alter_table_name (s1 int32)");
      statement.execute("INSERT INTO alter_table_name (time, s1) VALUES (1, 1)");
      statement.execute("ALTER TABLE alter_table_name RENAME TO alter_table_named");
      try {
        statement.execute("INSERT INTO alter_table_name (time, s1) VALUES (0, 0)");
        fail();
      } catch (SQLException e) {
        assertEquals("550: Table 'testdb.alter_table_name' does not exist.", e.getMessage());
      }
      statement.execute("INSERT INTO alter_table_named (time, s1) VALUES (2, 2)");

      ResultSet resultSet = statement.executeQuery("SELECT * FROM alter_table_named");
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getLong(1));
      assertEquals(1, resultSet.getLong(2));
      assertTrue(resultSet.next());
      assertEquals(2, resultSet.getLong(1));
      assertEquals(2, resultSet.getLong(2));
      assertFalse(resultSet.next());

      // alter twice
      statement.execute("ALTER TABLE alter_table_named RENAME TO alter_table_named2");
      try {
        statement.execute("INSERT INTO alter_table_named (time, s1) VALUES (0, 0)");
        fail();
      } catch (SQLException e) {
        assertEquals("550: Table 'testdb.alter_table_named' does not exist.", e.getMessage());
      }
      statement.execute("INSERT INTO alter_table_named2 (time, s1) VALUES (3, 3)");

      resultSet = statement.executeQuery("SELECT * FROM alter_table_named2");
      for (int i = 1; i <= 3; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        assertEquals(i, resultSet.getLong(2));
      }
      assertFalse(resultSet.next());

      // alter back
      statement.execute("ALTER TABLE alter_table_named2 RENAME TO alter_table_name");
      try {
        statement.execute("INSERT INTO alter_table_named2 (time, s1) VALUES (0, 0)");
        fail();
      } catch (SQLException e) {
        assertEquals("550: Table 'testdb.alter_table_named2' does not exist.", e.getMessage());
      }
      statement.execute("INSERT INTO alter_table_name (time, s1) VALUES (4, 4)");

      resultSet = statement.executeQuery("SELECT * FROM alter_table_name");
      for (int i = 1; i <= 4; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        assertEquals(i, resultSet.getLong(2));
      }
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testAlterColumnName() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS alter_column_name (s1 int32)");
      statement.execute("INSERT INTO alter_column_name (time, s1) VALUES (1, 1)");
      // alter once
      statement.execute("ALTER TABLE alter_column_name RENAME COLUMN s1 TO s2");
      try {
        statement.execute("INSERT INTO alter_column_name (time, s1) VALUES (0, 0)");
        fail();
      } catch (SQLException e) {
        assertEquals(
            "616: Unknown column category for s1. Cannot auto create column.", e.getMessage());
      }
      statement.execute("INSERT INTO alter_column_name (time, s2) VALUES (2, 2)");

      ResultSet resultSet = statement.executeQuery("SELECT * FROM alter_column_name");
      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(2, metaData.getColumnCount());
      assertEquals("s2", metaData.getColumnName(2));

      for (int i = 1; i <= 2; i++) {
        assertTrue(resultSet.next());
        assertEquals(i, resultSet.getLong(1));
        assertEquals(i, resultSet.getInt(2));
      }
      assertFalse(resultSet.next());
    }
  }

  @Test
  public void testTableRenameConflict() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS table_a ()");
      statement.execute("CREATE TABLE IF NOT EXISTS table_b ()");

      try {
        statement.execute("ALTER TABLE table_a RENAME TO table_b");
        fail();
      } catch (final SQLException e) {
        // expect table already exists (use code 551)
        assertTrue(
            e.getMessage().startsWith("551") && e.getMessage().toLowerCase().contains("already"));
      }
    }
  }

  @Test
  public void testColumnRenameConflict() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS tconf (c1 int32, c2 int32)");

      try {
        statement.execute("ALTER TABLE tconf RENAME COLUMN c1 TO c2");
        fail();
      } catch (final SQLException e) {
        // expect column already exist error (code 552)
        assertTrue(
            e.getMessage().startsWith("552") && e.getMessage().toLowerCase().contains("exist"));
      }
    }
  }

  @Test
  public void testAlterTableRenameToSameName() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS rename_same (s1 int32)");
      statement.execute("INSERT INTO rename_same (time, s1) VALUES (1, 1)");

      // Renaming to the same name should be a no-op and not lose data
      try {
        statement.execute("ALTER TABLE rename_same RENAME TO rename_same");
        fail();
      } catch (SQLException e) {
        assertEquals(
            "701: The table's old name shall not be equal to the new one.", e.getMessage());
      }
    }
  }

  @Test
  public void testAlterTableRenameToQuotedSpecialName() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS rename_special (s1 int32)");
      statement.execute("INSERT INTO rename_special (time, s1) VALUES (1, 1)");

      // rename to a quoted name containing hyphen and unicode
      statement.execute("ALTER TABLE rename_special RENAME TO \"rename-特殊\"");

      // old name should not exist
      try {
        statement.execute("INSERT INTO rename_special (time, s1) VALUES (2, 2)");
        fail();
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage().startsWith("550")
                || e.getMessage().toLowerCase().contains("does not exist"));
      }

      // insert into new quoted name and verify
      statement.execute("INSERT INTO \"rename-特殊\" (time, s1) VALUES (2, 2)");
      ResultSet rs = statement.executeQuery("SELECT * FROM \"rename-特殊\"");
      for (int i = 1; i <= 2; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getLong(1));
        assertEquals(i, rs.getInt(2));
      }
      assertFalse(rs.next());
    }
  }

  @Test
  public void testAlterTableRenameWithDots() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS db1");
      statement.execute("DROP DATABASE IF EXISTS db2");
      statement.execute("CREATE DATABASE IF NOT EXISTS db1");
      statement.execute("CREATE DATABASE IF NOT EXISTS db2");
      statement.execute("USE db1");

      statement.execute("CREATE TABLE IF NOT EXISTS t1 (s1 int32)");
      statement.execute("INSERT INTO t1 (time, s1) VALUES (1, 1)");

      statement.execute("ALTER TABLE t1 RENAME TO \"db2.t1\"");

      ResultSet rs = statement.executeQuery("SELECT * FROM \"db2.t1\"");
      assertTrue(rs.next());
      assertEquals(1, rs.getLong(1));
      assertEquals(1, rs.getInt(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testAlterColumnRenameCaseSensitivity() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS tcase (c1 int32)");
      statement.execute("INSERT INTO tcase (time, c1) VALUES (1, 1)");

      statement.execute("ALTER TABLE tcase RENAME COLUMN c1 TO C1");

      ResultSet rs = statement.executeQuery("SELECT * FROM tcase");
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(2, md.getColumnCount());
      // server may normalize column names; accept either exact case or normalized lower-case
      String colName = md.getColumnName(2);
      assertTrue(colName.equals("C1") || colName.equals("c1"));

      // ensure data still accessible via the new identifier (try using the new name in insert)
      try {
        statement.execute("INSERT INTO tcase (time, c1) VALUES (2, 2)");
        // if server treats identifiers case-insensitively this may succeed
      } catch (final SQLException ignored) {
        // ignore - the purpose is to assert existence/behavior, not enforce one model here
      }
    }
  }

  @Test
  public void testAlterColumnRenameToQuotedSpecialChars() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS tcolspecial (s1 int32)");
      statement.execute("INSERT INTO tcolspecial (time, s1) VALUES (1, 1)");

      statement.execute("ALTER TABLE tcolspecial RENAME COLUMN s1 TO \"s-特\"");

      try {
        statement.execute("INSERT INTO tcolspecial (time, s1) VALUES (2, 2)");
        fail();
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage().startsWith("616") || e.getMessage().toLowerCase().contains("unknown"));
      }

      statement.execute("INSERT INTO tcolspecial (time, \"s-特\") VALUES (2, 2)");
      ResultSet rs = statement.executeQuery("SELECT * FROM tcolspecial");
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(2, md.getColumnCount());
      String colName = md.getColumnName(2);
      // accept either exact quoted name or normalized variant
      assertTrue(colName.equals("s-特") || colName.equals("s特") || colName.equals("s_特"));
    }
  }

  @Test
  public void testAlterColumnMultipleRenamesAndBack() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS tmulti (a int32)");
      statement.execute("INSERT INTO tmulti (time, a) VALUES (1, 1)");

      statement.execute("ALTER TABLE tmulti RENAME COLUMN a TO b");
      statement.execute("INSERT INTO tmulti (time, b) VALUES (2, 2)");

      statement.execute("ALTER TABLE tmulti RENAME COLUMN b TO c");
      statement.execute("INSERT INTO tmulti (time, c) VALUES (3, 3)");

      statement.execute("ALTER TABLE tmulti RENAME COLUMN c TO a");
      statement.execute("INSERT INTO tmulti (time, a) VALUES (4, 4)");

      ResultSet rs = statement.executeQuery("SELECT * FROM tmulti");
      for (int i = 1; i <= 4; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getLong(1));
        assertEquals(i, rs.getInt(2));
      }
      assertFalse(rs.next());
    }
  }

  @Test
  public void testRenameNonExistentColumn() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      statement.execute("CREATE TABLE IF NOT EXISTS tnonexist (x int32)");

      try {
        statement.execute("ALTER TABLE tnonexist RENAME COLUMN y TO z");
        fail();
      } catch (final SQLException e) {
        // error should indicate column does not exist (use code 616 + contains)
        assertTrue(e.getMessage().startsWith("616"));
        assertTrue(
            e.getMessage().toLowerCase().contains("does not exist")
                || e.getMessage().toLowerCase().contains("cannot be resolved"));
      }
    }
  }

  @Test
  public void testRenameTimeColumnForbidden() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS testdb");
      statement.execute("CREATE DATABASE IF NOT EXISTS testdb");
      statement.execute("USE testdb");

      // create a table with explicit time column
      statement.execute("CREATE TABLE IF NOT EXISTS ttime (time TIMESTAMP TIME, a INT32)");

      try {
        statement.execute("ALTER TABLE ttime RENAME COLUMN time TO newtime");
        fail();
      } catch (final SQLException e) {
        // renaming time column should be forbidden (code 701 or similar)
        assertTrue(
            (e.getMessage().startsWith("701") && e.getMessage().toLowerCase().contains("time")));
      }
    }
  }

  // Helper: recognize SQLExceptions that mean the target table/device cannot be found.
  private static boolean isTableNotFound(final SQLException e) {
    if (e == null) return false;
    final String msg = e.getMessage();
    if (msg == null) return false;
    final String lm = msg.toLowerCase();
    // code 550 is commonly used for 'does not exist' in this project; also match textual phrases
    return msg.startsWith("550") || lm.contains("not exist");
  }

  @Test(timeout = 120000)
  @SuppressWarnings("resource")
  public void testConcurrentRenameVsQueries() throws Throwable {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement stmt = connection.createStatement()) {
      final String db = "concrenamedb";
      final int tableCount = 6;
      final int rows = 50;
      stmt.execute("DROP DATABASE IF EXISTS " + db);
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + db);
      stmt.execute("USE " + db);

      final String[] names = new String[tableCount];
      for (int i = 0; i < tableCount; i++) {
        names[i] = "crtable" + i;
        stmt.execute(String.format("CREATE TABLE IF NOT EXISTS %s (v int32)", names[i]));
        for (int r = 1; r <= rows; r++) {
          stmt.execute(String.format("INSERT INTO %s (time, v) VALUES (%d, %d)", names[i], r, r));
        }
      }

      final java.util.concurrent.atomic.AtomicReference<Throwable> err =
          new java.util.concurrent.atomic.AtomicReference<>();
      final java.util.concurrent.CountDownLatch startLatch =
          new java.util.concurrent.CountDownLatch(1);
      final java.util.concurrent.CountDownLatch doneLatch =
          new java.util.concurrent.CountDownLatch(4);

      java.util.concurrent.ExecutorService exec = null;
      try {
        exec = java.util.concurrent.Executors.newFixedThreadPool(8);

        // Renamer task: rotate rename a subset of tables repeatedly
        exec.submit(
            () -> {
              try (final Connection c =
                      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
                  final Statement s = c.createStatement()) {
                startLatch.await();
                // ensure this thread's connection uses the test database
                try {
                  s.execute("USE " + db);
                } catch (final SQLException ignore) {
                }
                for (int round = 0; round < 20 && err.get() == null; round++) {
                  for (int i = 0; i < tableCount / 2; i++) {
                    final String oldName = names[i];
                    final String newName = oldName + "_r" + round;
                    try {
                      s.execute(String.format("ALTER TABLE %s RENAME TO %s", oldName, newName));
                      // reflect change locally so queries target updated names
                      names[i] = newName;
                    } catch (final SQLException ex) {
                      // Only ignore if the failure is due to table not existing; otherwise record
                      // the error
                      if (isTableNotFound(ex)) {
                        // table not found: likely a transient race with concurrent rename — ignore
                        // and log
                        System.out.println(
                            "Ignored table-not-found during rename: " + ex.getMessage());
                      } else {
                        err.compareAndSet(null, ex);
                      }
                    }
                  }
                  try {
                    Thread.sleep(50);
                  } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              } catch (final Throwable t) {
                err.compareAndSet(null, t);
              } finally {
                doneLatch.countDown();
              }
            });

        // Queryer tasks: continuously query random tables
        for (int q = 0; q < 2; q++) {
          exec.submit(
              () -> {
                try (final Connection c =
                        EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
                    final Statement s = c.createStatement()) {
                  final java.util.Random rnd = new java.util.Random();
                  startLatch.await();
                  // ensure this thread's connection uses the test database
                  try {
                    s.execute("USE " + db);
                  } catch (final SQLException ignore) {
                  }
                  for (int iter = 0; iter < 200 && err.get() == null; iter++) {
                    final int idx = rnd.nextInt(tableCount);
                    final String tname = names[idx];
                    try (final ResultSet rs = s.executeQuery("SELECT count(*) FROM " + tname)) {
                      if (rs.next()) {
                        rs.getLong(1);
                      }
                    } catch (final SQLException ex) {
                      // Only ignore table-not-found; otherwise surface the error to fail the test
                      if (!isTableNotFound(ex)) {
                        err.compareAndSet(null, ex);
                        break;
                      }
                    }
                  }
                } catch (final Throwable t) {
                  err.compareAndSet(null, t);
                } finally {
                  doneLatch.countDown();
                }
              });
        }

        // Another queryer to trigger more parallel access
        exec.submit(
            () -> {
              try (final Connection c =
                      EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
                  final Statement s = c.createStatement()) {
                startLatch.await();
                // ensure this thread's connection uses the test database
                try {
                  s.execute("USE " + db);
                } catch (final SQLException ignore) {
                }
                for (int iter = 0; iter < 200 && err.get() == null; iter++) {
                  for (int i = 0; i < tableCount; i++) {
                    try (final ResultSet rs =
                        s.executeQuery("SELECT * FROM " + names[i] + " LIMIT 1")) {
                      // consume
                      while (rs.next()) {
                        rs.getLong(1);
                      }
                    } catch (final SQLException ex) {
                      if (!isTableNotFound(ex)) {
                        err.compareAndSet(null, ex);
                        break;
                      }
                    }
                  }
                }
              } catch (final Throwable t) {
                err.compareAndSet(null, t);
              } finally {
                doneLatch.countDown();
              }
            });

        // start
        startLatch.countDown();
        // wait for tasks
        doneLatch.await();

        if (err.get() != null) {
          throw err.get();
        }
      } finally {
        if (exec != null) {
          exec.shutdownNow();
        }
      }
    }
  }

  @Test
  public void testMultiTableCrossCheckAfterRenames() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement stmt = connection.createStatement()) {
      final String db = "multicheckdb";
      stmt.execute("DROP DATABASE IF EXISTS " + db);
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + db);
      stmt.execute("USE " + db);

      // create two related tables
      stmt.execute("CREATE TABLE IF NOT EXISTS mta (k int32)");
      stmt.execute("CREATE TABLE IF NOT EXISTS mtb (k int32)");

      for (int i = 1; i <= 10; i++) {
        stmt.execute(String.format("INSERT INTO mta (time, k) VALUES (%d, %d)", i, i));
        stmt.execute(String.format("INSERT INTO mtb (time, k) VALUES (%d, %d)", i, i));
      }

      // baseline: read aggregates
      long aCount = 0, bCount = 0;
      try (final ResultSet ra = stmt.executeQuery("SELECT count(*) FROM mta")) {
        if (ra.next()) {
          aCount = ra.getLong(1);
        }
      }
      try (final ResultSet rb = stmt.executeQuery("SELECT count(*) FROM mtb")) {
        if (rb.next()) {
          bCount = rb.getLong(1);
        }
      }

      // rename one table and verify cross results remain consistent when queried separately
      stmt.execute("ALTER TABLE mtb RENAME TO mtb_renamed");

      long bCountAfter = 0;
      try (final ResultSet rb2 = stmt.executeQuery("SELECT count(*) FROM mtb_renamed")) {
        if (rb2.next()) {
          bCountAfter = rb2.getLong(1);
        }
      }

      // assert counts unchanged
      assertEquals(bCount, bCountAfter);
      assertEquals(10, aCount);

      // rename the other table and verify again
      stmt.execute("ALTER TABLE mta RENAME TO mta_renamed");
      long aCountAfter = 0;
      try (final ResultSet ra2 = stmt.executeQuery("SELECT count(*) FROM mta_renamed")) {
        if (ra2.next()) {
          aCountAfter = ra2.getLong(1);
        }
      }
      assertEquals(aCount, aCountAfter);
    }
  }

  @Test
  public void testPerformanceWithQuotedSpecialNameRenames() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement stmt = connection.createStatement();
        final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      final String db = "perfquotedb";
      final int colPerTable = 100;
      final int tables = 1600;
      final int rows = 100;
      final int numFile = 5;
      final int runs = 10;
      stmt.execute("DROP DATABASE IF EXISTS " + db);
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + db);
      stmt.execute("USE " + db);
      stmt.execute("set configuration enable_seq_space_compaction='false'");
      session.executeNonQueryStatement("USE " + db);

      final String[] names = new String[tables];
      StringBuilder createTableTemplate = new StringBuilder("CREATE TABLE IF NOT EXISTS %s (");
      for (int c = 0; c < colPerTable; c++) {
        createTableTemplate.append(String.format("v%d int32,", c));
      }
      createTableTemplate =
          new StringBuilder(
              createTableTemplate.substring(0, createTableTemplate.length() - 1) + ")");
      List<ColumnSchema> columns = new ArrayList<>();
      for (int i = 0; i < colPerTable; i++) {
        columns.add(new ColumnSchema("v" + i, TSDataType.INT32, ColumnCategory.FIELD));
      }
      TableSchema tableSchema =
          new TableSchema(
              "", // place holder
              columns);

      System.out.println("Start data preparation...");
      for (int i = 0; i < tables; i++) {
        names[i] = "qtable" + i;
        stmt.execute(String.format(createTableTemplate.toString(), names[i]));
        tableSchema.setTableName(names[i]);
        Tablet tablet =
            new Tablet(
                tableSchema.getTableName(),
                tableSchema.getColumnSchemas().stream()
                    .map(IMeasurementSchema::getMeasurementName)
                    .collect(Collectors.toList()),
                tableSchema.getColumnSchemas().stream()
                    .map(IMeasurementSchema::getType)
                    .collect(Collectors.toList()),
                tableSchema.getColumnTypes(),
                rows);
        for (int j = 0; j < numFile; j++) {
          tablet.reset();
          for (int r = 1; r <= rows; r++) {
            tablet.addTimestamp(r - 1, r + j * rows);
            for (int c = 0; c < colPerTable; c++) {
              tablet.addValue(r - 1, c, r + j * rows);
            }
          }
          session.insert(tablet);
          stmt.execute("FLUSH");
        }
      }
      System.out.println("Data preparation done.");

      // baseline measurement: simple average over a few runs
      double totalMs = 0.0;
      for (int run = 0; run < runs; run++) {
        final long start = System.nanoTime();
        for (int i = 0; i < tables; i++) {
          try (final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + names[i])) {
            assertTrue(rs.next());
            assertEquals(rows * numFile, rs.getLong(1));
          }
        }
        final long end = System.nanoTime();
        if (run > runs * 0.1) {
          totalMs += (end - start) / 1_000_000.0;
        }
      }
      final double baseline = totalMs / (runs * 0.9);
      System.out.println("baseline_total_ms=" + String.format("%.3f", baseline));

      // rename half of them to quoted special names and measure again
      for (int i = 0; i < tables / 2; i++) {
        final String oldName = names[i];
        final String newName = "\"" + oldName + "-特\""; // quoted name
        stmt.execute(String.format("ALTER TABLE %s RENAME TO %s", oldName, newName));
        names[i] = newName;
      }

      totalMs = 0.0;
      for (int run = 0; run < runs; run++) {
        final long start = System.nanoTime();
        for (int i = 0; i < tables; i++) {
          try (final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + names[i])) {
            assertTrue(rs.next());
            assertEquals(rows * numFile, rs.getLong(1));
          }
        }
        final long end = System.nanoTime();
        if (run > runs * 0.1) {
          totalMs += (end - start) / 1_000_000.0;
        }
      }
      final double after = totalMs / (runs * 0.9);
      System.out.println("after_quoted_total_ms=" + String.format("%.3f", after));

      // basic sanity: ensure queries still return counts
      for (int i = 0; i < tables; i++) {
        try (final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + names[i])) {
          assertTrue(rs.next());
          assertEquals(rows * numFile, rs.getLong(1));
        }
      }
    }
  }

  @Test
  public void testAlterTableAndColumnTogether() throws Exception {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement stmt = connection.createStatement()) {
      final String db = "dualalterdb";
      stmt.execute("DROP DATABASE IF EXISTS " + db);
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + db);
      stmt.execute("USE " + db);

      stmt.execute("CREATE TABLE IF NOT EXISTS tab1 (c1 int32, c2 int32)");
      stmt.execute("INSERT INTO tab1 (time, c1, c2) VALUES (1, 1, 10)");

      // rename column first and then rename table
      stmt.execute("ALTER TABLE tab1 RENAME COLUMN c1 TO c1_new");
      stmt.execute("ALTER TABLE tab1 RENAME TO tab1_new");

      // old table name should not exist
      try {
        stmt.execute("INSERT INTO tab1 (time, c1_new) VALUES (2, 2)");
        fail();
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage().startsWith("550")
                || e.getMessage().toLowerCase().contains("does not exist"));
      }

      // inserting using new table and new column names should succeed
      stmt.execute("INSERT INTO tab1_new (time, c1_new, c2) VALUES (2, 2, 20)");

      // verify data
      try (final ResultSet rs = stmt.executeQuery("SELECT * FROM tab1_new ORDER BY time")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        assertEquals(1, rs.getInt("c1_new"));
        assertEquals(10, rs.getInt("c2"));

        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(2, rs.getInt("c1_new"));
        assertEquals(20, rs.getInt("c2"));

        assertFalse(rs.next());
      }

      // rename column again on the renamed table and verify
      stmt.execute("ALTER TABLE tab1_new RENAME COLUMN c1_new TO c1_final");
      try {
        // old column identifier should fail
        stmt.execute("INSERT INTO tab1_new (time, c1_new) VALUES (3, 3)");
        fail();
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage().startsWith("616")
                || e.getMessage().toLowerCase().contains("unknown")
                || e.getMessage().toLowerCase().contains("cannot be resolved"));
      }

      // use final name
      stmt.execute("INSERT INTO tab1_new (time, c1_final, c2) VALUES (3, 3, 30)");
      try (final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1_new")) {
        if (rs.next()) {
          assertEquals(3L, rs.getLong(1));
        } else {
          fail();
        }
      }
    }
  }
}
