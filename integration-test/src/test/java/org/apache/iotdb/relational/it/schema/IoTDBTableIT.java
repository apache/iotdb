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

import org.apache.tsfile.enums.TSDataType;
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
import java.util.List;

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
          "create table test1.table1(region_id STRING TAG, plant_id STRING TAG, device_id TAG, model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE) comment 'test' with (TTL='INF')");

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

      tableNames = new String[] {"table3", "table2"};
      ttls = new String[] {"3000000", "6600000"};

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

      ttls = new String[] {"INF", "6600000"};
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
      statement.execute("COMMENT ON COLUMN table2.region_id IS '重庆'");
      statement.execute("COMMENT ON COLUMN table2.region_id IS NULL");
      statement.execute("COMMENT ON COLUMN test2.table2.time IS 'recent'");
      statement.execute("COMMENT ON COLUMN test2.table2.region_id IS ''");

      columnNames = new String[] {"time", "region_id", "plant_id", "temperature", "speed"};
      dataTypes = new String[] {"TIMESTAMP", "STRING", "STRING", "FLOAT", "DOUBLE"};
      categories = new String[] {"TIME", "TAG", "TAG", "FIELD", "FIELD"};
      statuses = new String[] {"USING", "USING", "USING", "USING", "USING"};
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
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTableAuth() throws SQLException {
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password'");
      adminStmt.execute("create database db");
      adminStmt.execute("use db");
      adminStmt.execute("create table test (a tag, b attribute, c int32)");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("select * from db.test");
          });
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT SELECT ON db.test to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      try (final ResultSet resultSet = userStmt.executeQuery("SHOW DATABASES")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        Assert.assertTrue(resultSet.next());
        if (resultSet.getString(1).equals("information_schema")) {
          assertTrue(resultSet.next());
        }
        assertEquals("db", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      userStmt.execute("select * from db.test");
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT DROP ON DATABASE DB to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
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
                  final List<Tablet.ColumnCategory> columnTypes =
                      Arrays.asList(
                          Tablet.ColumnCategory.TAG,
                          Tablet.ColumnCategory.ATTRIBUTE,
                          Tablet.ColumnCategory.FIELD);

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
}
