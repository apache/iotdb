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

package com.timecho.iotdb.relational.it.schema;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWritableViewIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().getConfig().getCommonConfig().setRestrictObjectLimit(true);
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultSchemaRegionGroupNumPerDatabase(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testWritableViewShowDevicesAndCountUseAliases() throws Exception {
    final String database = "writable_view_device_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table table0(region_id STRING TAG, plant_id STRING TAG, device_id STRING TAG, "
                + "model STRING ATTRIBUTE, temperature FLOAT FIELD, humidity DOUBLE FIELD)");
        statement.execute(
            "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) "
                + "values('1', 'magnolia', '3', 'A', 37.6, 111.1)");
        statement.execute(
            "insert into table0(region_id, plant_id, device_id, model, temperature, humidity) "
                + "values('2', 'orchid', '4', 'B', 18.5, 22.2)");
        statement.execute(
            "create writable view writable_view_alias as select region_id as area, "
                + "plant_id as plant, model as label from table0");

        try (final ResultSet resultSet =
            statement.executeQuery(
                "show devices from writable_view_alias where area = '2' and label = 'B'")) {
          final ResultSetMetaData metaData = resultSet.getMetaData();
          final Set<String> actualHeaders = new HashSet<>();
          for (int i = 1; i <= metaData.getColumnCount(); i++) {
            actualHeaders.add(metaData.getColumnLabel(i));
          }
          assertEquals(new HashSet<>(Arrays.asList("area", "plant", "label")), actualHeaders);
          assertFalse(actualHeaders.contains("region_id"));
          assertTrue(resultSet.next());
          assertEquals("2", resultSet.getString("area"));
          assertEquals("orchid", resultSet.getString("plant"));
          assertEquals("B", resultSet.getString("label"));
          assertFalse(resultSet.next());
        }

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "count devices from writable_view_alias where area = '2' and label = 'B'"),
            "count(devices),",
            Collections.singleton("1,"));

        statement.execute(
            "select * from table0 where region_id = '2' and plant_id in ('orchid', '5') "
                + "and device_id = '4'");

        try (final ResultSet resultSet =
            statement.executeQuery(
                "show devices from writable_view_alias where area = '2' "
                    + "and plant in ('orchid', '5') and label = 'B'")) {
          assertTrue(resultSet.next());
          assertEquals("2", resultSet.getString("area"));
          assertEquals("orchid", resultSet.getString("plant"));
          assertEquals("B", resultSet.getString("label"));
          assertFalse(resultSet.next());
        }
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewTable() throws Exception {
    final String database = "writable_view_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "a tag comment 'source_a', "
                + "b attribute comment 'source_b', "
                + "c int32 comment 'source_c'"
                + ") comment 'source table' with (ttl=100)");
        statement.execute(
            "create writable view writable_view as select "
                + "a comment 'view_a', "
                + "b, "
                + "c comment 'view_c' "
                + "from source_table comment 'view comment' "
                + "with (ttl=200, schema_cascade=true)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,200,USING,view comment,BASE TABLE,null,",
                    "writable_view,200,USING,view comment,WRITABLE VIEW,source_table,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show writable views"),
            "TableName,TTL(ms),",
            Collections.singleton("writable_view,200,"));
        TestUtils.assertResultSetEqual(
            statement.executeQuery("show views"),
            "TableName,TTL(ms),",
            Collections.singleton("writable_view,200,"));
        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tree views"),
            "TableName,TTL(ms),",
            Collections.emptySet());

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "a,STRING,TAG,USING,view_a,",
                    "b,STRING,ATTRIBUTE,USING,source_b,",
                    "c,INT32,FIELD,USING,view_c,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "a,STRING,TAG,USING,view_a,a,",
                    "b,STRING,ATTRIBUTE,USING,source_b,b,",
                    "c,INT32,FIELD,USING,view_c,c,")));

        String showCreateViewSql;
        try (final ResultSet resultSet = statement.executeQuery("show create view writable_view")) {
          assertTrue(resultSet.next());
          assertEquals("writable_view", resultSet.getString(1));
          showCreateViewSql = resultSet.getString(2);
          assertTrue(
              showCreateViewSql.startsWith("CREATE WRITABLE VIEW \"writable_view\" AS SELECT "));
          assertTrue(showCreateViewSql.contains("\"a\" AS \"a\" COMMENT 'view_a'"));
          assertTrue(showCreateViewSql.contains("\"b\" AS \"b\" COMMENT 'source_b'"));
          assertTrue(showCreateViewSql.contains("\"c\" AS \"c\" COMMENT 'view_c'"));
          assertTrue(showCreateViewSql.contains("FROM \"source_table\""));
          assertTrue(showCreateViewSql.contains("COMMENT 'view comment'"));
          assertTrue(showCreateViewSql.contains("WITH (ttl=200, schema_cascade=true)"));
          assertFalse(resultSet.next());
        }

        try (final ResultSet resultSet =
            statement.executeQuery("show create table writable_view")) {
          assertTrue(resultSet.next());
          assertEquals("writable_view", resultSet.getString(1));
          assertEquals(showCreateViewSql, resultSet.getString(2));
          assertFalse(resultSet.next());
        }

        statement.execute("alter view writable_view set properties schema_cascade=false");
        statement.execute("alter view writable_view set properties ttl=300");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,200,USING,view comment,BASE TABLE,null,",
                    "writable_view,300,USING,view comment,WRITABLE VIEW,source_table,")));

        try (final ResultSet resultSet = statement.executeQuery("show create view writable_view")) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.getString(2).contains("WITH (ttl=300, schema_cascade=false)"));
          assertFalse(resultSet.next());
        }

        statement.execute(
            "create table source_table_without_explicit_comment("
                + "a tag comment 'source_a_without_explicit_comment', "
                + "b attribute comment 'source_b_without_explicit_comment', "
                + "c int32 comment 'source_c_without_explicit_comment'"
                + ") comment 'source_table_without_explicit_comment' with (ttl=100)");
        statement.execute(
            "create writable view writable_view_without_explicit_comment as select * "
                + "from source_table_without_explicit_comment with (ttl=200, schema_cascade=true)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,200,USING,view comment,BASE TABLE,null,",
                    "writable_view,300,USING,view comment,WRITABLE VIEW,source_table,",
                    "source_table_without_explicit_comment,200,USING,"
                        + "source_table_without_explicit_comment,BASE TABLE,null,",
                    "writable_view_without_explicit_comment,200,USING,"
                        + "source_table_without_explicit_comment,WRITABLE VIEW,"
                        + "source_table_without_explicit_comment,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table_without_explicit_comment details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "a,STRING,TAG,USING,source_a_without_explicit_comment,",
                    "b,STRING,ATTRIBUTE,USING,source_b_without_explicit_comment,",
                    "c,INT32,FIELD,USING,source_c_without_explicit_comment,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view_without_explicit_comment details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "a,STRING,TAG,USING,source_a_without_explicit_comment,a,",
                    "b,STRING,ATTRIBUTE,USING,source_b_without_explicit_comment,b,",
                    "c,INT32,FIELD,USING,source_c_without_explicit_comment,c,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewMetadataOperationsWithoutCascade() throws Exception {
    final String database = "writable_view_metadata_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id tag, "
                + "model attribute, "
                + "temperature int32, "
                + "pressure int32"
                + ") with (ttl=100)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id, "
                + "model, "
                + "temperature "
                + "from source_table "
                + "with (schema_cascade=false)");

        try {
          statement.execute("alter view writable_view add column humidity int32 field");
          fail();
        } catch (final SQLException e) {
          assertTrue(e.getMessage(), e.getMessage().contains("must already exist in source table"));
        }

        try {
          statement.execute("alter view writable_view add column pressure string attribute");
          fail();
        } catch (final SQLException e) {
          assertTrue(e.getMessage(), e.getMessage().contains("must match source column"));
        }

        try {
          statement.execute(
              "alter view writable_view add column pressure_alias int32 field from pressure");
          fail();
        } catch (final SQLException e) {
          assertTrue(
              e.getMessage(), e.getMessage().contains("does not support tree view FROM syntax"));
        }

        statement.execute("alter view writable_view add column pressure int32 field");

        try {
          statement.execute("alter table writable_view alter column pressure set data type int64");
          fail();
        } catch (final SQLException e) {
          assertTrue(
              e.getMessage().contains("ALTER COLUMN DATA TYPE requires schema_cascade=true"));
        }

        statement.execute("alter view writable_view rename column pressure to pressure_alias");
        statement.execute("alter view writable_view rename to writable_view_renamed");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "device_id,STRING,TAG,USING,null,",
                    "model,STRING,ATTRIBUTE,USING,null,",
                    "temperature,INT32,FIELD,USING,null,",
                    "pressure,INT32,FIELD,USING,null,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view_renamed details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "device_id,STRING,TAG,USING,null,device_id,",
                    "model,STRING,ATTRIBUTE,USING,null,model,",
                    "temperature,INT32,FIELD,USING,null,temperature,",
                    "pressure_alias,INT32,FIELD,USING,null,pressure,")));

        try (final ResultSet resultSet =
            statement.executeQuery("show create view writable_view_renamed")) {
          assertTrue(resultSet.next());
          final String sql = resultSet.getString(2);
          assertTrue(sql.contains("CREATE WRITABLE VIEW \"writable_view_renamed\""));
          assertTrue(sql.contains("\"pressure\" AS \"pressure_alias\""));
          assertTrue(sql.contains("FROM \"source_table\""));
          assertFalse(resultSet.next());
        }

        statement.execute("alter view writable_view_renamed drop column pressure_alias");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view_renamed details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "device_id,STRING,TAG,USING,null,device_id,",
                    "model,STRING,ATTRIBUTE,USING,null,model,",
                    "temperature,INT32,FIELD,USING,null,temperature,")));

        try (final ResultSet resultSet =
            statement.executeQuery("show create view writable_view_renamed")) {
          assertTrue(resultSet.next());
          final String sql = resultSet.getString(2);
          assertFalse(sql.contains("pressure_alias"));
          assertFalse(resultSet.next());
        }

        statement.execute(
            "alter table writable_view_renamed add column pressure AS pressure_alias");
        statement.execute("drop view writable_view_renamed");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            Collections.singleton("source_table,100,USING,null,BASE TABLE,null,"));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewMetadataOperationsWithCascade() throws Exception {
    final String database = "writable_view_metadata_cascade_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id tag, "
                + "model attribute, "
                + "temperature int32"
                + ") with (ttl=100)");
        statement.execute(
            "create writable view writable_view as select * from source_table "
                + "with (schema_cascade=true)");

        statement.execute("alter view writable_view add column pressure int32 field");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "device_id,STRING,TAG,USING,null,",
                    "model,STRING,ATTRIBUTE,USING,null,",
                    "temperature,INT32,FIELD,USING,null,",
                    "pressure,INT32,FIELD,USING,null,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "device_id,STRING,TAG,USING,null,device_id,",
                    "model,STRING,ATTRIBUTE,USING,null,model,",
                    "temperature,INT32,FIELD,USING,null,temperature,",
                    "pressure,INT32,FIELD,USING,null,pressure,")));

        statement.execute("alter table writable_view alter column pressure set data type int64");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "device_id,STRING,TAG,USING,null,",
                    "model,STRING,ATTRIBUTE,USING,null,",
                    "temperature,INT32,FIELD,USING,null,",
                    "pressure,INT64,FIELD,USING,null,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "device_id,STRING,TAG,USING,null,device_id,",
                    "model,STRING,ATTRIBUTE,USING,null,model,",
                    "temperature,INT32,FIELD,USING,null,temperature,",
                    "pressure,INT64,FIELD,USING,null,pressure,")));

        statement.execute("alter view writable_view drop column pressure");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe source_table details"),
            "ColumnName,DataType,Category,Status,Comment,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,",
                    "device_id,STRING,TAG,USING,null,",
                    "model,STRING,ATTRIBUTE,USING,null,",
                    "temperature,INT32,FIELD,USING,null,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "device_id,STRING,TAG,USING,null,device_id,",
                    "model,STRING,ATTRIBUTE,USING,null,model,",
                    "temperature,INT32,FIELD,USING,null,temperature,")));

        statement.execute("drop view writable_view");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            Collections.emptySet());
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewSetPropertiesUsesPreviousSchemaCascade() throws Exception {
    final String database = "writable_view_property_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id tag, "
                + "temperature int32"
                + ") with (ttl=100)");
        statement.execute(
            "create writable view writable_view as select * from source_table "
                + "with (ttl=200, schema_cascade=false)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,100,USING,null,BASE TABLE,null,",
                    "writable_view,200,USING,null,WRITABLE VIEW,source_table,")));

        statement.execute("alter view writable_view set properties ttl=300, schema_cascade=true");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,100,USING,null,BASE TABLE,null,",
                    "writable_view,300,USING,null,WRITABLE VIEW,source_table,")));

        statement.execute("alter view writable_view set properties ttl=400, schema_cascade=false");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            new HashSet<>(
                Arrays.asList(
                    "source_table,400,USING,null,BASE TABLE,null,",
                    "writable_view,400,USING,null,WRITABLE VIEW,source_table,")));

        try (final ResultSet resultSet = statement.executeQuery("show create view writable_view")) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.getString(2).contains("WITH (ttl=400, schema_cascade=false)"));
          assertFalse(resultSet.next());
        }
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewWriteAndQuery() throws Exception {
    final String database = "writable_view_write_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id string tag, "
                + "site string tag, "
                + "model string attribute, "
                + "temperature int32 field, "
                + "humidity double field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "site as area, "
                + "model as label, "
                + "temperature as temp, "
                + "humidity "
                + "from source_table");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show writable views"),
            "TableName,TTL(ms),",
            Collections.singleton("writable_view,INF,"));
        TestUtils.assertResultSetEqual(
            statement.executeQuery("show views"),
            "TableName,TTL(ms),",
            Collections.singleton("writable_view,INF,"));

        statement.execute(
            "insert into writable_view(time, dev, area, label, temp, humidity) values "
                + "(1, 'd1', 'north', 'A', 10, 1.1), "
                + "(2, 'd1', 'north', 'A', 11, 1.2), "
                + "(1, 'd2', 'south', 'B', 20, 2.1)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, dev, area, label, temp, humidity from writable_view "
                    + "order by time, dev"),
            "time,dev,area,label,temp,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d1,north,A,10,1.1,",
                    "1970-01-01T00:00:00.001Z,d2,south,B,20,2.1,",
                    "1970-01-01T00:00:00.002Z,d1,north,A,11,1.2,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, device_id, site, model, temperature, humidity from source_table "
                    + "order by time, device_id"),
            "time,device_id,site,model,temperature,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d1,north,A,10,1.1,",
                    "1970-01-01T00:00:00.001Z,d2,south,B,20,2.1,",
                    "1970-01-01T00:00:00.002Z,d1,north,A,11,1.2,")));

        statement.execute(
            "update writable_view set label = dev where dev = 'd1' and area = 'north'");
        statement.execute("clear all cache");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "show devices from writable_view where dev = 'd1' and area = 'north'"),
            "dev,area,label,",
            Collections.singleton("d1,north,d1,"));

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "show devices from source_table where device_id = 'd1' and site = 'north'"),
            "device_id,site,model,",
            Collections.singleton("d1,north,d1,"));

        statement.execute("delete from writable_view where time <= 1 and dev = 'd1'");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, dev, area, label, temp, humidity from writable_view "
                    + "order by time, dev"),
            "time,dev,area,label,temp,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d2,south,B,20,2.1,",
                    "1970-01-01T00:00:00.002Z,d1,north,d1,11,1.2,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, device_id, site, model, temperature, humidity from source_table "
                    + "order by time, device_id"),
            "time,device_id,site,model,temperature,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d2,south,B,20,2.1,",
                    "1970-01-01T00:00:00.002Z,d1,north,d1,11,1.2,")));

        statement.execute(
            "delete devices from writable_view where dev = 'd2' and area = 'south' and label = 'B'");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show devices from writable_view"),
            "dev,area,label,",
            Collections.singleton("d1,north,d1,"));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("count devices from source_table"),
            "count(devices),",
            Collections.singleton("1,"));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewTTLFiltersQuery() throws Exception {
    final String database = "writable_view_ttl_query_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table(" + "device_id string tag, " + "temperature int32 field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "temperature as temp "
                + "from source_table with (ttl=60000, schema_cascade=false)");

        final long expiredTime = System.currentTimeMillis() - 120000;
        statement.execute(
            "insert into source_table(time, device_id, temperature) values ("
                + expiredTime
                + ", 'd1', 10)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("select count(temperature) from source_table"),
            "_col0,",
            Collections.singleton("1,"));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("select dev, temp from writable_view"),
            "dev,temp,",
            Collections.emptySet());
        TestUtils.assertResultSetEqual(
            statement.executeQuery("select count(temp) from writable_view"),
            "_col0,",
            Collections.singleton("0,"));

        try (final ResultSet resultSet =
            statement.executeQuery("select last(temp) from writable_view")) {
          assertTrue(resultSet.next());
          assertEquals(0, resultSet.getInt(1));
          assertTrue(resultSet.wasNull());
          assertFalse(resultSet.next());
        }
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewGroupByAliasColumn() throws Exception {
    final String database = "writable_view_group_by_alias_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        createWritableViewWriteSchema(statement, database);

        statement.execute(
            "insert into writable_view(time, dev, area, label, temp, humidity) values "
                + "(1, 'd1', 'north', 'A', 10, 1.1), "
                + "(2, 'd1', 'north', 'A', 11, 1.2), "
                + "(3, 'd2', 'south', 'B', 20, 2.1)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select dev, count(temp) as temp_count from writable_view "
                    + "group by dev order by dev"),
            "dev,temp_count,",
            new HashSet<>(Arrays.asList("d1,2,", "d2,1,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewInsertWithMissingSourceTagFillsNull() throws Exception {
    final String database = "writable_view_missing_tag_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id string tag, "
                + "site string tag, "
                + "model string attribute, "
                + "temperature int32 field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "model as label, "
                + "temperature as temp "
                + "from source_table");

        statement.execute(
            "insert into writable_view(time, dev, label, temp) values (1, 'd1', 'A', 10)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("select time, dev, label, temp from writable_view"),
            "time,dev,label,temp,",
            Collections.singleton("1970-01-01T00:00:00.001Z,d1,A,10,"));

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, device_id, site, model, temperature from source_table"),
            "time,device_id,site,model,temperature,",
            Collections.singleton("1970-01-01T00:00:00.001Z,d1,null,A,10,"));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewInsertSelect() throws Exception {
    final String database = "writable_view_insert_select_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id string tag, "
                + "site string tag, "
                + "model string attribute, "
                + "temperature int32 field, "
                + "humidity double field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "site as area, "
                + "model as label, "
                + "temperature as temp, "
                + "humidity "
                + "from source_table");
        statement.execute(
            "insert into source_table(time, device_id, site, model, temperature, humidity) values "
                + "(1, 'd1', 'north', 'A', 10, 1.1), "
                + "(2, 'd2', 'south', 'B', 20, 2.2)");

        statement.execute(
            "insert into writable_view "
                + "select time + 100, device_id, site, model, temperature + 1, humidity + 0.5 "
                + "from source_table");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, dev, area, label, temp, humidity from writable_view "
                    + "where time >= 101 order by time, dev"),
            "time,dev,area,label,temp,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.101Z,d1,north,A,11,1.6,",
                    "1970-01-01T00:00:00.102Z,d2,south,B,21,2.7,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, device_id, site, model, temperature, humidity from source_table "
                    + "where time >= 101 order by time, device_id"),
            "time,device_id,site,model,temperature,humidity,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.101Z,d1,north,A,11,1.6,",
                    "1970-01-01T00:00:00.102Z,d2,south,B,21,2.7,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testProjectionWritableViewInsertSelectRequiresExplicitColumns() throws Exception {
    final String database = "writable_view_projection_insert_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id string tag, "
                + "site string tag, "
                + "model string attribute, "
                + "temperature int32 field, "
                + "humidity double field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "model as label, "
                + "temperature as temp "
                + "from source_table");
        statement.execute(
            "insert into source_table(time, device_id, site, model, temperature, humidity) values "
                + "(1, 'd1', 'north', 'A', 10, 1.1)");

        try {
          statement.execute(
              "insert into writable_view "
                  + "select time + 100, device_id, model, temperature from source_table");
          fail("projection writable view insert without column list should fail");
        } catch (final SQLException e) {
          assertTrue(
              e.getMessage(),
              e.getMessage()
                  .contains(
                      "INSERT into writable view without column list requires every source table "
                          + "column to be mapped"));
        }

        statement.execute(
            "insert into writable_view(time, dev, label, temp) "
                + "select time + 100, device_id, model, temperature from source_table");

        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, dev, label, temp from writable_view where time >= 101"),
            "time,dev,label,temp,",
            Collections.singleton("1970-01-01T00:00:00.101Z,d1,A,10,"));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewKeepsMetadataAfterSourceTableDropped() throws Exception {
    final String database = "writable_view_dropped_source_table_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table(device_id string tag, temperature int32 field)");
        statement.execute(
            "create writable view writable_view as select device_id as dev, "
                + "temperature as temp from source_table with (schema_cascade=true)");

        statement.execute("insert into writable_view(time, dev, temp) values (1, 'd1', 10)");
        statement.execute("drop table source_table");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            Collections.singleton("writable_view,INF,USING,null,WRITABLE VIEW,source_table,"));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "dev,STRING,TAG,USING,null,device_id,",
                    "temp,INT32,FIELD,USING,null,temperature,")));

        try {
          statement.execute("select temp from writable_view");
          fail("select on writable view whose source table was dropped should fail");
        } catch (final SQLException e) {
          assertMissingSourceTableException(e, database);
        }

        try {
          statement.execute("insert into writable_view(time, dev, temp) values (2, 'd1', 11)");
          fail("insert on writable view whose source table was dropped should fail");
        } catch (final SQLException e) {
          assertMissingSourceTableException(e, database);
        }

        statement.execute("drop view writable_view");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables details"),
            "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
            Collections.emptySet());
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewRejectsDuplicateSourceColumnMapping() throws Exception {
    final String database = "writable_view_duplicate_source_column_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table(device_id string tag, temperature int32 field)");

        try {
          statement.execute(
              "create writable view writable_view as select "
                  + "temperature as temp1, temperature as temp2 from source_table");
          fail("duplicate source-column mapping should fail on create writable view");
        } catch (final SQLException e) {
          assertTrue(
              e.getMessage(), e.getMessage().contains("cannot be from the same source column"));
        }

        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, temperature as temp from source_table");

        try {
          statement.execute("alter view writable_view add column temperature as temp2");
          fail("duplicate source-column mapping should fail on add column");
        } catch (final SQLException e) {
          assertTrue(
              e.getMessage(), e.getMessage().contains("cannot be from the same source column"));
        }
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testRenameSourceTableReferencedByWritableViewFails() throws Exception {
    final String database = "writable_view_rename_source_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table(device_id string tag, temperature int32 field)");
        statement.execute(
            "create writable view writable_view as select device_id as dev, "
                + "temperature as temp from source_table");
        statement.execute("insert into writable_view(time, dev, temp) values (1, 'd1', 10)");

        try {
          statement.execute("alter table source_table rename to source_table2");
          fail("renaming a base table should fail");
        } catch (final SQLException e) {
          assertEquals("701: The renaming for base table is currently unsupported", e.getMessage());
        }

        statement.execute("insert into writable_view(time, dev, temp) values (2, 'd2', 20)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("select time, dev, temp from writable_view order by time"),
            "time,dev,temp,",
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d1,10,", "1970-01-01T00:00:00.002Z,d2,20,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables"),
            "TableName,TTL(ms),",
            new HashSet<>(Arrays.asList("source_table,INF,", "writable_view,INF,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testRenameWritableViewWithCascadeSkipsSourceTableRename() throws Exception {
    final String database = "writable_view_rename_cascade_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table(device_id string tag, temperature int32 field)");
        statement.execute(
            "create writable view writable_view as select device_id as dev, "
                + "temperature as temp from source_table with (schema_cascade=true)");
        statement.execute("alter view writable_view rename to writable_view_renamed");
        statement.execute(
            "insert into writable_view_renamed(time, dev, temp) values (1, 'd1', 10)");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("show tables"),
            "TableName,TTL(ms),",
            new HashSet<>(Arrays.asList("source_table,INF,", "writable_view_renamed,INF,")));
        TestUtils.assertResultSetEqual(
            statement.executeQuery(
                "select time, device_id, temperature from source_table order by time"),
            "time,device_id,temperature,",
            Collections.singleton("1970-01-01T00:00:00.001Z,d1,10,"));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewDdlSkipsMissingSourceTableCascade() throws Exception {
    final String database = "writable_view_missing_source_ddl_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_cascade(device_id string tag, temperature int32 field)");
        statement.execute(
            "create writable view view_cascade as select device_id as dev, "
                + "temperature as temp from source_cascade with (schema_cascade=true)");
        statement.execute(
            "create table source_no_cascade(device_id string tag, pressure int32 field)");
        statement.execute(
            "create writable view view_no_cascade as select device_id as dev, "
                + "pressure from source_no_cascade with (schema_cascade=false)");
        statement.execute(
            "create table source_missing_column("
                + "device_id string tag, "
                + "temperature int32 field, "
                + "status string attribute)");
        statement.execute(
            "create writable view view_missing_column as select "
                + "device_id as dev, "
                + "temperature as temp, "
                + "status "
                + "from source_missing_column with (schema_cascade=true)");

        statement.execute("drop table source_cascade");
        statement.execute("drop table source_no_cascade");
        statement.execute("alter table source_missing_column drop column temperature");

        statement.execute("alter view view_cascade add column status string attribute");
        statement.execute("alter view view_cascade drop column temp");
        statement.execute("alter view view_no_cascade add column humidity double field");
        statement.execute("alter view view_no_cascade drop column pressure");
        statement.execute("alter view view_missing_column drop column temp");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe view_cascade details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "dev,STRING,TAG,USING,null,device_id,",
                    "status,STRING,ATTRIBUTE,USING,null,status,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe view_no_cascade details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "dev,STRING,TAG,USING,null,device_id,",
                    "humidity,DOUBLE,FIELD,USING,null,humidity,")));

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe view_missing_column details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "dev,STRING,TAG,USING,null,device_id,",
                    "status,STRING,ATTRIBUTE,USING,null,status,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewInsertRowsUsingTableSession() throws Exception {
    final String database = "writable_view_session_rows_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      try {
        createWritableViewWriteSchema(statement, database);

        session.executeNonQueryStatement("use " + database);
        session.executeNonQueryStatement(
            "insert into writable_view(time, dev, area, label, temp, humidity) values "
                + "(1, 'd1', 'north', 'A', 10, 1.1), "
                + "(2, 'd2', 'south', 'B', 20, 2.2)");
        session.executeNonQueryStatement("flush");

        assertWritableViewWriteRows(
            statement,
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.001Z,d1,north,A,10,1.1,",
                    "1970-01-01T00:00:00.002Z,d2,south,B,20,2.2,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewInsertTabletUsingTableSession() throws Exception {
    final String database = "writable_view_session_tablet_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      try {
        createWritableViewWriteSchema(statement, database);

        session.executeNonQueryStatement("use " + database);
        final Tablet tablet =
            new Tablet(
                "writable_view",
                Arrays.asList("dev", "area", "label", "temp", "humidity"),
                Arrays.asList(
                    TSDataType.STRING,
                    TSDataType.STRING,
                    TSDataType.STRING,
                    TSDataType.INT32,
                    TSDataType.DOUBLE),
                Arrays.asList(
                    ColumnCategory.TAG,
                    ColumnCategory.TAG,
                    ColumnCategory.ATTRIBUTE,
                    ColumnCategory.FIELD,
                    ColumnCategory.FIELD),
                2);
        tablet.addTimestamp(0, 3);
        tablet.addValue("dev", 0, "d3");
        tablet.addValue("area", 0, "west");
        tablet.addValue("label", 0, "C");
        tablet.addValue("temp", 0, 30);
        tablet.addValue("humidity", 0, 3.3);
        tablet.addTimestamp(1, 4);
        tablet.addValue("dev", 1, "d4");
        tablet.addValue("area", 1, "east");
        tablet.addValue("label", 1, "D");
        tablet.addValue("temp", 1, 40);
        tablet.addValue("humidity", 1, 4.4);
        session.insert(tablet);
        session.executeNonQueryStatement("flush");

        assertWritableViewWriteRows(
            statement,
            new HashSet<>(
                Arrays.asList(
                    "1970-01-01T00:00:00.003Z,d3,west,C,30,3.3,",
                    "1970-01-01T00:00:00.004Z,d4,east,D,40,4.4,")));
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  @Test
  public void testWritableViewKeepsDroppedSourceColumnMetadataButFailsOnUse() throws Exception {
    final String database = "writable_view_missing_source_column_db";
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database " + database);
        statement.execute("use " + database);
        statement.execute(
            "create table source_table("
                + "device_id string tag, "
                + "model string attribute, "
                + "temperature int32 field, "
                + "humidity double field)");
        statement.execute(
            "create writable view writable_view as select "
                + "device_id as dev, "
                + "model as label, "
                + "temperature as temp, "
                + "humidity "
                + "from source_table with (schema_cascade=false)");

        statement.execute("alter table source_table drop column temperature");

        TestUtils.assertResultSetEqual(
            statement.executeQuery("describe writable_view details"),
            "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
            new HashSet<>(
                Arrays.asList(
                    "time,TIMESTAMP,TIME,USING,null,time,",
                    "dev,STRING,TAG,USING,null,device_id,",
                    "label,STRING,ATTRIBUTE,USING,null,model,",
                    "temp,INT32,FIELD,USING,null,temperature,",
                    "humidity,DOUBLE,FIELD,USING,null,humidity,")));

        try (final ResultSet resultSet = statement.executeQuery("show create view writable_view")) {
          Assert.assertTrue(resultSet.next());
          Assert.assertTrue(resultSet.getString(2).contains("\"temperature\" AS \"temp\""));
          Assert.assertFalse(resultSet.next());
        }

        try {
          statement.execute("select temp from writable_view");
          fail("select on dropped writable_view source column should fail");
        } catch (final IoTDBSQLException e) {
          assertMissingSourceColumnException(e, database);
        }

        try {
          statement.execute(
              "insert into writable_view(time, dev, label, temp, humidity) values "
                  + "(1, 'd1', 'A', 10, 1.1)");
          fail("insert on dropped writable_view source column should fail");
        } catch (final IoTDBSQLException e) {
          assertMissingSourceColumnException(e, database);
        }
      } finally {
        dropDatabaseQuietly(statement, database);
      }
    }
  }

  private static void assertMissingSourceTableException(
      final SQLException exception, final String database) {
    final String message = exception.getMessage();
    Assert.assertTrue(
        message,
        message.contains(
                "The source table '"
                    + database
                    + ".source_table' of writable view '"
                    + database
                    + ".writable_view' does not exist.")
            || message.contains("Table '" + database + ".source_table' does not exist"));
  }

  private static void assertMissingSourceColumnException(
      final IoTDBSQLException exception, final String database) {
    assertEquals(TSStatusCode.COLUMN_NOT_EXISTS.getStatusCode(), exception.getErrorCode());
    Assert.assertTrue(
        exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "Column 'temp' in writable view '"
                    + database
                    + ".writable_view' cannot be resolved because source column 'temperature' "
                    + "does not exist in source table '"
                    + database
                    + ".source_table'."));
  }

  private static void createWritableViewWriteSchema(
      final Statement statement, final String database) throws SQLException {
    statement.execute("create database " + database);
    statement.execute("use " + database);
    statement.execute(
        "create table source_table("
            + "device_id string tag, "
            + "site string tag, "
            + "model string attribute, "
            + "temperature int32 field, "
            + "humidity double field)");
    statement.execute(
        "create writable view writable_view as select "
            + "device_id as dev, "
            + "site as area, "
            + "model as label, "
            + "temperature as temp, "
            + "humidity "
            + "from source_table");
  }

  private static void assertWritableViewWriteRows(
      final Statement statement, final Set<String> expectedRows) throws SQLException {
    TestUtils.assertResultSetEqual(
        statement.executeQuery(
            "select time, dev, area, label, temp, humidity from writable_view order by time, dev"),
        "time,dev,area,label,temp,humidity,",
        expectedRows);

    TestUtils.assertResultSetEqual(
        statement.executeQuery(
            "select time, device_id, site, model, temperature, humidity from source_table "
                + "order by time, device_id"),
        "time,device_id,site,model,temperature,humidity,",
        expectedRows);
  }

  private static void dropDatabaseQuietly(final Statement statement, final String... databases) {
    for (final String database : databases) {
      try {
        statement.execute("drop database if exists " + database);
      } catch (final SQLException ignored) {
        // Best-effort cleanup to keep the shared IT environment isolated across test cases.
      }
    }
  }
}
