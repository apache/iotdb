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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDBColumnHeaders;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.showDBDetailsColumnHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDatabaseIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testManageDatabase() {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {

      // create
      statement.execute("create database test with (ttl='INF')");

      // create duplicated database without IF NOT EXISTS
      try {
        statement.execute("create database test");
        fail("create database test shouldn't succeed because test already exists");
      } catch (final SQLException e) {
        assertEquals("501: Database test already exists", e.getMessage());
      }

      // create duplicated database with IF NOT EXISTS
      statement.execute("create database IF NOT EXISTS test");

      // alter non-exist
      try {
        statement.execute("alter database test1 set properties ttl='INF'");
        fail("alter database test1 shouldn't succeed because test does not exist");
      } catch (final SQLException e) {
        assertEquals("500: Database test1 doesn't exist", e.getMessage());
      }

      statement.execute("alter database if exists test1 set properties ttl='INF'");
      statement.execute("alter database test set properties ttl=default");

      String[] databaseNames = new String[] {"test"};
      String[] TTLs = new String[] {"INF"};
      int[] schemaReplicaFactors = new int[] {1};
      int[] dataReplicaFactors = new int[] {1};
      int[] timePartitionInterval = new int[] {604800000};

      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("information_schema")) {
            continue;
          }
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      final int[] schemaRegionGroupNum = new int[] {0};
      final int[] dataRegionGroupNum = new int[] {0};
      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBDetailsColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBDetailsColumnHeaders.size(); i++) {
          assertEquals(
              showDBDetailsColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("information_schema")) {
            continue;
          }
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          assertEquals(schemaRegionGroupNum[cnt], resultSet.getInt(6));
          assertEquals(dataRegionGroupNum[cnt], resultSet.getInt(7));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      // use
      statement.execute("use test");

      // use nonexistent database
      try {
        statement.execute("use test1");
        fail("use test1 shouldn't succeed because test1 doesn't exist");
      } catch (final SQLException e) {
        assertEquals("500: Unknown database test1", e.getMessage());
      }

      // drop
      statement.execute("drop database test");
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        // Information_schema
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
      }

      // drop nonexistent database without IF EXISTS
      try {
        statement.execute("drop database test");
        fail("drop database test shouldn't succeed because test doesn't exist");
      } catch (final SQLException e) {
        assertEquals("500: Database test doesn't exist", e.getMessage());
      }

      // drop nonexistent database with IF EXISTS
      statement.execute("drop database IF EXISTS test");

      // Test create database with properties
      statement.execute(
          "create database test_prop with (ttl=300, schema_region_group_num=DEFAULT, time_partition_interval=100000)");
      databaseNames = new String[] {"test_prop"};
      TTLs = new String[] {"300"};
      timePartitionInterval = new int[] {100000};

      // show
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        int cnt = 0;
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(showDBColumnHeaders.size(), metaData.getColumnCount());
        for (int i = 0; i < showDBColumnHeaders.size(); i++) {
          assertEquals(showDBColumnHeaders.get(i).getColumnName(), metaData.getColumnName(i + 1));
        }
        while (resultSet.next()) {
          if (resultSet.getString(1).equals("information_schema")) {
            continue;
          }
          assertEquals(databaseNames[cnt], resultSet.getString(1));
          assertEquals(TTLs[cnt], resultSet.getString(2));
          assertEquals(schemaReplicaFactors[cnt], resultSet.getInt(3));
          assertEquals(dataReplicaFactors[cnt], resultSet.getInt(4));
          assertEquals(timePartitionInterval[cnt], resultSet.getLong(5));
          cnt++;
        }
        assertEquals(databaseNames.length, cnt);
      }

      try {
        statement.execute("create database test_prop_2 with (non_exist_prop=DEFAULT)");
        fail(
            "create database test_prop_2 shouldn't succeed because the property key does not exist.");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("Unsupported database property key: non_exist_prop"));
      }

      // create with strange name
      try {
        statement.execute("create database 1test");
        fail(
            "create database 1test shouldn't succeed because 1test is not a legal identifier; identifiers must not start with a digit; surround the identifier with double quotes");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }

      statement.execute("create database \"1test\"");
      statement.execute("use \"1test\"");
      statement.execute("drop database \"1test\"");

      try {
        statement.execute("create database 1");
        fail("create database 1 shouldn't succeed because 1 is not a legal identifier");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '1'"));
      }

      statement.execute("create database \"1\"");
      statement.execute("use \"1\"");
      statement.execute("drop database \"1\"");

      try {
        statement.execute("create database a.b");
        fail("create database a.b shouldn't succeed because a.b is not a legal identifier");
      } catch (final SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("mismatched input '.'"));
      }

      // Test length limitation
      statement.execute(
          "create database thisDatabaseLengthIsPreciselySixtyFourThusItCanBeNormallyCreated");

      try {
        statement.execute(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated");
        fail(
            "create database thisDatabaseLengthHasExceededSixtyFourThusItCantBeNormallyCreated shouldn't succeed because it's length has exceeded 64");
      } catch (final SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("the length of database name shall not exceed 64"));
      }

    } catch (final SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDatabaseWithSpecificCharacters() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("create database \"````x.\"");
        fail("create database ````x. shouldn't succeed because it contains '.'");
      } catch (final SQLException e) {
        assertEquals(
            "509: ````x. is not a legal path, because the database name can only contain english or chinese characters, numbers, backticks and underscores.",
            e.getMessage());
      }

      try {
        statement.execute("create database \"#\"");
        fail("create database # shouldn't succeed because it contains illegal character '#'");
      } catch (final SQLException e) {
        assertEquals(
            "509: # is not a legal path, because the database name can only contain english or chinese characters, numbers, backticks and underscores.",
            e.getMessage());
      }

      statement.execute("create database \"````x\"");

      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        assertTrue(resultSet.next());
        assertEquals("````x", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("information_schema", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      statement.execute("use \"````x\"");

      statement.execute("create table table0 (a tag, b attribute, c int32)");

      statement.execute("desc table0");
      statement.execute("desc \"````x\".table0");

      statement.execute("show tables");
      statement.execute("show tables from \"````x\"");

      statement.execute("insert into table0 (time, a, b, c) values(0, '1', '2', 3)");
      statement.execute("insert into \"````x\".table0 (time, a, b, c) values(1, '1', '2', 3)");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select a, b, c from \"````x\".table0 where time = 0"),
          "a,b,c,",
          Collections.singleton("1,2,3,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0"),
          "a,b,",
          Collections.singleton("1,2,"));

      statement.execute("update \"````x\".table0 set b = '4'");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from table0"),
          "a,b,",
          Collections.singleton("1,4,"));
    }
  }

  @Test
  public void testInformationSchema() throws SQLException {
    // Use a normal user to test visibility
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password'");
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      // Test unsupported write plans
      final Set<String> writeSQLs =
          new HashSet<>(
              Arrays.asList(
                  "create database information_schema",
                  "drop database information_schema",
                  "create table information_schema.tableA ()",
                  "alter table information_schema.tableA add column a id",
                  "alter table information_schema.tableA set properties ttl=default",
                  "insert into information_schema.tables (database) values('db')",
                  "update information_schema.tables set status='RUNNING'"));

      for (final String writeSQL : writeSQLs) {
        try {
          statement.execute(writeSQL);
          fail("information_schema does not support write");
        } catch (final SQLException e) {
          assertEquals(
              "701: The database 'information_schema' can only be queried", e.getMessage());
        }
      }

      statement.execute("use information_schema");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("show databases"),
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show tables"),
          "TableName,TTL(ms),",
          Arrays.asList(
              "columns,INF,",
              "config_nodes,INF,",
              "configurations,INF,",
              "data_nodes,INF,",
              "databases,INF,",
              "functions,INF,",
              "keywords,INF,",
              "models,INF,",
              "nodes,INF,",
              "pipe_plugins,INF,",
              "pipes,INF,",
              "queries,INF,",
              "regions,INF,",
              "subscriptions,INF,",
              "tables,INF,",
              "topics,INF,",
              "views,INF,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc databases"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "database,STRING,TAG,",
                  "ttl(ms),STRING,ATTRIBUTE,",
                  "schema_replication_factor,INT32,ATTRIBUTE,",
                  "data_replication_factor,INT32,ATTRIBUTE,",
                  "time_partition_interval,INT64,ATTRIBUTE,",
                  "schema_region_group_num,INT32,ATTRIBUTE,",
                  "data_region_group_num,INT32,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc tables"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "database,STRING,TAG,",
                  "table_name,STRING,TAG,",
                  "ttl(ms),STRING,ATTRIBUTE,",
                  "status,STRING,ATTRIBUTE,",
                  "comment,STRING,ATTRIBUTE,",
                  "table_type,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc columns"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "database,STRING,TAG,",
                  "table_name,STRING,TAG,",
                  "column_name,STRING,TAG,",
                  "datatype,STRING,ATTRIBUTE,",
                  "category,STRING,ATTRIBUTE,",
                  "status,STRING,ATTRIBUTE,",
                  "comment,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc queries"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "query_id,STRING,TAG,",
                  "start_time,TIMESTAMP,ATTRIBUTE,",
                  "datanode_id,INT32,ATTRIBUTE,",
                  "elapsed_time,FLOAT,ATTRIBUTE,",
                  "statement,STRING,ATTRIBUTE,",
                  "user,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc pipes"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "id,STRING,TAG,",
                  "creation_time,TIMESTAMP,ATTRIBUTE,",
                  "state,STRING,ATTRIBUTE,",
                  "pipe_source,STRING,ATTRIBUTE,",
                  "pipe_processor,STRING,ATTRIBUTE,",
                  "pipe_sink,STRING,ATTRIBUTE,",
                  "exception_message,STRING,ATTRIBUTE,",
                  "remaining_event_count,INT64,ATTRIBUTE,",
                  "estimated_remaining_seconds,DOUBLE,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc pipe_plugins"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "plugin_name,STRING,TAG,",
                  "plugin_type,STRING,ATTRIBUTE,",
                  "class_name,STRING,ATTRIBUTE,",
                  "plugin_jar,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc topics"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList("topic_name,STRING,TAG,", "topic_configs,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc subscriptions"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "topic_name,STRING,TAG,",
                  "consumer_group_name,STRING,TAG,",
                  "subscribed_consumers,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc views"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "database,STRING,TAG,",
                  "table_name,STRING,TAG,",
                  "view_definition,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc models"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "model_id,STRING,TAG,",
                  "model_type,STRING,ATTRIBUTE,",
                  "state,STRING,ATTRIBUTE,",
                  "configs,STRING,ATTRIBUTE,",
                  "notes,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc functions"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "function_table,STRING,TAG,",
                  "function_type,STRING,ATTRIBUTE,",
                  "class_name(udf),STRING,ATTRIBUTE,",
                  "state,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc configurations"),
          "ColumnName,DataType,Category,",
          new HashSet<>(Arrays.asList("variable,STRING,TAG,", "value,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc keywords"),
          "ColumnName,DataType,Category,",
          new HashSet<>(Arrays.asList("word,STRING,TAG,", "reserved,INT32,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc nodes"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "node_id,INT32,TAG,",
                  "node_type,STRING,ATTRIBUTE,",
                  "status,STRING,ATTRIBUTE,",
                  "internal_address,STRING,ATTRIBUTE,",
                  "internal_port,INT32,ATTRIBUTE,",
                  "version,STRING,ATTRIBUTE,",
                  "build_info,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc config_nodes"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "node_id,INT32,TAG,",
                  "config_consensus_port,INT32,ATTRIBUTE,",
                  "role,STRING,ATTRIBUTE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("desc data_nodes"),
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "node_id,INT32,TAG,",
                  "data_region_num,INT32,ATTRIBUTE,",
                  "schema_region_num,INT32,ATTRIBUTE,",
                  "rpc_address,STRING,ATTRIBUTE,",
                  "rpc_port,INT32,ATTRIBUTE,",
                  "mpp_port,INT32,ATTRIBUTE,",
                  "data_consensus_port,INT32,ATTRIBUTE,",
                  "schema_consensus_port,INT32,ATTRIBUTE,")));

      // Only root user is allowed
      Assert.assertThrows(SQLException.class, () -> statement.execute("select * from regions"));
      Assert.assertThrows(SQLException.class, () -> statement.execute("select * from pipes"));
      Assert.assertThrows(SQLException.class, () -> statement.execute("select * from topics"));
      Assert.assertThrows(
          SQLException.class, () -> statement.execute("select * from subscriptions"));
      Assert.assertThrows(
          SQLException.class, () -> statement.execute("select * from configurations"));
      Assert.assertThrows(SQLException.class, () -> statement.execute("select * from nodes"));
      Assert.assertThrows(
          SQLException.class, () -> statement.execute("select * from config_nodes"));
      Assert.assertThrows(SQLException.class, () -> statement.execute("select * from data_nodes"));

      // No auth needed
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select * from pipe_plugins where plugin_name = 'IOTDB-THRIFT-SINK'"),
          "plugin_name,plugin_type,class_name,plugin_jar,",
          Collections.singleton(
              "IOTDB-THRIFT-SINK,Builtin,org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftConnector,null,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select model_id from information_schema.models where model_type = 'BUILT_IN_FORECAST'"),
          "model_id,",
          new HashSet<>(
              Arrays.asList(
                  "_STLForecaster,",
                  "_NaiveForecaster,",
                  "_sundial,",
                  "_HoltWinters,",
                  "_ExponentialSmoothing,",
                  "_ARIMA,")));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select distinct(function_type) from information_schema.functions"),
          "function_type,",
          new HashSet<>(
              Arrays.asList(
                  "built-in scalar function,",
                  "built-in aggregate function,",
                  "built-in table function,")));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select * from information_schema.keywords where reserved > 0 limit 1"),
          "word,reserved,",
          Collections.singleton("AINODES,1,"));
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      // Test table query
      statement.execute("use information_schema");

      statement.execute("create database test");
      statement.execute(
          "create table test.test (a tag, b attribute, c int32 comment 'turbine') comment 'test'");
      statement.execute(
          "CREATE VIEW test.view_table (tag1 STRING TAG,tag2 STRING TAG,s11 INT32 FIELD,s3 INT32 FIELD FROM s2) RESTRICT WITH (ttl=100) AS root.\"a\".**");

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select * from databases"),
          "database,ttl(ms),schema_replication_factor,data_replication_factor,time_partition_interval,schema_region_group_num,data_region_group_num,",
          new HashSet<>(
              Arrays.asList(
                  "information_schema,INF,null,null,null,null,null,",
                  "test,INF,1,1,604800000,0,0,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("show devices from tables where status = 'USING'"),
          "database,table_name,ttl(ms),status,comment,table_type,",
          new HashSet<>(
              Arrays.asList(
                  "information_schema,databases,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,tables,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,columns,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,queries,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,regions,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,topics,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,pipe_plugins,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,pipes,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,subscriptions,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,views,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,models,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,functions,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,configurations,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,keywords,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,nodes,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,config_nodes,INF,USING,null,SYSTEM VIEW,",
                  "information_schema,data_nodes,INF,USING,null,SYSTEM VIEW,",
                  "test,test,INF,USING,test,BASE TABLE,",
                  "test,view_table,100,USING,null,VIEW FROM TREE,")));
      TestUtils.assertResultSetEqual(
          statement.executeQuery("count devices from tables where status = 'USING'"),
          "count(devices),",
          Collections.singleton("19,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select * from columns where table_name = 'queries' or database = 'test'"),
          "database,table_name,column_name,datatype,category,status,comment,",
          new HashSet<>(
              Arrays.asList(
                  "information_schema,queries,query_id,STRING,TAG,USING,null,",
                  "information_schema,queries,start_time,TIMESTAMP,ATTRIBUTE,USING,null,",
                  "information_schema,queries,datanode_id,INT32,ATTRIBUTE,USING,null,",
                  "information_schema,queries,elapsed_time,FLOAT,ATTRIBUTE,USING,null,",
                  "information_schema,queries,statement,STRING,ATTRIBUTE,USING,null,",
                  "information_schema,queries,user,STRING,ATTRIBUTE,USING,null,",
                  "test,test,time,TIMESTAMP,TIME,USING,null,",
                  "test,test,a,STRING,TAG,USING,null,",
                  "test,test,b,STRING,ATTRIBUTE,USING,null,",
                  "test,test,c,INT32,FIELD,USING,turbine,",
                  "test,view_table,time,TIMESTAMP,TIME,USING,null,",
                  "test,view_table,tag1,STRING,TAG,USING,null,",
                  "test,view_table,tag2,STRING,TAG,USING,null,",
                  "test,view_table,s11,INT32,FIELD,USING,null,",
                  "test,view_table,s3,INT32,FIELD,USING,null,")));

      statement.execute(
          "create pipe a2b with source('double-living'='true') with sink ('sink'='write-back-sink')");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select id from pipes where creation_time > 0"),
          "id,",
          Collections.singleton("a2b,"));
      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select * from pipe_plugins where plugin_name = 'IOTDB-THRIFT-SINK'"),
          "plugin_name,plugin_type,class_name,plugin_jar,",
          Collections.singleton(
              "IOTDB-THRIFT-SINK,Builtin,org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.iotdb.thrift.IoTDBThriftConnector,null,"));

      statement.execute("create topic tp with ('start-time'='2025-01-13T10:03:19.229+08:00')");
      TestUtils.assertResultSetEqual(
          statement.executeQuery("select * from topics where topic_name = 'tp'"),
          "topic_name,topic_configs,",
          Collections.singleton(
              "tp,{__system.sql-dialect=table, start-time=2025-01-13T10:03:19.229+08:00},"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select * from views"),
          "database,table_name,view_definition,",
          Collections.singleton(
              "test,view_table,CREATE VIEW \"view_table\" (\"tag1\" STRING TAG,\"tag2\" STRING TAG,\"s11\" INT32 FIELD,\"s3\" INT32 FIELD FROM \"s2\") RESTRICT WITH (ttl=100) AS root.\"a\".**,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select model_id from information_schema.models where model_type = 'BUILT_IN_FORECAST'"),
          "model_id,",
          new HashSet<>(
              Arrays.asList(
                  "_sundial,",
                  "_STLForecaster,",
                  "_NaiveForecaster,",
                  "_HoltWinters,",
                  "_ARIMA,",
                  "_ExponentialSmoothing,")));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select distinct(function_type) from information_schema.functions"),
          "function_type,",
          new HashSet<>(
              Arrays.asList(
                  "built-in scalar function,",
                  "built-in aggregate function,",
                  "built-in table function,")));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select value from information_schema.configurations where variable = 'TimestampPrecision'"),
          "value,",
          Collections.singleton("ms,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery(
              "select * from information_schema.keywords where reserved > 0 limit 1"),
          "word,reserved,",
          Collections.singleton("AINODES,1,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select distinct(status) from information_schema.nodes"),
          "status,",
          Collections.singleton("Running,"));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select count(*) from information_schema.config_nodes"),
          "_col0,",
          Collections.singleton(EnvFactory.getEnv().getConfigNodeWrapperList().size() + ","));

      TestUtils.assertResultSetEqual(
          statement.executeQuery("select data_region_num from information_schema.data_nodes"),
          "data_region_num,",
          Collections.singleton("0,"));
    }
  }

  @Test
  public void testMixedDatabase() throws SQLException {
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use test");
      statement.execute("create table table1(id1 tag, s1 string)");
      statement.execute("insert into table1 values(0, 'd1', null), (1,'d1', 1)");
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root.test");
      statement.execute(
          "alter database root.test WITH SCHEMA_REGION_GROUP_NUM=2, DATA_REGION_GROUP_NUM=3");
      statement.execute("insert into root.test.d1 (s1) values(1)");
      statement.execute("drop database root.test");
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("use test");
      // Avoid clearing table cache
      statement.execute("select * from table1");

      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS")) {
        assertTrue(resultSet.next());
        assertEquals("information_schema", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("test", resultSet.getString(1));
        assertFalse(resultSet.next());
      }

      // Test adjustMaxRegionGroupNum
      statement.execute(
          "create table table2(region_id STRING TAG, plant_id STRING TAG, color STRING ATTRIBUTE, temperature FLOAT FIELD, speed DOUBLE FIELD)");
      statement.execute(
          "insert into table2(region_id, plant_id, color, temperature, speed) values(1, 1, 1, 1, 1)");

      statement.execute("create database test1");
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root.test");
    }

    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database test");
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      TestUtils.assertResultSetSize(statement.executeQuery("show databases"), 1);
    }
  }

  @Test
  public void testDBAuth() throws SQLException {
    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("create user test 'password'");
      adminStmt.execute("create database db");
      adminStmt.execute(
          "create pipe a2b with source('double-living'='true') with sink ('sink'='write-back-sink')");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      TestUtils.assertResultSetEqual(
          userStmt.executeQuery("show databases"),
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"));
      TestUtils.assertResultSetEqual(
          userStmt.executeQuery("select * from information_schema.databases"),
          "database,ttl(ms),schema_replication_factor,data_replication_factor,time_partition_interval,schema_region_group_num,data_region_group_num,",
          Collections.singleton("information_schema,INF,null,null,null,null,null,"));
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT SELECT ON DATABASE DB to user test");

      // Information_schema does not support grant & revoke
      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("GRANT SELECT ON DATABASE information_schema to user test");
          });

      Assert.assertThrows(
          SQLException.class,
          () -> {
            adminStmt.execute("REVOKE SELECT ON information_schema.tables from user test");
          });
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
        assertEquals("db", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        assertEquals("information_schema", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
      }

      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("alter database db set properties ttl=6600000");
          });

      Assert.assertThrows(
          SQLException.class,
          () -> {
            userStmt.execute("drop database db");
          });
    }

    try (final Connection adminCon = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement adminStmt = adminCon.createStatement()) {
      adminStmt.execute("GRANT DROP ON ANY to user test");
    }

    try (final Connection userCon =
            EnvFactory.getEnv().getConnection("test", "password", BaseEnv.TABLE_SQL_DIALECT);
        final Statement userStmt = userCon.createStatement()) {
      userStmt.execute("drop database db");
    }
  }
}
