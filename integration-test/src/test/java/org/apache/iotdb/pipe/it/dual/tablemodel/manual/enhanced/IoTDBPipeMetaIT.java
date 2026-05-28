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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.enhanced;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TStartPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeMetaIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testTableSync() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("extractor.table-name", "t.*[0-9]");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      final String dbName = "test";
      TableModelUtils.createDatabase(senderEnv, dbName, 300);

      TestUtils.executeNonQueries(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "create table table1(a tag, b attribute, c int32) with (ttl=3000)",
              "alter table table1 add column d int64",
              "alter table table1 drop column c",
              "alter table table1 set properties ttl=default",
              "insert into table1 (a, b, d) values(1, 1, 1)",
              "create table noTransferTable(a tag, b attribute, c int32) with (ttl=3000)"),
          null);

      TableModelUtils.createDatabase(senderEnv, "noTransferDatabase", 300);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables from test",
          "TableName,TTL(ms),",
          Collections.singleton("table1,300,"),
          dbName);

      // Test devices
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.singleton("1,1,"), dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "insert into table1 (a, b, d) values(1, 2, 1)",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.singleton("1,2,"), dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "update table1 set b = '3'", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.singleton("1,3,"), dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "delete from table1", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from table1", "time,a,b,d,", Collections.emptySet(), dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "delete devices from table1 where a = '1'",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.emptySet(), dbName);

      // Will not include no-transfer table
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show tables from test",
          "TableName,TTL(ms),",
          Collections.singleton("table1,300,"),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "desc table1",
          "ColumnName,DataType,Category,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,",
                  "a,STRING,TAG,",
                  "b,STRING,ATTRIBUTE,",
                  "d,INT64,FIELD,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "drop table table1", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables from test",
          "TableName,TTL(ms),",
          Collections.emptySet(),
          dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "drop database test", null);

      // Will not include no-transfer database
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"),
          (String) null);
    }
  }

  @Test
  public void testWritableViewMetaSync() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      final String dbName = "test";
      TableModelUtils.createDatabase(senderEnv, dbName, 100);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create table table1(a tag, b attribute, c int32) comment 'source table' with (ttl=100)",
          null);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create writable view view1 as select * from table1 comment 'view comment' with (ttl=200, schema_cascade=true)",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from test",
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,200,USING,view comment,BASE TABLE,null,",
                  "view1,200,USING,view comment,WRITABLE VIEW,table1,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view1 add column d int64 field",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe view1 details",
          "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,time,",
                  "a,STRING,TAG,USING,null,a,",
                  "b,STRING,ATTRIBUTE,USING,null,b,",
                  "c,INT32,FIELD,USING,null,c,",
                  "d,INT64,FIELD,USING,null,d,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe table1 details",
          "ColumnName,DataType,Category,Status,Comment,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,",
                  "a,STRING,TAG,USING,null,",
                  "b,STRING,ATTRIBUTE,USING,null,",
                  "c,INT32,FIELD,USING,null,",
                  "d,INT64,FIELD,USING,null,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter table view1 alter column d set data type double",
          null);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view1 rename column d to d1",
          null);
      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "alter view view1 rename to view2", null);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "comment on column view2.a is 'view_a_comment'",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from test",
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,200,USING,view comment,BASE TABLE,null,",
                  "view2,200,USING,view comment,WRITABLE VIEW,table1,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe view2 details",
          "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,time,",
                  "a,STRING,TAG,USING,view_a_comment,a,",
                  "b,STRING,ATTRIBUTE,USING,null,b,",
                  "c,INT32,FIELD,USING,null,c,",
                  "d1,DOUBLE,FIELD,USING,null,d,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe table1 details",
          "ColumnName,DataType,Category,Status,Comment,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,",
                  "a,STRING,TAG,USING,view_a_comment,",
                  "b,STRING,ATTRIBUTE,USING,null,",
                  "c,INT32,FIELD,USING,null,",
                  "d,DOUBLE,FIELD,USING,null,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "alter view view2 drop column d1", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe view2 details",
          "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,time,",
                  "a,STRING,TAG,USING,view_a_comment,a,",
                  "b,STRING,ATTRIBUTE,USING,null,b,",
                  "c,INT32,FIELD,USING,null,c,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe table1 details",
          "ColumnName,DataType,Category,Status,Comment,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,",
                  "a,STRING,TAG,USING,view_a_comment,",
                  "b,STRING,ATTRIBUTE,USING,null,",
                  "c,INT32,FIELD,USING,null,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view2 set properties schema_cascade=false",
          null);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view2 set properties ttl=300",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from test",
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,200,USING,view comment,BASE TABLE,null,",
                  "view2,300,USING,view comment,WRITABLE VIEW,table1,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe view2 details",
          "ColumnName,DataType,Category,Status,Comment,OriginalColumnName,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,time,",
                  "a,STRING,TAG,USING,view_a_comment,a,",
                  "b,STRING,ATTRIBUTE,USING,null,b,",
                  "c,INT32,FIELD,USING,null,c,")),
          dbName);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "describe table1 details",
          "ColumnName,DataType,Category,Status,Comment,",
          new HashSet<>(
              Arrays.asList(
                  "time,TIMESTAMP,TIME,USING,null,",
                  "a,STRING,TAG,USING,view_a_comment,",
                  "b,STRING,ATTRIBUTE,USING,null,",
                  "c,INT32,FIELD,USING,null,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "drop view view2", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from test",
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          Collections.singleton("table1,200,USING,view comment,BASE TABLE,null,"),
          dbName);
    }
  }

  @Test
  public void testWritableViewMetaSyncWithReceiverCascadeMismatch() throws Exception {
    final String dbName = "cascade_mismatch";

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      createTableModelPipe(client, "testPipe", dbName, null);

      TableModelUtils.createDatabase(senderEnv, dbName, 100);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create table table1(a tag, b attribute, c int32) with (ttl=100)",
          null);
      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create writable view view1 as select * from table1 with (schema_cascade=true)",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + dbName,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,100,USING,null,BASE TABLE,null,",
                  "view1,100,USING,null,WRITABLE VIEW,table1,")),
          dbName);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "alter view view1 set properties schema_cascade=false",
          null);

      TestUtils.executeNonQuery(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view1 set properties ttl=300",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + dbName,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,300,USING,null,BASE TABLE,null,",
                  "view1,300,USING,null,WRITABLE VIEW,table1,")),
          dbName);
    }
  }

  @Test
  public void testWritableViewMetaSyncWithTablePattern() throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final String viewOnlyDb = "view_only";
      createTableModelPipe(client, "viewOnlyPipe", viewOnlyDb, "view.*");

      TableModelUtils.createDatabase(receiverEnv, viewOnlyDb, 100);
      TestUtils.executeNonQuery(
          viewOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          receiverEnv,
          "create table table1(a tag, b attribute, c int32) with (ttl=100)",
          null);
      TableModelUtils.createDatabase(senderEnv, viewOnlyDb, 100);
      TestUtils.executeNonQuery(
          viewOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create table table1(a tag, b attribute, c int32) with (ttl=100)",
          null);
      TestUtils.executeNonQuery(
          viewOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create writable view view1 as select * from table1 with (schema_cascade=true)",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + viewOnlyDb,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,100,USING,null,BASE TABLE,null,",
                  "view1,100,USING,null,WRITABLE VIEW,table1,")),
          viewOnlyDb);

      TestUtils.executeNonQuery(
          viewOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view1 set properties ttl=200, schema_cascade=false",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + viewOnlyDb,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          new HashSet<>(
              Arrays.asList(
                  "table1,100,USING,null,BASE TABLE,null,",
                  "view1,200,USING,null,WRITABLE VIEW,table1,")),
          viewOnlyDb);

      final String sourceOnlyDb = "source_only";
      createTableModelPipe(client, "sourceOnlyPipe", sourceOnlyDb, "table.*");

      TableModelUtils.createDatabase(senderEnv, sourceOnlyDb, 100);
      TestUtils.executeNonQuery(
          sourceOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create table table1(a tag, b attribute, c int32) with (ttl=100)",
          null);
      TestUtils.executeNonQuery(
          sourceOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "create writable view view1 as select * from table1 with (schema_cascade=true)",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + sourceOnlyDb,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          Collections.singleton("table1,100,USING,null,BASE TABLE,null,"),
          sourceOnlyDb);

      TestUtils.executeNonQuery(
          sourceOnlyDb,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "alter view view1 set properties ttl=300",
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables details from " + sourceOnlyDb,
          "TableName,TTL(ms),Status,Comment,TableType,OriginalTableName,",
          Collections.singleton("table1,300,USING,null,BASE TABLE,null,"),
          sourceOnlyDb);
    }
  }

  @Test
  public void testNoTree() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create database root.test",
              "alter database root.test with schema_region_group_num=2, data_region_group_num=3",
              "create timeSeries root.test.d1.s1 int32",
              "insert into root.test.d1 (s1) values (1)"));

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.emptySet());
    }
  }

  @Test
  public void testNoTable() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "data.delete");
      extractorAttributes.put("extractor.capture.tree", "true");
      extractorAttributes.put("extractor.capture.table", "false");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      final String dbName = "test";
      TableModelUtils.createDatabase(senderEnv, dbName, 300);

      TestUtils.executeNonQueries(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "create table table1(a tag, b attribute, c int32) with (ttl=3000)",
              "alter table table1 add column d int64",
              "alter table table1 drop column b",
              "alter table table1 set properties ttl=default"),
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"),
          dbName);
    }
  }

  @Test
  public void testAuth() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user testUser 'password!@#$!@#$'", "grant all on root.** to user testUser"),
          null);

      TestUtils.executeNonQueries(
          null,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "grant create on db.tb to user testUser",
              "grant drop on database test to user testUser"),
          null);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("extractor.table-name", "t.*[0-9]");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          client.startPipeExtended(new TStartPipeReq("testPipe").setIsTableModel(true)).getCode());

      TestUtils.executeNonQuery(
          null,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "grant alter on any to user testUser with grant option",
          null);

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "list privileges of user testUser",
          "Role,Scope,Privileges,GrantOption,",
          new HashSet<>(
              Arrays.asList(
                  ",,MANAGE_USER,false,",
                  ",,MANAGE_ROLE,false,",
                  ",,MAINTAIN,false,",
                  ",*.*,ALTER,true,",
                  ",test.*,DROP,false,")),
          (String) null);
    }
  }

  @Test
  public void testValidation() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe test1 with source ('inclusion'='schema.table') with sink ('ip'='%s', 'port'='%s')",
              receiverIp, receiverPort));

      // Test tree parameters
      try {
        statement.execute(
            String.format(
                "create pipe test2 with source ('inclusion'='auth, schema.timeseries') with sink ('ip'='%s', 'port'='%s')",
                receiverIp, receiverPort));
        fail();
      } catch (final SQLException e) {
        assertEquals("1107: The 'inclusion' string contains illegal path.", e.getMessage());
      }
    }

    try (final Connection connection = senderEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(
              "create pipe test3 with source ('inclusion'='schema.timeseries') with sink ('ip'='%s', 'port'='%s')",
              receiverIp, receiverPort));

      // Test tree parameters
      try {
        statement.execute(
            String.format(
                "create pipe test4 with source ('inclusion'='auth, schema.table') with sink ('ip'='%s', 'port'='%s')",
                receiverIp, receiverPort));
        fail();
      } catch (final SQLException e) {
        assertEquals("1107: The 'inclusion' string contains illegal path.", e.getMessage());
      }
    }
  }

  @Test
  public void testAttributeSync() {
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create database test",
            "use test",
            "create table table1(a tag, b attribute, c attribute, d int32)",
            "insert into table1 (time, a, b, c, d) values(1, 1, null, 1, 1), (2, 2, 2, null, 2)"),
        BaseEnv.TABLE_SQL_DIALECT);

    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database test",
            "use test",
            "create table table1(a tag, b attribute, c attribute, d int32)",
            "insert into table1 (time, a, b, c, d) values(1, 1, 1, null, 1), (2, 2, null, 2, 2)",
            String.format(
                "create pipe a2b with source ('inclusion'='schema') with sink ('node-urls'='%s')",
                receiverEnv.getDataNodeWrapperList().get(0).getIpAndPortString())),
        BaseEnv.TABLE_SQL_DIALECT);

    TestUtils.assertDataAlwaysOnEnv(
        receiverEnv,
        "show devices from table1",
        "a,b,c,",
        new HashSet<>(Arrays.asList("1,1,1,", "2,2,2,")),
        "test");
  }

  private void createTableModelPipe(
      final SyncConfigNodeIServiceClient client,
      final String pipeName,
      final String databaseName,
      final String tableNamePattern)
      throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("extractor.inclusion", "all");
    extractorAttributes.put("extractor.capture.tree", "false");
    extractorAttributes.put("extractor.capture.table", "true");
    extractorAttributes.put("extractor.database-name", databaseName);
    if (tableNamePattern != null) {
      extractorAttributes.put("extractor.table-name", tableNamePattern);
    }
    extractorAttributes.put("user", "root");

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.ip", receiverDataNode.getIp());
    connectorAttributes.put("connector.port", Integer.toString(receiverDataNode.getPort()));

    final TSStatus status =
        client.createPipe(
            new TCreatePipeReq(pipeName, connectorAttributes)
                .setExtractorAttributes(extractorAttributes)
                .setProcessorAttributes(processorAttributes));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }
}
