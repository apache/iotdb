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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "create table table1(a id, b attribute, c int32) with (ttl=3000)",
              "alter table table1 add column d int64",
              "alter table table1 drop column c",
              "alter table table1 set properties ttl=default",
              "insert into table1 (a, b, d) values(1, 1, 1)",
              "create table noTransferTable(a id, b attribute, c int32) with (ttl=3000)"))) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "insert into table1 (a, b) values(1, 2)")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.singleton("1,2,"), dbName);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "update table1 set b = '3'")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "show devices from table1", "a,b,", Collections.singleton("1,3,"), dbName);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "delete from table1")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "select * from table1", "a,b,d,", Collections.emptySet(), dbName);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "delete devices from table1 where a = '1'")) {
        return;
      }

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
                  "a,STRING,ID,",
                  "b,STRING,ATTRIBUTE,",
                  "d,INT64,MEASUREMENT,")),
          dbName);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "drop table table1")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show tables from test",
          "TableName,TTL(ms),",
          Collections.emptySet(),
          dbName);

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName, BaseEnv.TABLE_SQL_DIALECT, senderEnv, "drop database test")) {
        return;
      }

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

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.test",
              "alter database root.test with schema_region_group_num=2, data_region_group_num=3",
              "create timeSeries root.test.d1.s1 int32",
              "insert into root.test.d1 (s1) values (1)"))) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "create table table1(a id, b attribute, c int32) with (ttl=3000)",
              "alter table table1 add column d int64",
              "alter table table1 drop column b",
              "alter table table1 set properties ttl=default"))) {
        return;
      }

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

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create user testUser 'password'", "grant all on root.** to user testUser"))) {
        return;
      }

      final String dbName = "test";
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          Arrays.asList(
              "grant create on db.tb to user testUser",
              "grant drop on database test to user testUser"))) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.capture.tree", "false");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.database-name", "test");
      extractorAttributes.put("extractor.table-name", "t.*[0-9]");

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

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          dbName,
          BaseEnv.TABLE_SQL_DIALECT,
          senderEnv,
          "grant alter on any to user testUser with grant option")) {
        return;
      }

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
          dbName);
    }
  }
}
