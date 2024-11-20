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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.tablemodel.TableModelUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeTableManualIT extends AbstractPipeDualManualIT {
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
      TableModelUtils.createDataBase(senderEnv, dbName, 300);

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

      TestUtils.assertDataEventuallyOnEnv(
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
                  "c,INT32,MEASUREMENT,",
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

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.test",
              "create timeSeries root.test.d1.s1 int32",
              "insert into root.test.d1 (s1) values (1)"))) {
        return;
      }

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.emptySet(),
          "test");
    }
  }
}
