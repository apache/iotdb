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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils.RetryFlushHandler;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

// All pattern cases use the same configuration; reset pipes/databases between methods.
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBTablePatternFormatIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  // A table-name pattern must replicate both matching tables' history and realtime rows [0, 200).
  @Test
  public void testTableNamePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));
      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      TableModelUtils.insertData(database("test"), "test", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 100, 200, senderEnv);

      TableModelUtils.assertData(database("test"), "test", 0, 200, receiverEnv, handleFailure);
      TableModelUtils.assertData(database("test1"), "test1", 0, 200, receiverEnv, handleFailure);
      TableModelUtils.hasDataBase(database("test"), receiverEnv);
      TableModelUtils.hasDataBase(database("test1"), receiverEnv);
    }
  }

  // A table-name pattern with time bounds [0, 49] must load only matching historical rows.
  @Test
  public void testTableNamePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));
      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      //      extractorAttributes.put("extractor.database-name", database("test.*"));
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.start-time", "0");
      extractorAttributes.put("extractor.end-time", "49");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.assertData(database("test"), "test", 0, 50, receiverEnv, handleFailure);
      TableModelUtils.assertData(database("test1"), "test1", 0, 50, receiverEnv, handleFailure);
      TableModelUtils.hasDataBase(database("test"), receiverEnv);
      TableModelUtils.hasDataBase(database("test1"), receiverEnv);
    }
  }

  // A table-name pattern with time bounds [100, 149] must filter the rows written after Pipe
  // creation.
  @Test
  public void testTableNamePatternByRealTime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      //      extractorAttributes.put("extractor.database-name", database("test.*"));
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("extractor.start-time", "100");
      extractorAttributes.put("extractor.end-time", "149");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      TableModelUtils.insertData(database("test"), "test", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 100, 200, senderEnv);

      TableModelUtils.assertData(database("test"), "test", 100, 150, receiverEnv, handleFailure);
      TableModelUtils.assertData(database("test1"), "test1", 100, 150, receiverEnv, handleFailure);
      TableModelUtils.hasDataBase(database("test"), receiverEnv);
      TableModelUtils.hasDataBase(database("test1"), receiverEnv);
    }
  }

  // A database-name pattern must replicate all rows [0, 200) from both matching databases.
  @Test
  public void testDataBasePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));

      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", database("pattern.*"));
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      TableModelUtils.insertData(database("test"), "test", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 100, 200, senderEnv);

      TableModelUtils.assertData(
          database("pattern"), "pattern", 0, 200, receiverEnv, handleFailure);
      TableModelUtils.assertData(
          database("pattern1"), "pattern1", 0, 200, receiverEnv, handleFailure);

      HashSet<String> expectedResults = new HashSet();
      TableModelUtils.hasDataBase(database("pattern"), receiverEnv);
      TableModelUtils.hasDataBase(database("pattern1"), receiverEnv);
    }
  }

  // Creating a Pipe after the writes must load historical rows [0, 100) from matching databases.
  @Test
  public void testDataBasePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));

      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", database("pattern.*"));
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      TableModelUtils.assertData(
          database("pattern"), "pattern", 0, 100, receiverEnv, handleFailure);
      TableModelUtils.assertData(
          database("pattern1"), "pattern1", 0, 100, receiverEnv, handleFailure);

      TableModelUtils.hasDataBase(database("pattern1"), receiverEnv);
      TableModelUtils.hasDataBase(database("pattern"), receiverEnv);
    }
  }

  // Creating a Pipe before the writes must replicate realtime rows from both matching databases.
  @Test
  public void testDataBasePatternByRealtime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
        final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", database("pattern.*"));
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.assertData(
          database("pattern"), "pattern", 0, 100, receiverEnv, handleFailure);
      TableModelUtils.assertData(
          database("pattern1"), "pattern1", 0, 100, receiverEnv, handleFailure);

      TableModelUtils.hasDataBase(database("pattern"), receiverEnv);
      TableModelUtils.hasDataBase(database("pattern1"), receiverEnv);
    }
  }

  // When no table matches both database/table patterns, the receiver must keep no user databases.
  @Test
  public void testIoTDBPatternWithDataBaseAndTable() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", database("test"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", database("test1"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", database("pattern"));
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", database("pattern1"));

      TableModelUtils.insertData(database("test"), "test", 0, 100, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 0, 100, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", database("pattern.*"));
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("user", "root");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
      TableModelUtils.insertData(database("test"), "test", 100, 200, senderEnv);

      TableModelUtils.insertData(database("test1"), "test1", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern"), "pattern", 100, 200, senderEnv);

      TableModelUtils.insertData(database("pattern1"), "pattern1", 100, 200, senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("information_schema,INF,null,null,null,"),
          (String) null);
    }
  }

  private String database(final String name) {
    // Database deletion is asynchronous; never reuse another method's database incarnation.
    return name + "_" + testName.getMethodName().toLowerCase(Locale.ROOT);
  }
}
