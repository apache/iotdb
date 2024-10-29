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

package org.apache.iotdb.pipe.it.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBTablePatternFormatIT extends AbstractPipeTableModelTestIT {

  @Test
  public void testTableNamePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");
      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);
      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");

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
      Utils.insertData("test", "test", 100, 200, senderEnv);
      Utils.insertData("test1", "test1", 100, 200, senderEnv);
      Utils.insertData("pattern", "pattern", 100, 200, senderEnv);
      Utils.insertData("pattern1", "pattern1", 100, 200, senderEnv);

      Utils.assertData("test", "test", 0, 200, receiverEnv);
      Utils.assertData("test1", "test1", 0, 200, receiverEnv);
      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("test,1,1,604800000,");
      expectedResults.add("test1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testTableNamePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");
      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);
      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      //      extractorAttributes.put("extractor.database-name", "test.*");
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.start-time", "0");
      extractorAttributes.put("extractor.end-time", "49");

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

      Utils.assertData("test", "test", 0, 50, receiverEnv);
      Utils.assertData("test1", "test1", 0, 50, receiverEnv);
      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("test,1,1,604800000,");
      expectedResults.add("test1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testTableNamePatternByRealTime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      //      extractorAttributes.put("extractor.database-name", "test.*");
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");
      extractorAttributes.put("extractor.start-time", "100");
      extractorAttributes.put("extractor.end-time", "149");

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
      Utils.insertData("test", "test", 100, 200, senderEnv);
      Utils.insertData("test1", "test1", 100, 200, senderEnv);
      Utils.insertData("pattern", "pattern", 100, 200, senderEnv);
      Utils.insertData("pattern1", "pattern1", 100, 200, senderEnv);

      Utils.assertData("test", "test", 100, 150, receiverEnv);
      Utils.assertData("test1", "test1", 100, 150, receiverEnv);
      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("test,1,1,604800000,");
      expectedResults.add("test1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testDataBasePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);
      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", "pattern.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");

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
      Utils.insertData("test", "test", 100, 200, senderEnv);
      Utils.insertData("test1", "test1", 100, 200, senderEnv);
      Utils.insertData("pattern", "pattern", 100, 200, senderEnv);
      Utils.insertData("pattern1", "pattern1", 100, 200, senderEnv);
      Utils.assertData("pattern", "pattern", 0, 200, receiverEnv);
      Utils.assertData("pattern1", "pattern1", 0, 200, receiverEnv);

      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("pattern,1,1,604800000,");
      expectedResults.add("pattern1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testDataBasePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);
      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", "pattern.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");

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
      Utils.assertData("pattern", "pattern", 0, 100, receiverEnv);
      Utils.assertData("pattern1", "pattern1", 0, 100, receiverEnv);

      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("pattern,1,1,604800000,");
      expectedResults.add("pattern1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testDataBasePatternByRealtime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", "pattern.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");

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

      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);

      Utils.assertData("pattern", "pattern", 0, 100, receiverEnv);
      Utils.assertData("pattern1", "pattern1", 0, 100, receiverEnv);

      HashSet<String> expectedResults = new HashSet();
      expectedResults.add("pattern,1,1,604800000,");
      expectedResults.add("pattern1,1,1,604800000,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          expectedResults,
          null);
    }
  }

  @Test
  public void testIoTDBPatternWithDataBaseAndTable() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Utils.createDataBaseAndTable(senderEnv, "test", "test");
      Utils.createDataBaseAndTable(senderEnv, "test1", "test1");
      Utils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      Utils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      Utils.insertData("test", "test", 0, 100, senderEnv);
      Utils.insertData("test1", "test1", 0, 100, senderEnv);
      Utils.insertData("pattern", "pattern", 0, 100, senderEnv);
      Utils.insertData("pattern1", "pattern1", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.database-name", "pattern.*");
      extractorAttributes.put("extractor.table-name", "test.*");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.capture.table", "true");

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
      Utils.insertData("test", "test", 100, 200, senderEnv);
      Utils.insertData("test1", "test1", 100, 200, senderEnv);
      Utils.insertData("pattern", "pattern", 100, 200, senderEnv);
      Utils.insertData("pattern1", "pattern1", 100, 200, senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.emptySet(),
          null);
    }
  }
}
