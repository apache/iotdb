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
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
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
@Category({MultiClusterIT2TableModel.class})
public class IoTDBTablePatternFormatIT extends AbstractPipeTableModelTestIT {

  @Test
  public void testTableNamePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

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
      insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertData("test", "test", 0, 200, receiverEnv);
      TableModelUtils.assertData("test1", "test1", 0, 200, receiverEnv);
      if (!TableModelUtils.hasDataBase("test", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("test1", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testTableNamePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

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

      TableModelUtils.assertData("test", "test", 0, 50, receiverEnv);
      TableModelUtils.assertData("test1", "test1", 0, 50, receiverEnv);
      HashSet<String> expectedResults = new HashSet();
      if (!TableModelUtils.hasDataBase("test", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("test1", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testTableNamePatternByRealTime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

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
      insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertData("test", "test", 100, 150, receiverEnv);
      TableModelUtils.assertData("test1", "test1", 100, 150, receiverEnv);
      if (!TableModelUtils.hasDataBase("test", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("test1", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testDataBasePattern() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

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
      insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.assertData("pattern", "pattern", 0, 200, receiverEnv);
      TableModelUtils.assertData("pattern1", "pattern1", 0, 200, receiverEnv);

      HashSet<String> expectedResults = new HashSet();
      if (!TableModelUtils.hasDataBase("pattern", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("pattern1", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testDataBasePatternByHistoryTSFile() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

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
      TableModelUtils.assertData("pattern", "pattern", 0, 100, receiverEnv);
      TableModelUtils.assertData("pattern1", "pattern1", 0, 100, receiverEnv);

      if (!TableModelUtils.hasDataBase("pattern1", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("pattern", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testDataBasePatternByRealtime() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

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

      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertData("pattern", "pattern", 0, 100, receiverEnv);
      TableModelUtils.assertData("pattern1", "pattern1", 0, 100, receiverEnv);

      if (!TableModelUtils.hasDataBase("pattern", receiverEnv)) {
        Assert.fail();
      }
      if (!TableModelUtils.hasDataBase("pattern1", receiverEnv)) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testIoTDBPatternWithDataBaseAndTable() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern", "pattern");
      TableModelUtils.createDataBaseAndTable(senderEnv, "pattern1", "pattern1");

      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

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
      insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern", "pattern", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      insertResult = TableModelUtils.insertData("pattern1", "pattern1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL(ms),SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.emptySet(),
          null);
    }
  }
}
