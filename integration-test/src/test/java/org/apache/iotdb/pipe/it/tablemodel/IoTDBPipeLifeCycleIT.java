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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeLifeCycleIT extends AbstractPipeTableModelTestIT {
  @Test
  public void testLifeCycleWithHistoryEnabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");

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

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

      TableModelUtils.assertCountData("test", "test", 200, receiverEnv);

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);

      TableModelUtils.assertCountData("test", "test", 300, receiverEnv);

      TableModelUtils.insertData("test", "test", 300, 400, senderEnv);

      TableModelUtils.assertCountData("test", "test", 400, receiverEnv);

      TableModelUtils.insertData("test", "test", 400, 500, senderEnv);

      TableModelUtils.assertCountData("test", "test", 500, receiverEnv);
    }
  }

  @Test
  public void testLifeCycleWithHistoryDisabled() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("extractor.inclusion", "data.insert");
      extractorAttributes.put("extractor.inclusion.exclusion", "");

      extractorAttributes.put("extractor.history.enable", "false");
      // start-time and end-time should not work
      extractorAttributes.put("extractor.history.start-time", "0");
      extractorAttributes.put("extractor.history.end-time", "50");

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

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test1", "test1", 100, receiverEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test2");
      TableModelUtils.insertData("test2", "test2", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test1", "test1", 100, receiverEnv);

      TableModelUtils.assertCountData("test", "test", 0, receiverEnv);
    }
  }

  @Test
  public void testLifeCycleLogMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("extractor.mode", "forced-log");

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

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test", "test", 100, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test1", "test1", 100, senderEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test2", "test2");
      TableModelUtils.insertData("test2", "test2", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test2", "test2", 100, senderEnv);
    }
  }

  @Test
  public void testLifeCycleFileMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("extractor.mode", "file");

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

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test", "test", 100, receiverEnv);

      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      TableModelUtils.assertCountData("test1", "test1", 100, receiverEnv);
    }
  }

  @Test
  public void testLifeCycleHybridMode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("extractor.mode", "hybrid");

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

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      TableModelUtils.assertCountData("test", "test", 200, receiverEnv);

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);
      TableModelUtils.assertCountData("test", "test", 300, receiverEnv);
    }
  }

  @Test
  public void testLifeCycleWithClusterRestart() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");

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

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      TableModelUtils.assertCountData("test", "test", 200, receiverEnv);
    }

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (final SyncConfigNodeIServiceClient ignored =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);
      TableModelUtils.assertCountData("test", "test", 300, receiverEnv);
    }
  }

  @Test
  public void testReceiverRestartWhenTransferring() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");

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

      final AtomicInteger succeedNum = new AtomicInteger(0);
      final Thread t =
          new Thread(
              () -> {
                try {
                  for (int i = 100; i < 200; ++i) {
                    if (TableModelUtils.insertDataNotThrowError(
                        "test", "test", i, i + 1, senderEnv)) {
                      succeedNum.incrementAndGet();
                    }
                    Thread.sleep(100);
                  }
                } catch (InterruptedException ignored) {
                }
              });
      t.start();

      try {
        TestUtils.restartCluster(receiverEnv);
      } catch (final Throwable e) {
        e.printStackTrace();
        try {
          t.interrupt();
          t.join();
        } catch (Throwable ignored) {
        }
        return;
      }

      t.join();
      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
        return;
      }

      TableModelUtils.assertCountData("test", "test", 100 + succeedNum.get(), receiverEnv);
    }
  }

  @Test
  public void testReceiverAlreadyHaveTimeSeries() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(receiverEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, receiverEnv);

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("capture.table", "true");

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

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
      TableModelUtils.assertCountData("test", "test", 200, receiverEnv);

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.stopPipe("p1").getCode());

      TableModelUtils.insertData("test", "test", 200, 300, senderEnv);
      TableModelUtils.assertCountData("test", "test", 200, receiverEnv);
    }
  }

  @Test
  public void testDoubleLiving() throws Exception {
    // Double living is two clusters with pipes connecting each other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    for (int i = 0; i < 100; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      extractorAttributes.put("capture.table", "true");
      connectorAttributes.put("source.forwarding-pipe-requests", "false");
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
    }
    for (int i = 100; i < 200; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

    for (int i = 200; i < 300; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }

    TableModelUtils.insertData("test", "test", 200, 300, receiverEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      // Add this property to avoid to make self cycle.
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("capture.tree", "true");
      connectorAttributes.put("source.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", senderIp);
      connectorAttributes.put("connector.port", Integer.toString(senderPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    for (int i = 300; i < 400; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          receiverEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i))) {
        return;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(receiverEnv, "flush")) {
      return;
    }
    TableModelUtils.insertData("test", "test", 300, 400, receiverEnv);

    TableModelUtils.assertData("test", "test", 0, 400, senderEnv);
    TableModelUtils.assertData("test", "test", 0, 400, receiverEnv);

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    TableModelUtils.insertData("test", "test", 500, 600, receiverEnv);

    TableModelUtils.insertData("test", "test", 400, 500, senderEnv);

    TableModelUtils.assertData("test", "test", 0, 600, senderEnv);
    TableModelUtils.assertData("test", "test", 0, 600, receiverEnv);
  }
}
