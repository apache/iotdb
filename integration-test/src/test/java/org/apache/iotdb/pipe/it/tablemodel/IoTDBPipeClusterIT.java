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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2TableModel.class})
public class IoTDBPipeClusterIT extends AbstractPipeTableModelTestIT {

  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment(3, 3, 180);
    receiverEnv.initClusterEnvironment(3, 3, 180);
  }

  @Test
  public void testWithAllParametersInStreamingMode() throws Exception {
    testWithAllParameters("true");
  }

  @Test
  public void testWithAllParametersInNotStreamingMode() throws Exception {
    testWithAllParameters("false");
  }

  private void testWithAllParameters(final String realtimeMode) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    boolean insertResult = true;
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("table-name", "test");
      extractorAttributes.put("start-time", "0");
      extractorAttributes.put("end-time", "199");
      extractorAttributes.put("mode.streaming", realtimeMode);

      processorAttributes.put("processor", "do-nothing-processor");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.user", "root");
      connectorAttributes.put("connector.password", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          TableModelUtils.getQuerySql("test"),
          TableModelUtils.generateHeaderResults(),
          TableModelUtils.generateExpectedResults(0, 100),
          "test");

      insertResult = TableModelUtils.insertData("test", "test", 100, 300, senderEnv);
      if (!insertResult) {
        return;
      }
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          TableModelUtils.getQuerySql("test"),
          TableModelUtils.generateHeaderResults(),
          TableModelUtils.generateExpectedResults(0, 200),
          "test");
    }
  }

  @Test
  public void testPipeAfterDataRegionLeaderStop() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");
      extractorAttributes.put("start-time", "0");
      extractorAttributes.put("end-time", "300");

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
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }

      final AtomicInteger leaderPort = new AtomicInteger(-1);
      final TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
      showRegionResp
          .getRegionInfoList()
          .forEach(
              regionInfo -> {
                if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
                  leaderPort.set(regionInfo.getClientRpcPort());
                }
              });

      int leaderIndex = -1;
      for (int i = 0; i < 3; ++i) {
        if (senderEnv.getDataNodeWrapper(i).getPort() == leaderPort.get()) {
          leaderIndex = i;
          try {
            senderEnv.shutdownDataNode(i);
          } catch (final Throwable e) {
            e.printStackTrace();
            return;
          }
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (final InterruptedException ignored) {
          }
          try {
            senderEnv.startDataNode(i);
            ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
          } catch (final Throwable e) {
            e.printStackTrace();
            return;
          }
        }
      }
      if (leaderIndex == -1) { // ensure the leader is stopped
        fail();
      }

      insertResult = TableModelUtils.insertData("test", "test", 200, 300, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 200, 300, senderEnv);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertData("test", "test", 0, 300, receiverEnv);
    }

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // Create a new pipe and write new data
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test1");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test1");
      extractorAttributes.put("start-time", "0");
      extractorAttributes.put("end-time", "300");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      insertResult = TableModelUtils.insertData("test", "test", 300, 400, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 300, 400, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.assertData("test", "test", 0, 301, receiverEnv);
      TableModelUtils.assertData("test1", "test1", 0, 301, receiverEnv);
    }
  }

  @Test
  public void testPipeAfterRegisterNewDataNode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }

      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");

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
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
      if (!insertResult) {
        return;
      }
      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }

      final DataNodeWrapper newDataNode =
          senderEnv.getDataNodeWrapper(senderEnv.getDataNodeWrapperList().size() - 1);
      insertResult = TableModelUtils.insertData("test", "test", 200, 300, senderEnv, newDataNode);
      insertResult =
          insertResult
              && TableModelUtils.insertData("test1", "test1", 200, 300, senderEnv, newDataNode);
      if (!insertResult) {
        return;
      }

      TableModelUtils.assertData("test", "test", 0, 300, receiverEnv);
    }

    try {
      TestUtils.restartCluster(senderEnv);
      TestUtils.restartCluster(receiverEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // create a new pipe and write new data
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test1");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test1");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      insertResult = TableModelUtils.insertData("test", "test", 300, 400, senderEnv);
      insertResult =
          insertResult && TableModelUtils.insertData("test1", "test1", 300, 400, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.assertData("test1", "test1", 0, 400, receiverEnv);
      TableModelUtils.assertData("test", "test", 0, 400, receiverEnv);
    }
  }

  @Test
  public void testCreatePipeWhenRegisteringNewDataNode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test1");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test1");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      final Thread t =
          new Thread(
              () -> {
                for (int i = 0; i < 30; ++i) {
                  try {
                    client.createPipe(
                        new TCreatePipeReq("p" + i, connectorAttributes)
                            .setExtractorAttributes(extractorAttributes)
                            .setProcessorAttributes(processorAttributes));
                  } catch (final TException e) {
                    // Not sure if the "createPipe" has succeeded
                    e.printStackTrace();
                    return;
                  }
                  try {
                    Thread.sleep(100);
                  } catch (final Exception ignored) {
                  }
                }
              });
      t.start();
      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }
      t.join();
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(30, showPipeResult.size());
    }
  }

  @Test
  public void testRegisteringNewDataNodeWhenTransferringData() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");

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
                      Thread.sleep(100);
                    }
                  }
                } catch (final InterruptedException ignored) {
                }
              });
      t.start();
      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }
      t.join();
      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
        return;
      }

      TableModelUtils.assertCountData("test", "test", succeedNum.get() + 100, receiverEnv);

      try {
        senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
        senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
      } catch (final Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testRegisteringNewDataNodeAfterTransferringData() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
    if (!insertResult) {
      return;
    }
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");

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

      int succeedNum = 0;
      for (int i = 100; i < 200; ++i) {
        if (TableModelUtils.insertDataNotThrowError("test", "test", i, i + 1, senderEnv)) {
          succeedNum++;
        }
      }

      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }

      TableModelUtils.assertCountData("test", "test", succeedNum + 100, receiverEnv);

      try {
        senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
        senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
      } catch (final Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testSenderRestartWhenTransferring() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");

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

    int succeedNum = 0;
    for (int i = 100; i < 200; ++i) {
      if (TableModelUtils.insertDataNotThrowError("test", "test", i, i + 1, senderEnv)) {
        succeedNum++;
      }
    }
    if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "flush")) {
      return;
    }

    try {
      TestUtils.restartCluster(senderEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    TableModelUtils.assertCountData("test", "test", succeedNum + 100, receiverEnv);
  }

  @Test
  public void testConcurrentlyCreatePipeOfSameName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("database-name", "test");
    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("table-name", "test");

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    final AtomicInteger successCount = new AtomicInteger(0);
    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      final Thread t =
          new Thread(
              () -> {
                try (final SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  final TSStatus status =
                      client.createPipe(
                          new TCreatePipeReq("p1", connectorAttributes)
                              .setExtractorAttributes(extractorAttributes)
                              .setProcessorAttributes(processorAttributes));
                  if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    successCount.incrementAndGet();
                  }
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                } catch (final TException | ClientManagerException | IOException e) {
                  e.printStackTrace();
                } catch (final Exception e) {
                  // Fail iff pipe exception occurs
                  e.printStackTrace();
                  fail(e.getMessage());
                }
              });
      t.start();
      threads.add(t);
    }

    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(1, successCount.get());

    successCount.set(0);
    for (int i = 0; i < 10; ++i) {
      final Thread t =
          new Thread(
              () -> {
                try (final SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  final TSStatus status = client.dropPipe("p1");
                  if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    successCount.incrementAndGet();
                  }
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                } catch (final TException | ClientManagerException | IOException e) {
                  e.printStackTrace();
                } catch (final Exception e) {
                  // Fail iff pipe exception occurs
                  e.printStackTrace();
                  fail(e.getMessage());
                }
              });
      t.start();
      threads.add(t);
    }
    for (final Thread t : threads) {
      t.join();
    }

    // Assert at least 1 drop operation succeeds
    Assert.assertTrue(successCount.get() >= 1);
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(0, showPipeResult.size());
    }
  }

  @Test
  public void testCreate10PipesWithSameConnector() throws Exception {
    testCreatePipesWithSameConnector(10);
  }

  @Test
  public void testCreate50PipesWithSameConnector() throws Exception {
    testCreatePipesWithSameConnector(50);
  }

  @Test
  public void testCreate100PipesWithSameConnector() throws Exception {
    testCreatePipesWithSameConnector(100);
  }

  private void testCreatePipesWithSameConnector(final int pipeCount) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> extractorAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> connectorAttributes = new HashMap<>();

    extractorAttributes.put("database-name", "test");
    extractorAttributes.put("capture.table", "true");
    extractorAttributes.put("table-name", "test");

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    final AtomicInteger successCount = new AtomicInteger(0);
    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < pipeCount; ++i) {
      final int finalI = i;
      final Thread t =
          new Thread(
              () -> {
                try (final SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  final TSStatus status =
                      client.createPipe(
                          new TCreatePipeReq("p" + finalI, connectorAttributes)
                              .setExtractorAttributes(extractorAttributes)
                              .setProcessorAttributes(processorAttributes));
                  Assert.assertEquals(
                      TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
                  successCount.incrementAndGet();
                } catch (final InterruptedException e) {
                  e.printStackTrace();
                  Thread.currentThread().interrupt();
                } catch (final TException | ClientManagerException | IOException e) {
                  e.printStackTrace();
                } catch (final Exception e) {
                  // Fail iff pipe exception occurs
                  e.printStackTrace();
                  fail(e.getMessage());
                }
              });
      t.start();
      threads.add(t);
    }
    for (final Thread t : threads) {
      t.join();
    }

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(successCount.get(), showPipeResult.size());
      showPipeResult =
          client.showPipe(new TShowPipeReq().setPipeName("p1").setWhereClause(true)).pipeInfoList;
      Assert.assertEquals(successCount.get(), showPipeResult.size());
    }
  }

  @Test
  public void testNegativeTimestamp() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    boolean insertResult = true;

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      insertResult = TableModelUtils.insertData("test", "test", -100, 100, senderEnv);
      if (!insertResult) {
        return;
      }
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("database-name", "test");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("table-name", "test");

      processorAttributes.put("processor", "do-nothing-processor");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
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

      TableModelUtils.assertData("test", "test", -100, 100, receiverEnv);

      insertResult = TableModelUtils.insertData("test", "test", -200, -100, senderEnv);
      if (!insertResult) {
        return;
      }
      TableModelUtils.assertData("test", "test", -200, 100, receiverEnv);
    }
  }
}
