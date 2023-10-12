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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
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
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeClusterIT {

  private BaseEnv senderEnv;
  private BaseEnv receiverEnv;

  @Before
  public void setUp() throws Exception {
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

    senderEnv.initClusterEnvironment(3, 3, 180);
    receiverEnv.initClusterEnvironment(3, 3, 180);
  }

  @After
  public void tearDown() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  @Test
  public void testWithAllParametersInLogMode() throws Exception {
    testWithAllParameters("log");
  }

  @Test
  public void testWithAllParametersInFileMode() throws Exception {
    testWithAllParameters("file");
  }

  @Test
  public void testWithAllParametersInHybridMode() throws Exception {
    testWithAllParameters("hybrid");
  }

  public void testWithAllParameters(String realtimeMode) throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)");
      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor", "iotdb-extractor");
      extractorAttributes.put("extractor.pattern", "root.db.d1");
      extractorAttributes.put("extractor.history.enable", "true");
      extractorAttributes.put("extractor.history.start-time", "2010-01-01T08:00:00+08:00");
      extractorAttributes.put("extractor.history.end-time", "2010-01-02T08:00:00+08:00");
      extractorAttributes.put("extractor.realtime.enable", "true");
      extractorAttributes.put("extractor.realtime.mode", realtimeMode);

      processorAttributes.put("processor", "do-nothing-processor");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.user", "root");
      connectorAttributes.put("connector.password", "root");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (now(), 3)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  @Test
  public void testPipeAfterDataRegionLeaderStop() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db.d1");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      AtomicInteger leaderPort = new AtomicInteger(-1);
      TShowRegionResp showRegionResp = client.showRegion(new TShowRegionReq());
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
          senderEnv.shutdownDataNode(i);
          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ignored) {
          }
          senderEnv.startDataNode(i);
          ((AbstractEnv) senderEnv).testWorkingNoUnknown();
        }
      }
      if (leaderIndex == -1) { // ensure the leader is stopped
        fail();
      }

      TestUtils.executeNonQueryOnSpecifiedDataNodeWithRetry(
          senderEnv,
          senderEnv.getDataNodeWrapper(leaderIndex),
          "insert into root.db.d1(time, s1) values (2, 2)");
      TestUtils.executeNonQueryOnSpecifiedDataNodeWithRetry(
          senderEnv, senderEnv.getDataNodeWrapper(leaderIndex), "flush");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }

    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // create a new pipe and write new data
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db.d2");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d2(time, s1) values (1, 1)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d2",
          "count(root.db.d2.s1),",
          Collections.singleton("1,"));
    }
  }

  @Test
  public void testPipeAfterRegisterNewDataNode() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db.d1");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d1(time, s1) values (1, 1)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      senderEnv.registerNewDataNode(true);
      DataNodeWrapper newDataNode =
          senderEnv.getDataNodeWrapper(senderEnv.getDataNodeWrapperList().size() - 1);
      TestUtils.executeNonQueryOnSpecifiedDataNodeWithRetry(
          senderEnv, newDataNode, "insert into root.db.d1(time, s1) values (2, 2)");
      TestUtils.executeNonQueryOnSpecifiedDataNodeWithRetry(senderEnv, newDataNode, "flush");
      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }

    TestUtils.restartCluster(senderEnv);
    TestUtils.restartCluster(receiverEnv);

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      // create a new pipe and write new data
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.pattern", "root.db.d2");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.executeNonQueryWithRetry(
          senderEnv, "insert into root.db.d2(time, s1) values (1, 1)");
      TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d2",
          "count(root.db.d2.s1),",
          Collections.singleton("1,"));
    }
  }

  @Test
  public void testCreatePipeWhenRegisteringNewDataNode() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      Thread t =
          new Thread(
              () -> {
                for (int i = 0; i < 30; ++i) {
                  try {
                    client.createPipe(
                        new TCreatePipeReq("p" + i, connectorAttributes)
                            .setExtractorAttributes(extractorAttributes)
                            .setProcessorAttributes(processorAttributes));
                  } catch (TException e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                  }
                  try {
                    Thread.sleep(100);
                  } catch (Exception ignored) {
                  }
                }
              });
      t.start();
      senderEnv.registerNewDataNode(true);
      t.join();
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(30, showPipeResult.size());
    }
  }

  @Test
  public void testRegisteringNewDataNodeWhenTransferringData() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      Thread t =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 100; ++i) {
                    TestUtils.executeNonQueryWithRetry(
                        senderEnv,
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
                    Thread.sleep(100);
                  }
                } catch (InterruptedException ignored) {
                }
              });
      t.start();
      senderEnv.registerNewDataNode(true);
      t.join();

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

      senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
      senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
    }
  }

  @Test
  public void testRegisteringNewDataNodeAfterTransferringData() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      for (int i = 0; i < 100; ++i) {
        TestUtils.executeNonQueryWithRetry(
            senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i));
      }

      senderEnv.registerNewDataNode(true);

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

      senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
      senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
    }
  }

  @Test
  public void testNewDataNodeFailureAfterTransferringData() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      for (int i = 0; i < 100; ++i) {
        TestUtils.executeNonQueryWithRetry(
            senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i * 1000));
      }

      senderEnv.registerNewDataNode(false);
      senderEnv.startDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
      senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1); // ctrl + c
      senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
      ((AbstractEnv) senderEnv).testWorkingNoUnknown();

      TestUtils.assertDataOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(1, showPipeResult.size());
    }
  }

  @Test
  public void testSenderRestartWhenTransferring() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "false");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    for (int i = 0; i < 100; ++i) {
      TestUtils.executeNonQueryWithRetry(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", i * 1000));
    }
    TestUtils.executeNonQueryWithRetry(senderEnv, "flush");

    TestUtils.restartCluster(senderEnv);

    TestUtils.assertDataOnEnv(
        receiverEnv,
        "select count(*) from root.**",
        "count(root.db.d1.s1),",
        Collections.singleton("100,"));
  }

  @Test
  public void testConcurrentlyCreatePipeOfSameName() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    AtomicInteger successCount = new AtomicInteger(0);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      Thread t =
          new Thread(
              () -> {
                try (SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  TSStatus status =
                      client.createPipe(
                          new TCreatePipeReq("p1", connectorAttributes)
                              .setExtractorAttributes(extractorAttributes)
                              .setProcessorAttributes(processorAttributes));
                  if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    successCount.updateAndGet(v -> v + 1);
                  }
                } catch (Exception e) {
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
      Thread t =
          new Thread(
              () -> {
                try (SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  TSStatus status = client.dropPipe("p1");
                  if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                    successCount.updateAndGet(v -> v + 1);
                  }
                } catch (Exception e) {
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

    Assert.assertEquals(10, successCount.get());
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
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

  private void testCreatePipesWithSameConnector(int pipeCount) throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    Map<String, String> extractorAttributes = new HashMap<>();
    Map<String, String> processorAttributes = new HashMap<>();
    Map<String, String> connectorAttributes = new HashMap<>();

    connectorAttributes.put("connector", "iotdb-thrift-connector");
    connectorAttributes.put("connector.batch.enable", "false");
    connectorAttributes.put("connector.ip", receiverIp);
    connectorAttributes.put("connector.port", Integer.toString(receiverPort));

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < pipeCount; ++i) {
      int finalI = i;
      Thread t =
          new Thread(
              () -> {
                try (SyncConfigNodeIServiceClient client =
                    (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
                  TSStatus status =
                      client.createPipe(
                          new TCreatePipeReq("p" + finalI, connectorAttributes)
                              .setExtractorAttributes(extractorAttributes)
                              .setProcessorAttributes(processorAttributes));
                  Assert.assertEquals(
                      TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
                } catch (Exception e) {
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

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      List<TShowPipeInfo> showPipeResult = client.showPipe(new TShowPipeReq()).pipeInfoList;
      Assert.assertEquals(pipeCount, showPipeResult.size());
      showPipeResult =
          client.showPipe(new TShowPipeReq().setPipeName("p1").setWhereClause(true)).pipeInfoList;
      Assert.assertEquals(pipeCount, showPipeResult.size());
    }
  }
}
