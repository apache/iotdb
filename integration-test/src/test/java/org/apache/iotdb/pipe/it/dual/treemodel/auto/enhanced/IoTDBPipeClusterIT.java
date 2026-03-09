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

package org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced;

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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.treemodel.auto.AbstractPipeDualTreeModelAutoIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeClusterIT extends AbstractPipeDualTreeModelAutoIT {

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
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment(3, 3, 180);
    receiverEnv.initClusterEnvironment(3, 3, 180);
  }

  @Test
  public void testMachineDowntimeAsync() {
    testMachineDowntime("iotdb-thrift-sink");
  }

  @Test
  public void testMachineDowntimeSync() {
    testMachineDowntime("iotdb-thrift-sync-sink");
  }

  private void testMachineDowntime(String sink) {
    StringBuilder a = new StringBuilder();
    for (DataNodeWrapper nodeWrapper : receiverEnv.getDataNodeWrapperList()) {
      a.append(nodeWrapper.getIp()).append(":").append(nodeWrapper.getPort());
      a.append(",");
    }
    a.deleteCharAt(a.length() - 1);

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.insertData("test", "test", 0, 1, senderEnv);
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
              "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
              "flush"),
          null);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("capture.tree", "true");
      sourceAttributes.put("user", "root");

      processorAttributes.put("processor", "do-nothing-processor");

      sinkAttributes.put("sink", sink);
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.node-urls", a.toString());

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      receiverEnv.getDataNodeWrapper(0).stop();

      // Ensure that the kill -9 operation is completed
      Thread.sleep(5000);
      for (DataNodeWrapper nodeWrapper : receiverEnv.getDataNodeWrapperList()) {
        if (!nodeWrapper.isAlive()) {
          continue;
        }
        TestUtils.assertDataEventuallyOnEnv(
            receiverEnv,
            nodeWrapper,
            "select count(*) from root.db.**",
            "count(root.db.d1.s1),",
            Collections.singleton("2,"),
            600);
      }
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (now(), 3)", "flush"),
          null);

    } catch (Exception e) {
      fail(e.getMessage());
    }

    for (DataNodeWrapper nodeWrapper : receiverEnv.getDataNodeWrapperList()) {
      if (!nodeWrapper.isAlive()) {
        continue;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          nodeWrapper,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"),
          600);
      return;
    }
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

  public void testWithAllParameters(final String realtimeMode) throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
              "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
              "flush"),
          null);
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("source.pattern", "root.db.d1");
      sourceAttributes.put("source.history.enable", "true");
      sourceAttributes.put("source.history.start-time", "2010-01-01T08:00:00+08:00");
      sourceAttributes.put("source.history.end-time", "2010-01-02T08:00:00+08:00");
      sourceAttributes.put("source.realtime.enable", "true");
      sourceAttributes.put("source.realtime.mode", realtimeMode);
      sourceAttributes.put("user", "root");

      processorAttributes.put("processor", "do-nothing-processor");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.user", "root");
      sinkAttributes.put("sink.password", "root");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("1,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (now(), 3)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
    }
  }

  @Test
  public void testPipeAfterDataRegionLeaderStop() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.db.d1");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

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

      TestUtils.tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
          senderEnv,
          senderEnv.getDataNodeWrapper(leaderIndex),
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"));

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
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
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.db.d2");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d2(time, s1) values (1, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d2",
          "count(root.db.d2.s1),",
          Collections.singleton("1,"));
    }
  }

  @Test
  public void testPipeAfterRegisterNewDataNode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.db.d1");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d1(time, s1) values (1, 1)", "flush"),
          null);

      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }
      final DataNodeWrapper newDataNode =
          senderEnv.getDataNodeWrapper(senderEnv.getDataNodeWrapperList().size() - 1);
      TestUtils.tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
          senderEnv,
          newDataNode,
          Arrays.asList("insert into root.db.d1(time, s1) values (2, 2)", "flush"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("2,"));
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
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.pattern", "root.db.d2");
      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p2", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p2").getCode());

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList("insert into root.db.d2(time, s1) values (1, 1)", "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d2",
          "count(root.db.d2.s1),",
          Collections.singleton("1,"));
    }
  }

  @Test
  public void testCreatePipeWhenRegisteringNewDataNode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final Thread t =
          new Thread(
              () -> {
                for (int i = 0; i < 30; ++i) {
                  try {
                    client.createPipe(
                        new TCreatePipeReq("p" + i, sinkAttributes)
                            .setExtractorAttributes(sourceAttributes)
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
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(30, showPipeResult.size());
    }
  }

  @Test
  public void testRegisteringNewDataNodeWhenTransferringData() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      final AtomicInteger succeedNum = new AtomicInteger(0);

      // ensure at least one insert happens before adding node so that the time series can be
      // created
      TestUtils.executeNonQuery(
          senderEnv, String.format("insert into root.db.d1(time, s1) values (%s, 1)", 0), null);
      final Thread t =
          new Thread(
              () -> {
                Connection connection = null;
                try {
                  connection = senderEnv.getConnection();
                } catch (SQLException e) {
                  // ignore
                }
                try {
                  for (int i = 1; i < 100; ++i) {
                    TestUtils.executeNonQuery(
                        senderEnv,
                        String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
                        connection);
                    Thread.sleep(100);
                  }
                } catch (final InterruptedException ignored) {
                } finally {
                  if (connection != null) {
                    try {
                      connection.close();
                    } catch (SQLException e) {
                      // ignore
                    }
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
      TestUtils.executeNonQuery(senderEnv, "flush", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

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

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      try (Connection connection = senderEnv.getConnection()) {
        for (int i = 0; i < 100; ++i) {
          TestUtils.executeNonQuery(
              senderEnv,
              String.format("insert into root.db.d1(time, s1) values (%s, 1)", i),
              connection);
        }
      }

      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }

      TestUtils.executeNonQuery(senderEnv, "flush", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

      try {
        senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
        senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
      } catch (final Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Ignore(
      "Currently ignore this test because this test intends to test the behaviour when the sender has"
          + " a temporary node joined and then removed, but in reality it just tears it down. In this"
          + " circumstance the IT may fail. However, the \"remove\" method is currently not provided thus"
          + " we ignore this test now.")
  @Test
  public void testNewDataNodeFailureParallelToTransferringData() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      try (Connection connection = senderEnv.getConnection()) {
        for (int i = 0; i < 100; ++i) {
          TestUtils.executeNonQuery(
              senderEnv,
              String.format("insert into root.db.d1(time, s1) values (%s, 1)", i * 1000),
              connection);
        }
      }

      try {
        senderEnv.registerNewDataNode(false);
        senderEnv.startDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
        senderEnv.shutdownDataNode(senderEnv.getDataNodeWrapperList().size() - 1);
        senderEnv.getDataNodeWrapperList().remove(senderEnv.getDataNodeWrapperList().size() - 1);
        ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }

      TestUtils.executeNonQuery(senderEnv, "flush", null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.d1",
          "count(root.db.d1.s1),",
          Collections.singleton("100,"));

      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
    }
  }

  @Test
  public void testSenderRestartWhenTransferring() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("user", "root");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.batch.enable", "false");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }

    try (Connection connection = senderEnv.getConnection()) {
      for (int i = 0; i < 100; ++i) {
        TestUtils.executeNonQuery(
            senderEnv,
            String.format("insert into root.db.d1(time, s1) values (%s, 1)", i * 1000),
            connection);
      }
    }

    TestUtils.executeNonQuery(senderEnv, "flush", null);

    try {
      TestUtils.restartCluster(senderEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select count(*) from root.db.**",
        "count(root.db.d1.s1),",
        Collections.singleton("100,"));
  }

  @Test
  public void testConcurrentlyCreatePipeOfSameName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    sourceAttributes.put("user", "root");

    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverIp);
    sinkAttributes.put("sink.port", Integer.toString(receiverPort));

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
                          new TCreatePipeReq("p1", sinkAttributes)
                              .setExtractorAttributes(sourceAttributes)
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
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
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

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    sourceAttributes.put("user", "root");

    sinkAttributes.put("sink", "iotdb-thrift-sink");
    sinkAttributes.put("sink.batch.enable", "false");
    sinkAttributes.put("sink.ip", receiverIp);
    sinkAttributes.put("sink.port", Integer.toString(receiverPort));

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
                          new TCreatePipeReq("p" + finalI, sinkAttributes)
                              .setExtractorAttributes(sourceAttributes)
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
      List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(successCount.get(), showPipeResult.size());
      showPipeResult =
          client.showPipe(new TShowPipeReq().setPipeName("p1").setWhereClause(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(successCount.get(), showPipeResult.size());
    }
  }

  @Test
  public void testNegativeTimestamp() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (0, 1)",
              "insert into root.db.d1(time, s1) values (-1, 2)",
              "insert into root.db.d1(time, s1) values (1960-01-02T10:00:00+08:00, 2)",
              "flush"),
          null);
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("user", "root");

      processorAttributes.put("processor", "do-nothing-processor");

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("3,"));

      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              // Test the correctness of insertRowsNode transmission
              "insert into root.db.d1(time, s1) values (-122, 3)",
              "insert into root.db.d1(time, s1) values (-123, 3), (now(), 3)",
              "flush"),
          null);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.db.**",
          "count(root.db.d1.s1),",
          Collections.singleton("6,"));
    }
  }
}
