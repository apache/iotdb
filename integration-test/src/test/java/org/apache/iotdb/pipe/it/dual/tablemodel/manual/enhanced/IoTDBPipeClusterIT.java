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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
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
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeClusterIT extends AbstractPipeTableModelDualManualIT {

  private static final double SYNC_LAG_DELTA = 0.001;

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
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    senderEnv
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setDataReplicationFactor(2)
        .setSchemaReplicationFactor(3)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getDataNodeConfig()
        .setMetricReporterType(Collections.singletonList("PROMETHEUS"));

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

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
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

      TableModelUtils.assertCountData("test", "test", 1, receiverEnv);
      receiverEnv.getDataNodeWrapper(0).stop();

      // Ensure that the kill -9 operation is completed
      Thread.sleep(5000);
      TableModelUtils.insertData("test", "test", 1, 2, senderEnv);
    } catch (Exception e) {
      fail(e.getMessage());
    }

    for (DataNodeWrapper nodeWrapper : receiverEnv.getDataNodeWrapperList()) {
      if (!nodeWrapper.isAlive()) {
        continue;
      }
      TableModelUtils.assertCountData("test", "test", 2, receiverEnv, nodeWrapper);
      return;
    }
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

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("table-name", "test");
      sourceAttributes.put("start-time", "0");
      sourceAttributes.put("end-time", "199");
      sourceAttributes.put("mode.streaming", realtimeMode);
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
          TableModelUtils.getQuerySql("test"),
          TableModelUtils.generateHeaderResults(),
          TableModelUtils.generateExpectedResults(0, 100),
          "test",
          handleFailure);

      TableModelUtils.insertData("test", "test", 100, 300, senderEnv);

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          TableModelUtils.getQuerySql("test"),
          TableModelUtils.generateHeaderResults(),
          TableModelUtils.generateExpectedResults(0, 200),
          "test",
          handleFailure);
    }
  }

  // This function has a certain probability of triggering replica asynchrony. To ensure the success
  // of the test, it will be retried 5 times. The test will be ignored after five retries.
  @Test
  public void testPipeAfterDataRegionLeaderStop() {
    for (int retry = 0; retry < 5; retry++) {
      try {
        if (retry != 0) {
          this.setUp();
        }
        final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

        final String receiverIp = receiverDataNode.getIp();
        final int receiverPort = receiverDataNode.getPort();
        final Consumer<String> handleFailure =
            o -> {
              TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
              TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
            };

        try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
          final Map<String, String> sourceAttributes = new HashMap<>();
          final Map<String, String> processorAttributes = new HashMap<>();
          final Map<String, String> sinkAttributes = new HashMap<>();
          TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
          TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
          TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
          TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);

          sourceAttributes.put("source", "iotdb-source");
          sourceAttributes.put("database-name", "test");
          sourceAttributes.put("capture.table", "true");
          sourceAttributes.put("__system.sql-dialect", "table");
          sourceAttributes.put("table-name", "test");
          sourceAttributes.put("start-time", "0");
          sourceAttributes.put("end-time", "300");
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

          TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

          TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
          // Avoid electing a stale follower after stopping the current test1 leader.
          flushTableDataRegionReplicasAfterReplicationComplete(
              senderEnv, Collections.singletonList("test1"));

          final int leaderIndex = restartTableDataRegionLeader(client, "test1");
          if (leaderIndex == -1) { // ensure the leader is stopped
            fail();
          }

          TableModelUtils.insertData("test", "test", 200, 300, senderEnv);

          TableModelUtils.insertData("test1", "test1", 200, 300, senderEnv);

          TableModelUtils.assertData("test", "test", 0, 300, receiverEnv, handleFailure);
          flushTableDataRegionReplicasAfterReplicationComplete(
              senderEnv, Arrays.asList("test", "test1"));
          flushTableDataRegionReplicasAfterReplicationComplete(
              receiverEnv, Collections.singletonList("test"));
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

          sourceAttributes.put("database-name", "test1");
          sourceAttributes.put("capture.table", "true");
          sourceAttributes.put("__system.sql-dialect", "table");
          sourceAttributes.put("table-name", "test1");
          sourceAttributes.put("start-time", "0");
          sourceAttributes.put("end-time", "300");
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

          TableModelUtils.insertData("test", "test", 300, 400, senderEnv);

          TableModelUtils.insertData("test1", "test1", 300, 400, senderEnv);

          TableModelUtils.assertData("test", "test", 0, 301, receiverEnv, handleFailure);
          TableModelUtils.assertData("test1", "test1", 0, 301, receiverEnv, handleFailure);
        }
        return;
      } catch (final Exception e) {
        if (retry < 4) {
          this.tearDown();
        }
      }
    }
  }

  private int restartTableDataRegionLeader(
      final SyncConfigNodeIServiceClient client, final String database) throws TException {
    final List<TRegionInfo> leaderRegionInfoList =
        showTableDataRegionLeaders(Collections.singletonList(database), client);
    if (leaderRegionInfoList.isEmpty()) {
      return -1;
    }

    final TRegionInfo targetRegionInfo =
        leaderRegionInfoList.stream()
            .min(Comparator.comparingInt(regionInfo -> regionInfo.getConsensusGroupId().getId()))
            .orElse(null);
    if (targetRegionInfo == null) {
      return -1;
    }

    final int leaderPort = targetRegionInfo.getClientRpcPort();
    for (int i = 0; i < senderEnv.getDataNodeWrapperList().size(); ++i) {
      if (senderEnv.getDataNodeWrapper(i).getPort() != leaderPort) {
        continue;
      }

      try {
        senderEnv.shutdownDataNode(i);
      } catch (final Throwable e) {
        e.printStackTrace();
        return -1;
      }

      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (final InterruptedException ignored) {
        Thread.currentThread().interrupt();
        return -1;
      }

      try {
        senderEnv.startDataNode(i);
        ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
      } catch (final Throwable e) {
        e.printStackTrace();
        return -1;
      }
      return i;
    }
    return -1;
  }

  private void flushTableDataRegionReplicasAfterReplicationComplete(
      final BaseEnv env, final List<String> databases) {
    waitForTableDataRegionReplicationComplete(env, databases);
    TestUtils.executeNonQueryWithRetry(env, "flush");
    waitForTableDataRegionReplicationComplete(env, databases);
  }

  private void waitForTableDataRegionReplicationComplete(
      final BaseEnv env, final List<String> databases) {
    await()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              try (final SyncConfigNodeIServiceClient client =
                  (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
                final List<TRegionInfo> leaderRegionInfoList =
                    showTableDataRegionLeaders(databases, client);
                Assert.assertFalse(
                    "No table DataRegion leader found for databases " + databases,
                    leaderRegionInfoList.isEmpty());

                for (final TRegionInfo regionInfo : leaderRegionInfoList) {
                  final DataNodeWrapper leaderNode =
                      findDataNodeWrapperByPort(env, regionInfo.getClientRpcPort());
                  final String metricsUrl =
                      "http://"
                          + leaderNode.getIp()
                          + ":"
                          + leaderNode.getMetricPort()
                          + "/metrics";
                  final String metricsContent = env.getUrlContent(metricsUrl, null);
                  Assert.assertNotNull(
                      "Failed to fetch metrics from leader DataNode at " + metricsUrl,
                      metricsContent);
                  assertSyncLagIsZero(metricsContent, buildDataRegionTag(regionInfo), metricsUrl);
                }
              }
            });
  }

  private List<TRegionInfo> showTableDataRegionLeaders(
      final List<String> databases, final SyncConfigNodeIServiceClient client) throws TException {
    final TShowRegionResp showRegionResp =
        client.showRegion(
            new TShowRegionReq()
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .setDatabases(databases)
                .setIsTableModel(true));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
    final List<TRegionInfo> result = new ArrayList<>();
    for (final TRegionInfo regionInfo : showRegionResp.getRegionInfoList()) {
      if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
        result.add(regionInfo);
      }
    }
    return result;
  }

  private DataNodeWrapper findDataNodeWrapperByPort(final BaseEnv env, final int port) {
    for (final DataNodeWrapper dataNodeWrapper : env.getDataNodeWrapperList()) {
      if (dataNodeWrapper.getPort() == port) {
        return dataNodeWrapper;
      }
    }
    fail("Failed to find DataNodeWrapper for client rpc port " + port);
    return null;
  }

  private String buildDataRegionTag(final TRegionInfo regionInfo) {
    return "DataRegion[" + regionInfo.getConsensusGroupId().getId() + "]";
  }

  private void assertSyncLagIsZero(
      final String metricsContent, final String dataRegionTag, final String metricsUrl) {
    for (final String line : metricsContent.split("\\R")) {
      if (!line.startsWith("iot_consensus{")
          || !line.contains("type=\"syncLag\"")
          || !line.contains("region=\"" + dataRegionTag + "\"")) {
        continue;
      }
      final int lastSpaceIndex = line.lastIndexOf(' ');
      Assert.assertTrue("Malformed syncLag metric line: " + line, lastSpaceIndex > 0);
      Assert.assertEquals(
          "Expected syncLag of " + dataRegionTag + " to be 0 at " + metricsUrl + " but got " + line,
          0.0,
          Double.parseDouble(line.substring(lastSpaceIndex + 1)),
          SYNC_LAG_DELTA);
      return;
    }
    fail("No syncLag metric found for " + dataRegionTag + " at " + metricsUrl);
  }

  @Test
  public void testPipeAfterRegisterNewDataNode() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);

      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test");
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

      TableModelUtils.insertData("test", "test", 100, 200, senderEnv);

      TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);

      try {
        senderEnv.registerNewDataNode(true);
      } catch (final Throwable e) {
        e.printStackTrace();
        return;
      }

      final DataNodeWrapper newDataNode =
          senderEnv.getDataNodeWrapper(senderEnv.getDataNodeWrapperList().size() - 1);
      TableModelUtils.insertData("test", "test", 200, 300, senderEnv, newDataNode);
      TableModelUtils.insertData("test1", "test1", 200, 300, senderEnv, newDataNode);
      TableModelUtils.assertData("test", "test", 0, 300, receiverEnv, handleFailure);
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

      sourceAttributes.put("database-name", "test1");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test1");
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

      TableModelUtils.insertData("test", "test", 300, 400, senderEnv);

      TableModelUtils.insertData("test1", "test1", 300, 400, senderEnv);

      TableModelUtils.assertData("test1", "test1", 0, 400, receiverEnv, handleFailure);
      TableModelUtils.assertData("test", "test", 0, 400, receiverEnv, handleFailure);
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

      sourceAttributes.put("database-name", "test1");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test1");
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
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test");
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
      TestUtils.executeNonQuery(senderEnv, "flush", null);

      TableModelUtils.assertCountData(
          "test", "test", succeedNum.get() + 100, receiverEnv, handleFailure);

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

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test");
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

      TableModelUtils.assertCountData("test", "test", succeedNum + 100, receiverEnv, handleFailure);

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
    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", 0, 100, senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test");
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

    int succeedNum = 0;
    for (int i = 100; i < 200; ++i) {
      if (TableModelUtils.insertDataNotThrowError("test", "test", i, i + 1, senderEnv)) {
        succeedNum++;
      }
    }
    TestUtils.executeNonQuery(senderEnv, "flush", null);

    try {
      TestUtils.restartCluster(senderEnv);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    TableModelUtils.assertCountData("test", "test", succeedNum + 100, receiverEnv, handleFailure);
  }

  @Test
  public void testConcurrentlyCreatePipeOfSameName() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    final Map<String, String> sourceAttributes = new HashMap<>();
    final Map<String, String> processorAttributes = new HashMap<>();
    final Map<String, String> sinkAttributes = new HashMap<>();

    sourceAttributes.put("database-name", "test");
    sourceAttributes.put("capture.table", "true");
    sourceAttributes.put("__system.sql-dialect", "table");
    sourceAttributes.put("table-name", "test");
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

    sourceAttributes.put("database-name", "test");
    sourceAttributes.put("capture.table", "true");
    sourceAttributes.put("__system.sql-dialect", "table");
    sourceAttributes.put("table-name", "test");
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

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
      TableModelUtils.insertData("test", "test", -100, 100, senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source", "iotdb-source");
      sourceAttributes.put("database-name", "test");
      sourceAttributes.put("capture.table", "true");
      sourceAttributes.put("__system.sql-dialect", "table");
      sourceAttributes.put("table-name", "test");
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

      TableModelUtils.assertData("test", "test", -100, 100, receiverEnv, handleFailure);

      TableModelUtils.insertData("test", "test", -200, -100, senderEnv);

      TableModelUtils.assertData("test", "test", -200, 100, receiverEnv, handleFailure);
    }
  }

  @Test
  public void testHistoryDataWithEmptyField() {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "CREATE DATABASE iot_table_stream_attr",
            "USE iot_table_stream_attr",
            "CREATE TABLE table1 (region STRING TAG, device_id STRING TAG, model_id STRING ATTRIBUTE, maintenance STRING ATTRIBUTE COMMENT 'maintenance', temperature FLOAT FIELD COMMENT 'temperature', humidity STRING ATTRIBUTE COMMENT 'humidity', plant_id STRING TAG) COMMENT 'table1'",
            String.format(
                "create pipe test with source ('inclusion'='all') with sink('node-urls'='%s')",
                receiverEnv.getDataNodeWrapper(0).getIpAndPortString()),
            "select * from table1 order by time",
            "INSERT INTO table1(region, plant_id, device_id, model_id, maintenance, time, temperature, humidity) VALUES ('north', null, 'd101', 'red', null, '2025-11-26 13:38:00', 91.0, null), (null, '1003', null, null, 'maint-a', '2025-11-26 13:39:00', null, '36.2'), (null, null, null, 'green', 'maint-b', '2025-11-26 13:40:00', 88.8, '34.9')",
            "INSERT INTO table1(region, plant_id, device_id, model_id, maintenance, time, temperature, humidity) VALUES ('south', '1005', 'd105', null, null, '2025-11-26 13:41:00', 87.5, null)",
            "INSERT INTO table1(region, plant_id, device_id, model_id, maintenance, time, temperature, humidity) VALUES ('west', '1006', 'd106', 'blue', 'maint-c', '2025-11-26 13:42:00', null, '36.8')"),
        BaseEnv.TABLE_SQL_DIALECT);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from iot_table_stream_attr.table1 order by time",
        "time,region,device_id,model_id,maintenance,temperature,humidity,plant_id,",
        new HashSet<>(
            Arrays.asList(
                "2025-11-26T13:38:00.000Z,north,d101,red,null,91.0,null,null,",
                "2025-11-26T13:39:00.000Z,null,null,null,maint-a,null,36.2,1003,",
                "2025-11-26T13:40:00.000Z,null,null,green,maint-b,88.8,34.9,null,",
                "2025-11-26T13:41:00.000Z,south,d105,null,null,87.5,null,1005,",
                "2025-11-26T13:42:00.000Z,west,d106,blue,maint-c,null,36.8,1006,")),
        (String) null);
  }
}
