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
import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils;
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
import java.sql.SQLException;
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
        // Keep AUTO region expansion tied to the test topology instead of the host CPU count.
        // Dozens of tiny regions exhaust the IT nodes' WAL/load memory and obscure failover tests.
        .setDataRegionPerDataNode(2)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    senderEnv
        .getConfig()
        .getDataNodeConfig()
        // The small test files need little schema-analysis memory. Avoid 4 MiB reservations per
        // concurrent load competing for the 256 MiB IT node's much smaller query memory pool.
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(256 * 1024L)
        .setMetricReporterType(Collections.singletonList("JMX"));

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setDataRegionPerDataNode(2)
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
        .setLoadTsFileAnalyzeSchemaMemorySizeInBytes(256 * 1024L)
        .setMetricReporterType(Collections.singletonList("JMX"));

    executeOnBothEnvs(env -> env.initClusterEnvironment(3, 3, 180));
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
      a.append(nodeWrapper.getIpAndPortString());
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
      // stop() waits for process exit; verify it succeeded before checking failover.
      Assert.assertFalse(receiverEnv.getDataNodeWrapper(0).isAlive());
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

  // Stopping a DataRegion leader and restarting both clusters must preserve the 300 source rows;
  // existing and newly created pipes must continue to apply the inclusive timestamp limit of 300.
  // Read live replication gauges on every replica before flushing/restarting to avoid stale zeros.
  // Retry replica-asynchrony exceptions up to five times, then report the final failure.
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

          insertDataAfterLeaderRestart("test");
          insertDataAfterLeaderRestart("test1");

          TableModelUtils.assertData("test", "test", 0, 300, receiverEnv, handleFailure);
          flushTableDataRegionReplicasAfterReplicationComplete(
              senderEnv, Arrays.asList("test", "test1"));
          flushTableDataRegionReplicasAfterReplicationComplete(
              receiverEnv, Collections.singletonList("test"));
        }

        executeOnBothEnvs(TestUtils::restartCluster);

        try (final SyncConfigNodeIServiceClient client =
            (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
          // Distinguish a source-side write loss during the restart from a p2 transfer failure.
          TableModelUtils.assertData("test1", "test1", 0, 300, senderEnv);

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
          IoTDBTestLogger.logger.warn(
              "Retrying table pipe leader-stop scenario after attempt {}", retry + 1, e);
          this.tearDown();
        } else {
          throw new AssertionError("Pipe replication failed after five leader-stop attempts", e);
        }
      }
    }
  }

  private void insertDataAfterLeaderRestart(final String database) {
    // A Running node can still have stale internal RPC connections on other DataNodes. Retry only
    // those transport failures instead of rebuilding both clusters and replaying the whole case.
    // These inserts use fixed timestamps and values, so retrying a partial batch only overwrites
    // the same rows; flush is also safe to repeat.
    await()
        .pollInSameThread()
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(1, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              try {
                TableModelUtils.insertData(database, database, 200, 300, senderEnv);
              } catch (final RuntimeException e) {
                if (e.getCause() instanceof SQLException sqlException
                    && sqlException.getMessage() != null
                    && (sqlException.getMessage().contains("Connection reset by peer")
                        || sqlException.getMessage().contains("Connection refused"))) {
                  throw new AssertionError(
                      "DataNode internal RPC connections have not recovered after leader restart",
                      e);
                }
                throw e;
              }
            });
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
    // IoTConsensus accepts writes on any replica, including a previous leader during failover.
    // Check every replica's outgoing replication before flushing or restarting the cluster.
    await()
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(2, TimeUnit.MINUTES)
        .untilAsserted(
            () -> {
              try (final SyncConfigNodeIServiceClient client =
                  (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
                final List<TRegionInfo> regionInfoList = showTableDataRegions(databases, client);
                Assert.assertFalse(
                    "No table DataRegion found for databases " + databases,
                    regionInfoList.isEmpty());

                PipeTestUtils.assertDataRegionReplicationComplete(env, regionInfoList);
              }
            });
  }

  private List<TRegionInfo> showTableDataRegionLeaders(
      final List<String> databases, final SyncConfigNodeIServiceClient client) throws TException {
    final List<TRegionInfo> result = new ArrayList<>();
    for (final TRegionInfo regionInfo : showTableDataRegions(databases, client)) {
      if (RegionRoleType.Leader.getRoleType().equals(regionInfo.getRoleType())) {
        result.add(regionInfo);
      }
    }
    return result;
  }

  private List<TRegionInfo> showTableDataRegions(
      final List<String> databases, final SyncConfigNodeIServiceClient client) throws TException {
    final TShowRegionResp showRegionResp =
        client.showRegion(
            new TShowRegionReq()
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .setDatabases(databases)
                .setIsTableModel(true));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), showRegionResp.getStatus().getCode());
    return showRegionResp.getRegionInfoList();
  }

  // After adding a sender DataNode, writes through that node and a restart of both clusters must
  // preserve replication; the existing and new pipe must deliver all 400 rows in their tables.
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

      senderEnv.registerNewDataNode(true);

      final DataNodeWrapper newDataNode =
          senderEnv.getDataNodeWrapper(senderEnv.getDataNodeWrapperList().size() - 1);
      TableModelUtils.insertData("test", "test", 200, 300, senderEnv, newDataNode);
      TableModelUtils.insertData("test1", "test1", 200, 300, senderEnv, newDataNode);
      TableModelUtils.assertData("test", "test", 0, 300, receiverEnv, handleFailure);
    }

    executeOnBothEnvs(TestUtils::restartCluster);

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
