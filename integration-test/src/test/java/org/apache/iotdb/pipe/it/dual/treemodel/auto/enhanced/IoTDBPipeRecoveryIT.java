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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionRoleType;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.config.MppCommonConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.model.MultipleFailureException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.pipe.it.dual.treemodel.auto.enhanced.PipeTransferFaultInjector.Mode;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeRecoveryIT {

  private static BaseEnv senderEnv;
  private static BaseEnv receiverEnv;
  private static final int RETRY_QUEUE_THRESHOLD = 2;
  private static final List<String> SINK_PENDING_METRICS =
      Arrays.asList(
          "untransferred_tablet_count",
          "untransferred_tsfile_count",
          "pipe_async_connector_retry_event_queue_size",
          "pipe_pending_handlers_size");
  private final List<String> pipes = new ArrayList<>();
  private final List<DataSet> dataSets = new ArrayList<>();

  @BeforeClass
  public static void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(false);
    for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
      env.getConfig()
          .getCommonConfig()
          .setEnforceStrongPassword(false)
          .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
          .setSchemaReplicationFactor(3)
          .setDataReplicationFactor(3)
          .setDataRegionGroupExtensionPolicy("CUSTOM")
          .setDefaultDataRegionGroupNumPerDatabase(2)
          .setPipeMemoryManagementEnabled(true)
          .setIsPipeEnableMemoryCheck(true)
          .setPipeAutoSplitFullEnabled(false)
          .setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(1)
          .setDatanodeMemoryProportion("3:3:1:1:2:0");
      env.getConfig()
          .getDataNodeConfig()
          .setMetricReporterType(Collections.singletonList("PROMETHEUS"));
      final Properties properties = new Properties();
      // A small admission budget and forced-retry threshold expose pressure with few rows.
      properties.setProperty("pipe_insert_node_queue_memory", "262144");
      properties.setProperty(
          "pipe_async_sink_forced_retry_tablet_event_queue_size",
          Integer.toString(RETRY_QUEUE_THRESHOLD));
      properties.setProperty(
          "pipe_async_sink_forced_retry_total_event_queue_size",
          Integer.toString(RETRY_QUEUE_THRESHOLD));
      properties.setProperty("pipe_sink_rpc_thrift_compression_enabled", "false");
      ((MppCommonConfig) env.getConfig().getDataNodeCommonConfig()).updateProperties(properties);
      env.initClusterEnvironment(1, 3);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    final List<Throwable> failures = new ArrayList<>();
    for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
      if (env != null) {
        try {
          env.cleanClusterEnvironment();
        } catch (final Throwable e) {
          failures.add(e);
        }
      }
    }
    MultipleFailureException.assertEmpty(failures);
  }

  @After
  public void cleanUp() throws Exception {
    // JUnit combines @After failures with the test failure; retain all cleanup failures as well.
    final List<Throwable> failures = new ArrayList<>();
    for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
      try {
        startStoppedDataNodes(env);
      } catch (final Throwable e) {
        failures.add(e);
      }
    }
    for (final String pipe : pipes) {
      try {
        execute(senderEnv, "DROP PIPE IF EXISTS " + pipe);
        assertPipeReleased(pipe);
      } catch (final Throwable e) {
        failures.add(e);
      }
    }
    for (final DataSet dataSet : dataSets) {
      for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
        try {
          execute(env, "DELETE DATABASE " + dataSet.database);
        } catch (final Throwable e) {
          failures.add(e);
        }
      }
    }
    MultipleFailureException.assertEmpty(failures);
  }

  @Test
  public void testAllReceiverDataNodesDownRetainAndDrainBacklogWithoutNewWrites() throws Exception {
    // All endpoints, rather than just one failover candidate, fail after an established transfer.
    final DataSet data = dataSet("receiver_outage");
    final String pipe = createPipe(data, "outage", receiverEndpoints(), "iotdb-thrift-sync-sink");
    data.write(0, 0, 4, senderEnv.getDataNodeWrapper(0));
    assertData(receiverEnv, data);
    assertQuiet(pipe);

    stopAllDataNodes(receiverEnv);
    assertEndpointsUnavailable(receiverEnv);
    data.write(0, 4, 28, senderEnv.getDataNodeWrapper(0));
    execute(senderEnv, "FLUSH");
    assertBacklog(pipe);
    // From this point onward, no writes, endpoint edits, or pipe recreation may help recovery.
    startStoppedDataNodes(receiverEnv);
    assertData(senderEnv, data);
    assertData(receiverEnv, data);
    assertQuiet(pipe);
  }

  @Test
  public void testHistoryRealtimeHandoffSurvivesReceiverRestart() throws Exception {
    // Hold a real historical request before forwarding, proving history is still incomplete when
    // realtime rows and the receiver restart cross the history/realtime boundary.
    final DataSet data = dataSet("handoff");
    for (int batch = 0; batch < 4; batch++) {
      data.write(0, batch * 8, batch * 8 + 8, senderEnv.getDataNodeWrapper(0));
      execute(senderEnv, "FLUSH");
    }
    try (final PipeTransferFaultInjector relay =
        new PipeTransferFaultInjector(
            receiverEnv.getDataNodeWrapper(0), Mode.PAUSE_BEFORE_FORWARD)) {
      final String pipe =
          createPipe(data, "handoff", relay.getEndpoint(), "iotdb-thrift-sync-sink");
      await().atMost(60, TimeUnit.SECONDS).until(() -> relay.getPausedRequests() > 0);
      assertBacklog(pipe);
      Assert.assertEquals(0, visibleRows(receiverEnv, data.device(0)));
      data.write(0, 32, 48, senderEnv.getDataNodeWrapper(1));
      stopAllDataNodes(receiverEnv);
      relay.passThrough();
      await().atMost(60, TimeUnit.SECONDS).until(() -> relay.getForwardingFailures() > 0);
      startStoppedDataNodes(receiverEnv);
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      data.write(0, 48, 56, senderEnv.getDataNodeWrapper(2));
      assertData(senderEnv, data);
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      dropPipe(pipe);
    }
  }

  @Test
  public void testMultipleDataRegionsAndWritersDrainAfterLeaderTransfer() throws Exception {
    // Hold an earlier event while writes cross two DataRegions and the owner of one region moves.
    final DataSet data = dataSet("region_writers");
    final Map<Integer, Integer> deviceRegions = new LinkedHashMap<>();
    final Set<Integer> regions = new HashSet<>();
    final Set<Integer> writers = new HashSet<>();
    for (int device = 0; device < 16 && regions.size() < 2; device++) {
      final int writer = device % 3;
      data.write(device, 0, 4, senderEnv.getDataNodeWrapper(writer));
      writers.add(writer);
      final int region = regionOf(data.device(device));
      deviceRegions.put(device, region);
      regions.add(region);
    }
    Assert.assertEquals("Devices must actually occupy two DataRegions", 2, regions.size());
    Assert.assertTrue("Writes must use multiple DataNodes", writers.size() > 1);
    execute(senderEnv, "FLUSH");
    final int region = deviceRegions.values().iterator().next();
    try (final PipeTransferFaultInjector relay =
        new PipeTransferFaultInjector(
            receiverEnv.getDataNodeWrapper(0), Mode.PAUSE_BEFORE_FORWARD)) {
      final String pipe =
          createPipe(data, "regions", relay.getEndpoint(), "iotdb-thrift-sync-sink");
      await().atMost(60, TimeUnit.SECONDS).until(() -> relay.getPausedRequests() > 0);
      assertBacklog(pipe);

      for (final int device : deviceRegions.keySet()) {
        data.write(device, 4, 12, senderEnv.getDataNodeWrapper(device % 3));
      }
      final TRegionInfo oldLeader = awaitLeader(data.database, region, -1);
      final DataNodeWrapper oldLeaderNode = nodeAtPort(senderEnv, oldLeader.getClientRpcPort());
      // SHOW REGIONS established the exact leader before the stop; require a different Running
      // leader before writing the next range.
      oldLeaderNode.stop();
      Assert.assertFalse("Selected DataRegion leader is still alive", oldLeaderNode.isAlive());
      final TRegionInfo newLeader = awaitLeader(data.database, region, oldLeader.getDataNodeId());
      final DataNodeWrapper newLeaderNode = nodeAtPort(senderEnv, newLeader.getClientRpcPort());
      for (final int device : deviceRegions.keySet()) {
        data.setAlias(device, "s_after_transfer", newLeaderNode);
        data.write(device, 12, 20, newLeaderNode);
      }

      startStoppedDataNodes(senderEnv);
      for (final int device : deviceRegions.keySet()) {
        data.write(device, 20, 24, oldLeaderNode);
      }
      relay.passThrough();
      assertData(senderEnv, data);
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      // Reconcile again after a quiet drain while no more source writes are being produced.
      assertData(receiverEnv, data);
    }
  }

  @Test
  public void testRequestNotDeliveredRetriesWithoutStarvingAnotherPipe() throws Exception {
    assertTransferBoundary(Mode.REJECT_BEFORE_FORWARD, "request_loss");
  }

  @Test
  public void testCommittedWriteWithLostResponseRetriesIdempotently() throws Exception {
    assertTransferBoundary(Mode.LOSE_RESPONSE, "response_loss");
  }

  private void assertTransferBoundary(final Mode mode, final String name) throws Exception {
    // The relay distinguishes a request never forwarded from a successful receiver response never
    // returned. Final-state equality checks IoTDB's idempotent writes, not exactly-once delivery.
    final DataSet data = dataSet(name);
    final DataSet healthy = dataSet(name + "_healthy");
    try (final PipeTransferFaultInjector relay =
        new PipeTransferFaultInjector(receiverEnv.getDataNodeWrapper(0), Mode.PASS)) {
      final String pipe = createPipe(data, name, relay.getEndpoint(), "iotdb-thrift-sync-sink");
      final String healthyPipe =
          createPipe(healthy, name + "_healthy", receiverEndpoints(), "iotdb-thrift-sync-sink");
      data.write(0, 0, 1, senderEnv.getDataNodeWrapper(0));
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      relay.setMode(mode);
      data.write(0, 1, 2, senderEnv.getDataNodeWrapper(0));
      if (mode == Mode.LOSE_RESPONSE) {
        await().atMost(60, TimeUnit.SECONDS).until(() -> relay.getCommittedResponses() > 0);
        assertData(receiverEnv, data);
        execute(receiverEnv, "FLUSH");
        Assert.assertEquals(
            "Response must remain withheld until the flush", 0, relay.getLostResponses());
        relay.allowResponseLoss();
      }
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(
              () ->
                  relay.hasRetriedFailedRequest()
                      && (mode == Mode.LOSE_RESPONSE
                          ? relay.getLostResponses() >= 2
                          : relay.getRejectedRequests() >= 2));
      assertBacklog(pipe);
      if (mode == Mode.REJECT_BEFORE_FORWARD) {
        Assert.assertEquals(1, visibleRows(receiverEnv, data.device(0)));
      }
      healthy.write(0, 0, 8, senderEnv.getDataNodeWrapper(1));
      assertData(receiverEnv, healthy);
      assertQuiet(healthyPipe);
      relay.passThrough();
      assertData(senderEnv, data);
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      assertData(receiverEnv, data);
      final int deliveredRequests = relay.getSuccessfulDataRequests();
      await()
          .during(2, TimeUnit.SECONDS)
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> Assert.assertEquals(deliveredRequests, relay.getSuccessfulDataRequests()));
      dropPipe(pipe);
    }
  }

  @Test
  public void testRuntimeQueueBackpressureDrainsAndReleasesMemory() throws Exception {
    // Unlike creation-time admission rejection, a running async sink reaches its forced-retry
    // threshold while another pipe and ordinary writes continue on the same sender and receiver.
    final DataSet data = dataSet("backpressure");
    final DataSet healthy = dataSet("backpressure_healthy");
    try (final PipeTransferFaultInjector relay =
        new PipeTransferFaultInjector(receiverEnv.getDataNodeWrapper(0), Mode.PASS)) {
      final String pipe =
          createPipe(data, "backpressure", relay.getEndpoint(), "iotdb-thrift-sink");
      final String healthyPipe =
          createPipe(healthy, "backpressure_healthy", receiverEndpoints(), "iotdb-thrift-sink");
      data.write(0, 0, 1, senderEnv.getDataNodeWrapper(0));
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      final double baselineFloatingMemory = floatingMemory(pipe);
      relay.setMode(Mode.REJECT_BEFORE_FORWARD);
      data.write(0, 1, 49, senderEnv.getDataNodeWrapper(0));
      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Assert.assertTrue(relay.getRejectedRequests() > 0);
                Assert.assertTrue(
                    "Async retry queue did not reach its configured forced-retry threshold",
                    metric(pipe, "pipe_async_connector_retry_event_queue_size", true)
                        >= RETRY_QUEUE_THRESHOLD);
                Assert.assertTrue(
                    "No retained floating pipe memory",
                    floatingMemory(pipe) > baselineFloatingMemory);
              });
      assertBacklog(pipe);
      healthy.write(0, 0, 12, senderEnv.getDataNodeWrapper(1));
      assertData(senderEnv, healthy);
      assertData(receiverEnv, healthy);
      assertQuiet(healthyPipe);
      relay.passThrough();
      assertData(receiverEnv, data);
      assertQuiet(pipe);
      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assert.assertEquals(
                      0, metric(pipe, "pipe_async_connector_retry_event_queue_size"), 0));
      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final double memory = floatingMemory(pipe);
                Assert.assertTrue(
                    "Floating pipe memory did not return to its idle bound: "
                        + memory
                        + " > "
                        + baselineFloatingMemory,
                    memory <= baselineFloatingMemory);
              });
      // STOP must preserve this suffix for a later START, without another business write.
      final int failuresBeforeStop = relay.getRejectedRequests();
      relay.setMode(Mode.REJECT_BEFORE_FORWARD);
      data.write(0, 49, 57, senderEnv.getDataNodeWrapper(0));
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> relay.getRejectedRequests() > failuresBeforeStop);
      assertBacklog(pipe);
      execute(senderEnv, "STOP PIPE " + pipe);
      Assert.assertEquals("STOPPED", pipeInfo(pipe).getState());
      await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertNoReceiverRuntime(pipe));
      relay.passThrough();
      execute(senderEnv, "START PIPE " + pipe);
      assertData(senderEnv, data);
      assertData(receiverEnv, data);
      assertQuiet(pipe);

      // DROP also releases a running sink with retained events. These rows are deliberately
      // abandoned only after verifying complete delivery of the earlier ranges above.
      final double floatingMemoryBeforeDrop = floatingMemory(pipe);
      final int failuresBeforeDrop = relay.getRejectedRequests();
      relay.setMode(Mode.REJECT_BEFORE_FORWARD);
      data.write(0, 57, 65, senderEnv.getDataNodeWrapper(0));
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> relay.getRejectedRequests() > failuresBeforeDrop);
      assertBacklog(pipe);
      Assert.assertTrue(
          "No retained floating pipe memory before DROP",
          floatingMemory(pipe) > floatingMemoryBeforeDrop);
      dropPipe(pipe);
      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assert.assertTrue(
                      "DROP did not release floating pipe memory",
                      floatingMemory(pipe) <= floatingMemoryBeforeDrop));
      healthy.write(0, 12, 16, senderEnv.getDataNodeWrapper(2));
      assertData(receiverEnv, healthy);
      assertQuiet(healthyPipe);
    }
  }

  private DataSet dataSet(final String name) throws Exception {
    final DataSet data = new DataSet("root.pipe_recovery_" + name);
    dataSets.add(data);
    // Create the database at both ends so even an early test failure has deterministic cleanup.
    execute(senderEnv, "CREATE DATABASE " + data.database);
    execute(receiverEnv, "CREATE DATABASE " + data.database);
    return data;
  }

  private String createPipe(
      final DataSet data, final String name, final String endpoints, final String sink)
      throws Exception {
    final String pipe = "recovery_" + name;
    pipes.add(pipe);
    final Map<String, String> source = new HashMap<>();
    source.put("source.path", data.database + ".**");
    source.put("source.inclusion", "data.insert,schema.timeseries");
    source.put("source.realtime.mode", "log");
    source.put("source.history.enable", "true");
    source.put("source.realtime.enable", "true");
    source.put("capture.tree", "true");
    source.put("capture.table", "false");
    source.put("user", SessionConfig.DEFAULT_USER);
    final Map<String, String> connector = new HashMap<>();
    connector.put("sink", sink);
    connector.put("sink.node-urls", endpoints);
    connector.put("sink.batch.enable", "false");
    connector.put("sink.parallel.tasks", "1");
    // Keep all requests on the relay when it is used, including after receiver leader discovery.
    connector.put("sink.leader-cache.enable", "false");
    try (final SyncConfigNodeIServiceClient client = leaderClient()) {
      assertSuccess(
          client.createPipe(new TCreatePipeReq(pipe, connector).setExtractorAttributes(source)));
    }
    Assert.assertEquals("RUNNING", pipeInfo(pipe).getState());
    return pipe;
  }

  private static String receiverEndpoints() {
    return receiverEnv.getDataNodeWrapperList().stream()
        .map(DataNodeWrapper::getIpAndPortString)
        .collect(Collectors.joining(","));
  }

  private static void stopAllDataNodes(final BaseEnv env) {
    env.shutdownAllDataNodes();
    for (final DataNodeWrapper node : env.getDataNodeWrapperList()) {
      Assert.assertFalse(
          "Sink endpoint still running: " + node.getIpAndPortString(), node.isAlive());
    }
  }

  private static void assertEndpointsUnavailable(final BaseEnv env) throws IOException {
    for (final DataNodeWrapper node : env.getDataNodeWrapperList()) {
      try (final Socket socket = new Socket()) {
        try {
          socket.connect(new InetSocketAddress(node.getIp(), node.getPort()), 1000);
          Assert.fail("Sink endpoint is reachable while all receiver nodes are stopped: " + node);
        } catch (final IOException expected) {
          // Connection refusal or timeout confirms that this endpoint cannot receive Pipe data.
        }
      }
    }
  }

  private static void startStoppedDataNodes(final BaseEnv env) {
    for (final DataNodeWrapper node : env.getDataNodeWrapperList()) {
      if (!node.isAlive()) {
        node.start();
      }
    }
    env.ensureNodeStatus(
        new ArrayList<>(env.getDataNodeWrapperList()),
        Collections.nCopies(env.getDataNodeWrapperList().size(), NodeStatus.Running));
  }

  private static SyncConfigNodeIServiceClient leaderClient() throws Exception {
    return (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection();
  }

  private static TShowPipeInfo pipeInfo(final String pipe) throws Exception {
    try (final SyncConfigNodeIServiceClient client = leaderClient()) {
      final TShowPipeResp response =
          client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
      assertSuccess(response.getStatus());
      return response.getPipeInfoList().stream()
          .filter(info -> pipe.equals(info.getId()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Missing pipe " + pipe));
    }
  }

  private static void assertBacklog(final String pipe) {
    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final TShowPipeInfo info = pipeInfo(pipe);
              Assert.assertTrue("No pending events: " + info, info.getRemainingEventCount() > 0);
            });
  }

  private static void assertQuiet(final String pipe) {
    await()
        .atMost(90, TimeUnit.SECONDS)
        .during(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final TShowPipeInfo info = pipeInfo(pipe);
              Assert.assertEquals(info.toString(), "RUNNING", info.getState());
              Assert.assertTrue(info.toString(), info.isSetRemainingEventCount());
              Assert.assertEquals(info.toString(), 0, info.getRemainingEventCount());
              final Map<String, List<Double>> metrics = sinkMetrics(pipe);
              for (final String name : SINK_PENDING_METRICS) {
                Assert.assertTrue(
                    "Missing sink metric " + name + " for " + pipe, metrics.containsKey(name));
                Assert.assertEquals(
                    "Pending sink work for " + pipe + ": " + name,
                    0,
                    metrics.get(name).stream().mapToDouble(Double::doubleValue).sum(),
                    0);
              }
            });
  }

  private static void assertData(final BaseEnv env, final DataSet data) {
    await()
        .atMost(90, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final Connection connection = env.getConnection();
                  final Statement statement = connection.createStatement()) {
                for (final Map.Entry<Integer, List<String>> entry : data.rows.entrySet()) {
                  final String device = data.device(entry.getKey());
                  try (final ResultSet result =
                      statement.executeQuery("SELECT s, payload FROM " + device)) {
                    TestUtils.assertResultSetEqualUnordered(
                        result, "Time," + device + ".s," + device + ".payload,", entry.getValue());
                  }
                }
                final List<String> schema = new ArrayList<>();
                for (final int device : data.rows.keySet()) {
                  schema.add(
                      data.device(device)
                          + ".s,"
                          + data.aliases.getOrDefault(device, "null")
                          + ","
                          + data.database
                          + ",INT32,PLAIN,SNAPPY,null,null,null,null,BASE,");
                  schema.add(
                      data.device(device)
                          + ".payload,null,"
                          + data.database
                          + ",TEXT,PLAIN,SNAPPY,null,null,null,null,BASE,");
                }
                try (final ResultSet result =
                    statement.executeQuery("SHOW TIMESERIES " + data.database + ".**")) {
                  TestUtils.assertResultSetEqualUnordered(
                      result,
                      "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
                      schema);
                }
              } catch (final SQLException e) {
                throw new AssertionError("Cannot reconcile " + data.database, e);
              }
            });
  }

  private static int visibleRows(final BaseEnv env, final String device) throws Exception {
    int rows = 0;
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery("SELECT s FROM " + device)) {
      while (result.next()) {
        rows++;
      }
    }
    return rows;
  }

  private static int regionOf(final String device) throws Exception {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    IDeviceID.Factory.DEFAULT_FACTORY.create(device).serialize(buffer);
    try (final SyncConfigNodeIServiceClient client = leaderClient()) {
      final TGetRegionIdResp response =
          client.getRegionId(
              new TGetRegionIdReq(TConsensusGroupType.DataRegion).setDevice(buffer.toByteArray()));
      assertSuccess(response.getStatus());
      Assert.assertEquals(response.toString(), 1, response.getDataRegionIdListSize());
      return response.getDataRegionIdList().get(0).getId();
    }
  }

  private static TRegionInfo awaitLeader(
      final String database, final int region, final int excludedNode) {
    final TRegionInfo[] leader = new TRegionInfo[1];
    await()
        .atMost(90, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              try (final SyncConfigNodeIServiceClient client = leaderClient()) {
                final TShowRegionResp response =
                    client.showRegion(
                        new TShowRegionReq()
                            .setConsensusGroupType(TConsensusGroupType.DataRegion)
                            .setDatabases(Collections.singletonList(database)));
                assertSuccess(response.getStatus());
                leader[0] =
                    response.getRegionInfoList().stream()
                        .filter(info -> info.getConsensusGroupId().getId() == region)
                        .filter(
                            info -> RegionRoleType.Leader.getRoleType().equals(info.getRoleType()))
                        .filter(info -> NodeStatus.Running.getStatus().equals(info.getStatus()))
                        .filter(info -> info.getDataNodeId() != excludedNode)
                        .findFirst()
                        .orElse(null);
                Assert.assertNotNull("No new leader: " + response, leader[0]);
              }
            });
    return leader[0];
  }

  private static DataNodeWrapper nodeAtPort(final BaseEnv env, final int port) {
    return env.getDataNodeWrapperList().stream()
        .filter(node -> node.getPort() == port)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Unknown DataNode port " + port));
  }

  private static double metric(final String pipe, final String metricName) {
    return metric(pipe, metricName, false);
  }

  private static double metric(final String pipe, final String metricName, final boolean maximum) {
    final List<Double> values = sinkMetrics(pipe).get(metricName);
    Assert.assertNotNull("No " + metricName + " metric for " + pipe, values);
    return maximum
        ? values.stream().mapToDouble(Double::doubleValue).max().orElse(0)
        : values.stream().mapToDouble(Double::doubleValue).sum();
  }

  private static Map<String, List<Double>> sinkMetrics(final String pipe) {
    final Map<String, List<Double>> metrics = new HashMap<>();
    for (final DataNodeWrapper node : senderEnv.getDataNodeWrapperList()) {
      final String content =
          senderEnv.getUrlContent(
              "http://" + node.getIp() + ":" + node.getMetricPort() + "/metrics", null);
      Assert.assertNotNull("Cannot read metrics from " + node.getIpAndPortString(), content);
      for (final String line : content.split("\\R")) {
        final int tags = line.indexOf('{');
        if (tags > 0 && line.contains("pipe=\"" + pipe + "\"")) {
          final double value = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
          metrics.computeIfAbsent(line.substring(0, tags), ignored -> new ArrayList<>()).add(value);
        }
      }
    }
    return metrics;
  }

  private static long ownedMemory(final String pipe) throws Exception {
    long usage = 0;
    for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
      for (final DataNodeWrapper node : env.getDataNodeWrapperList()) {
        try (final Connection connection = env.getConnection(node);
            final Statement statement = connection.createStatement();
            final ResultSet result = statement.executeQuery("SHOW PIPE MEMORY")) {
          while (result.next()) {
            if (pipe.equals(result.getString("pipe_name"))) {
              usage += result.getLong("memory_usage_in_bytes");
            }
          }
        }
      }
    }
    return usage;
  }

  private static double floatingMemory(final String pipe) {
    double usage = 0;
    for (final DataNodeWrapper node : senderEnv.getDataNodeWrapperList()) {
      final String content =
          senderEnv.getUrlContent(
              "http://" + node.getIp() + ":" + node.getMetricPort() + "/metrics", null);
      Assert.assertNotNull("Cannot read metrics from " + node.getIpAndPortString(), content);
      for (final String line : content.split("\\R")) {
        if (line.startsWith("pipe_floating_memory_usage{")
            && line.contains("name=\"" + pipe + "\"")) {
          usage += Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
        }
      }
    }
    return usage;
  }

  private static void assertPipeReleased(final String pipe) {
    await()
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertEquals("Pipe-owned memory remains after DROP", 0, ownedMemory(pipe));
              Assert.assertTrue(
                  "Sink task metrics remain after DROP: " + pipe, sinkMetrics(pipe).isEmpty());
              try (final SyncConfigNodeIServiceClient client = leaderClient()) {
                final TShowPipeResp response =
                    client.showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER));
                assertSuccess(response.getStatus());
                Assert.assertFalse(
                    "Pipe metadata remains after DROP: " + pipe,
                    response.getPipeInfoList().stream()
                        .anyMatch(info -> pipe.equals(info.getId())));
              }
              assertNoReceiverRuntime(pipe);
            });
  }

  private static void assertNoReceiverRuntime(final String pipe) throws SQLException {
    for (final DataNodeWrapper node : receiverEnv.getDataNodeWrapperList()) {
      try (final Connection connection = receiverEnv.getConnection(node);
          final Statement statement = connection.createStatement();
          final ResultSet result = statement.executeQuery("SHOW RECEIVERS")) {
        while (result.next()) {
          final String ids = result.getString("PipeIDs");
          Assert.assertTrue(
              "Receiver runtime remains for " + pipe, ids == null || !ids.contains(pipe + "@"));
        }
      }
    }
  }

  private static void dropPipe(final String pipe) throws Exception {
    execute(senderEnv, "DROP PIPE IF EXISTS " + pipe);
    assertPipeReleased(pipe);
  }

  private static void execute(final BaseEnv env, final String sql) throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void assertSuccess(final TSStatus status) {
    Assert.assertEquals(
        status.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  private static final class DataSet {
    private final String database;
    private final Map<Integer, List<String>> rows = new LinkedHashMap<>();
    private final Map<Integer, String> aliases = new HashMap<>();

    private DataSet(final String database) {
      this.database = database;
    }

    private String device(final int device) {
      return database + ".d" + device;
    }

    private void setAlias(final int device, final String alias, final DataNodeWrapper writer)
        throws Exception {
      try (final Connection connection = senderEnv.getConnection(writer);
          final Statement statement = connection.createStatement()) {
        statement.execute("ALTER TIMESERIES " + device(device) + ".s UPSERT ALIAS=" + alias);
        aliases.put(device, alias);
      }
    }

    private void write(
        final int device, final int start, final int end, final DataNodeWrapper writer)
        throws Exception {
      try (final Connection connection = senderEnv.getConnection(writer);
          final Statement statement = connection.createStatement()) {
        if (!rows.containsKey(device)) {
          statement.execute(
              "CREATE TIMESERIES "
                  + device(device)
                  + ".s WITH DATATYPE=INT32, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
          statement.execute(
              "CREATE TIMESERIES "
                  + device(device)
                  + ".payload WITH DATATYPE=TEXT, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
          rows.put(device, new ArrayList<>());
        }
        for (int time = start; time < end; time++) {
          final int value = device * 1000 + time * 7;
          final String payload = "device_" + device + "_key_" + time + "_value_" + value;
          statement.execute(
              "INSERT INTO "
                  + device(device)
                  + "(time,s,payload) VALUES ("
                  + time
                  + ","
                  + value
                  + ",'"
                  + payload
                  + "')");
          // Record only successful business writes; expected rows never come from a receiver query.
          rows.get(device).add(time + "," + value + "," + payload + ",");
        }
      }
    }
  }
}
