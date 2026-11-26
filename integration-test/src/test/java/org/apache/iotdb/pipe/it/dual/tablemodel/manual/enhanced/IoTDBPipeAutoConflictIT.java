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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.PipeTestUtils.RetryFlushHandler;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelSharedClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualEnhanced.class})
public class IoTDBPipeAutoConflictIT extends AbstractPipeTableModelSharedClusterIT {

  @Rule public final TestName testName = new TestName();

  private String databaseName() {
    return "conflict_" + Integer.toUnsignedString(testName.getMethodName().hashCode(), 16);
  }

  @Override
  protected void setupConfig() {

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        // Keep multi-region completion/conflict coverage without CPU-based tiny-file expansion.
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(3)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        // Keep multi-region completion/conflict coverage without CPU-based tiny-file expansion.
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(3)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
  }

  // Two IOT-consensus clusters must merge all 600 rows across bidirectional pipes, including
  // the 200 rows written after both clusters restart. Recreating receiver schema must not block
  // sync.
  @Test
  public void testDoubleLivingAutoConflict() throws Exception {
    final long testBodyStartedNanos = System.nanoTime();
    long phaseStartedNanos = testBodyStartedNanos;
    // Double living is two clusters each with a pipe connecting to the other.
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    createDataBaseAndTable(senderEnv);
    createDataBaseAndTable(receiverEnv);
    phaseStartedNanos = logPhaseDuration("create databases and tables", phaseStartedNanos);

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    insertData(databaseName(), "test1", 0, 100, senderEnv);
    phaseStartedNanos = logPhaseDuration("insert initial sender rows [0, 100)", phaseStartedNanos);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", databaseName());
      extractorAttributes.put("__system.sql-dialect", "table");
      // Add this property to avoid making self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");
      extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

      connectorAttributes.put("sink", "iotdb-thrift-sink");
      connectorAttributes.put("sink.batch.enable", "false");
      connectorAttributes.put("sink.ip", receiverIp);
      connectorAttributes.put("sink.port", Integer.toString(receiverPort));

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("p1").getCode());
    }
    phaseStartedNanos =
        logPhaseDuration("create and start sender-to-receiver pipe", phaseStartedNanos);

    insertData(databaseName(), "test1", 100, 200, senderEnv);
    insertData(databaseName(), "test1", 200, 300, receiverEnv);
    phaseStartedNanos = logPhaseDuration("insert bidirectional rows [100, 300)", phaseStartedNanos);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", databaseName());
      extractorAttributes.put("__system.sql-dialect", "table");
      // Add this property to avoid to make self cycle.
      extractorAttributes.put("source.forwarding-pipe-requests", "false");
      extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

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
    insertData(databaseName(), "test1", 300, 400, receiverEnv);
    phaseStartedNanos =
        logPhaseDuration("create reverse pipe and insert rows [300, 400)", phaseStartedNanos);

    // Reuse retry Sessions within this phase and close them before either cluster restarts.
    final long[] preRestartRetryCount = {0};
    final long[] preRestartFlushNanos = {0};
    try (final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {
      TableModelUtils.assertData(
          databaseName(),
          "test1",
          0,
          400,
          receiverEnv,
          failure -> {
            preRestartRetryCount[0]++;
            final long flushStartedNanos = System.nanoTime();
            handleFailure.accept(failure);
            preRestartFlushNanos[0] += System.nanoTime() - flushStartedNanos;
          });
    }
    phaseStartedNanos =
        logPhaseDuration("assert replicated rows [0, 400) before restart", phaseStartedNanos);
    logRetryFlushMetrics("before restart", preRestartRetryCount[0], preRestartFlushNanos[0]);

    // Both restarts are part of the scenario; overlap them and propagate any recovery failure.
    CompletableFuture.allOf(
            CompletableFuture.runAsync(
                () -> TestUtils.restartCluster(senderEnv, restartProbe("sender"))),
            CompletableFuture.runAsync(
                () -> TestUtils.restartCluster(receiverEnv, restartProbe("receiver"))))
        .join();
    phaseStartedNanos =
        logPhaseDuration("restart both clusters and await readiness", phaseStartedNanos);

    insertData(databaseName(), "test1", 400, 500, senderEnv);
    insertData(databaseName(), "test1", 500, 600, receiverEnv);
    phaseStartedNanos = logPhaseDuration("insert post-restart rows [400, 600)", phaseStartedNanos);

    // Recovery must use fresh clients; never carry the pre-restart Sessions into this phase.
    final long[] postRestartRetryCount = {0};
    final long[] postRestartFlushNanos = {0};
    try (final RetryFlushHandler handleFailure = new RetryFlushHandler(senderEnv, receiverEnv)) {
      TableModelUtils.assertData(
          databaseName(),
          "test1",
          0,
          600,
          receiverEnv,
          failure -> {
            postRestartRetryCount[0]++;
            final long flushStartedNanos = System.nanoTime();
            handleFailure.accept(failure);
            postRestartFlushNanos[0] += System.nanoTime() - flushStartedNanos;
          });
    }
    phaseStartedNanos =
        logPhaseDuration("assert replicated rows [0, 600) after restart", phaseStartedNanos);
    logRetryFlushMetrics("after restart", postRestartRetryCount[0], postRestartFlushNanos[0]);
    logPhaseDuration("test body total", testBodyStartedNanos);
  }

  private static long logPhaseDuration(final String phase, final long phaseStartedNanos) {
    final long phaseEndedNanos = System.nanoTime();
    System.out.printf(
        "[IT-PHASE] AutoConflict.%s duration=%.3f s%n",
        phase, (phaseEndedNanos - phaseStartedNanos) / 1_000_000_000.0);
    return System.nanoTime();
  }

  private static BiConsumer<String, Duration> restartProbe(final String cluster) {
    return (phase, duration) ->
        System.out.printf(
            "[IT-PHASE] AutoConflict.restart.%s.%s duration=%.3f s%n",
            cluster, phase, duration.toNanos() / 1_000_000_000.0);
  }

  private static void logRetryFlushMetrics(
      final String phase, final long retryCount, final long flushNanos) {
    System.out.printf(
        "[IT-PHASE] AutoConflict.%s retries=%d cumulativeFlush=%.3f s%n",
        phase, retryCount, flushNanos / 1_000_000_000.0);
  }

  // With test.* table filtering and opposite sink batching modes, each side must receive the
  // other side's disjoint table ranges despite both clusters already containing the table schema.
  @Test
  public void testDoubleLivingAutoConflictTemplate() throws Exception {
    final DataNodeWrapper senderDataNode = senderEnv.getDataNodeWrapper(0);
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    final String senderIp = senderDataNode.getIp();
    final int senderPort = senderDataNode.getPort();
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      createDataBaseAndTable(senderEnv);
      createDataBaseAndTable(receiverEnv);
      insertData(databaseName(), "test", 0, 100, senderEnv);
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", databaseName());
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");
      extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

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

    TableModelUtils.insertData(databaseName(), "test", 100, 200, senderEnv);
    TableModelUtils.insertData(databaseName(), "test1", 200, 300, receiverEnv);

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) receiverEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("source.inclusion", "data.insert");
      extractorAttributes.put("source.inclusion.exclusion", "");
      extractorAttributes.put("table-name", "test.*");
      extractorAttributes.put("capture.table", "true");
      extractorAttributes.put("database-name", databaseName());
      extractorAttributes.put("__system.sql-dialect", "table");
      extractorAttributes.put("source.forwarding-pipe-requests", "false");
      extractorAttributes.put("user", SessionConfig.DEFAULT_USER);

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.batch.enable", "true");
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
    insertData(databaseName(), "test1", 300, 400, senderEnv);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 200),
        databaseName(),
        handleFailure);

    TestUtils.assertDataEventuallyOnEnv(
        senderEnv,
        TableModelUtils.getQuerySql("test1"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(200, 400),
        databaseName(),
        handleFailure);
  }

  private void createDataBaseAndTable(BaseEnv baseEnv) {
    TableModelUtils.createDataBaseAndTable(baseEnv, "test", databaseName());
    TableModelUtils.createDataBaseAndTable(baseEnv, "test1", databaseName());
  }

  private void insertData(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    TableModelUtils.insertData(dataBaseName, tableName, start, end, baseEnv);
  }
}
