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
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBatchReqV2;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.storageengine.load.active.ActiveLoadPathHelper;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeAutoEnhanced;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.BoundaryProcessor;
import org.apache.iotdb.pipe.it.dual.PipeTransferObserver;
import org.apache.iotdb.pipe.it.dual.PipeTransferObserver.Transfer;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.assertInt32Schema;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.assertRows;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.awaitBacklog;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.awaitDrained;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.awaitRows;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.configure;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.createPipe;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.dropDatabase;
import static org.apache.iotdb.pipe.it.dual.PipeRecoveryTestUtils.execute;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeAutoEnhanced.class})
public class IoTDBPipeTransferRecoveryIT {
  private static BaseEnv senderEnv;
  private static BaseEnv receiverEnv;
  private String pipeName;
  private String database;
  private static Path pluginJar;
  private Path auditFile;
  private String receiverUser;

  @BeforeClass
  public static void setUpClusters() throws Exception {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    configure(senderEnv);
    configure(receiverEnv);
    // Acknowledged source writes must survive the forced process exits in the progress cases.
    senderEnv.getConfig().getCommonConfig().setWalMode("SYNC");
    // Admission control uses one source region and one sink worker throughout these cases.
    senderEnv.initClusterEnvironment(1, 1);
    receiverEnv.initClusterEnvironment(1, 1);
    pluginJar = Files.createTempFile("pipe-boundary-processor-", ".jar");
    final String classFile = BoundaryProcessor.class.getName().replace('.', '/') + ".class";
    try (final JarOutputStream jar = new JarOutputStream(Files.newOutputStream(pluginJar));
        final InputStream bytes =
            BoundaryProcessor.class.getClassLoader().getResourceAsStream(classFile)) {
      Assert.assertNotNull(bytes);
      jar.putNextEntry(new JarEntry(classFile));
      bytes.transferTo(jar);
      jar.closeEntry();
    }
    execute(
        senderEnv,
        "create pipeplugin boundary_processor as '"
            + BoundaryProcessor.class.getName()
            + "' using uri '"
            + pluginJar.toUri()
            + "'");
  }

  @AfterClass
  public static void tearDownClusters() throws Exception {
    try {
      if (senderEnv != null) {
        senderEnv.cleanClusterEnvironment();
      }
    } finally {
      if (receiverEnv != null) {
        receiverEnv.cleanClusterEnvironment();
      }
      if (pluginJar != null) {
        Files.deleteIfExists(pluginJar);
      }
    }
  }

  @After
  public void cleanCase() throws Exception {
    execute(
        receiverEnv,
        "set system to running",
        "set configuration \"enable_auto_create_schema\"=\"true\"");
    if (pipeName != null) {
      execute(senderEnv, "drop pipe if exists " + pipeName);
    }
    if (database != null) {
      dropDatabase(senderEnv, database);
      dropDatabase(receiverEnv, database);
    }
    if (auditFile != null) {
      Files.deleteIfExists(auditFile);
    }
    if (receiverUser != null) {
      execute(receiverEnv, "drop user " + receiverUser);
    }
  }

  @Test
  public void testAcknowledgedAsyncLoadSurvivesReceiverCrashWithoutSenderReplay() throws Exception {
    // A1: the ACK transfers ownership before business loading; a crash must not lose that handoff.
    pipeName = "async_handoff";
    database = "root.pipe_async_handoff";
    final String device = database + ".d1";
    execute(
        senderEnv,
        "create timeseries " + device + ".s1 INT32 encoding=RLE",
        "insert into " + device + "(time,s1) values(1,101),(2,202),(3,303)",
        "flush");
    final DataNodeWrapper receiver = receiverEnv.getDataNodeWrapper(0);
    final Path pending = Path.of(receiver.getNodePath(), "ext", "load", "pending");
    receiverUser = "async_owner";
    execute(
        receiverEnv,
        "create database " + database,
        "create user " + receiverUser + " 'Pipe!Async2026Owner'",
        "grant WRITE_DATA, WRITE_SCHEMA on " + database + ".** to user " + receiverUser);

    // The scanner explicitly skips read-only nodes, but the asynchronous seal can durably take
    // ownership. This public state avoids racing a scanner timer or changing production code.
    execute(receiverEnv, "set system to readonly");
    try (final PipeTransferObserver observer = new PipeTransferObserver(receiver)) {
      Assert.assertTrue("Previous async loads must have drained", pendingFiles(pending).isEmpty());
      final Path saved = pending.resolveSibling("pipe-before-handoff");
      final boolean existing = Files.exists(pending);
      Files.createDirectories(pending.getParent());
      if (existing) {
        Files.move(pending, saved);
      }
      Files.createFile(pending);
      try {
        createPipe(
            senderEnv,
            pipeName,
            database,
            observer.getEndpoint(),
            Map.of(),
            Map.of(),
            Map.of(
                "sink.format",
                "tsfile",
                "sink.load-tsfile-strategy",
                "async",
                "sink.user",
                receiverUser,
                "sink.password",
                "Pipe!Async2026Owner",
                "sink.exception.data.convert-on-type-mismatch",
                "false",
                "sink.exception.others.retry-max-time-seconds",
                "-1"));
        // A regular file at the active-load root prevents durable takeover, while RPC file
        // staging remains available. The real receiver must reject the seal and retain backlog.
        await()
            .atMost(60, TimeUnit.SECONDS)
            .until(
                () ->
                    observer.getTransfers().stream()
                        .anyMatch(
                            t ->
                                isFileSeal(t)
                                    && t.response.getStatus().getCode()
                                        == TSStatusCode.PIPE_TRANSFER_FILE_ERROR.getStatusCode()));
        awaitBacklog(senderEnv, pipeName);
        Assert.assertTrue(pendingFiles(pending).isEmpty());
        assertRows(receiverEnv, "count timeseries " + database + ".**", "count(timeseries),", "0,");
      } finally {
        Files.delete(pending);
        if (existing) {
          Files.move(saved, pending);
        } else {
          Files.createDirectory(pending);
        }
      }
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(
              () ->
                  !pendingFiles(pending).isEmpty()
                      && observer.getTransfers().stream()
                          .anyMatch(
                              t -> isFileSeal(t) && t.response.getStatus().getCode() == success()));
      awaitDrained(senderEnv, pipeName);
      assertRows(receiverEnv, "count timeseries " + database + ".**", "count(timeseries),", "0,");
      final Set<Path> acceptedFiles = pendingFiles(pending);
      Assert.assertFalse("No durable file was accepted", acceptedFiles.isEmpty());
      for (final Path file : acceptedFiles) {
        final Map<String, String> attributes =
            ActiveLoadPathHelper.parseAttributes(file.toFile(), pending.toFile());
        Assert.assertEquals(receiverUser, attributes.get(ActiveLoadPathHelper.USER_KEY));
        Assert.assertEquals(database, attributes.get(LoadTsFileConfigurator.DATABASE_NAME_KEY));
        Assert.assertEquals(
            "false", attributes.get(LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY));
      }
      // Keep handshakes available but prevent data replay until receiver-owned loading finishes.
      // This proves receiver responsibility without treating permitted duplicates as failures.
      observer.blockDataTransfers();

      receiver.stopForcibly();
      try {
        Assert.assertFalse(receiver.isAlive());
        for (final Path file : acceptedFiles) {
          Assert.assertTrue(
              "The accepted file must survive process death: " + file, Files.isRegularFile(file));
        }
        receiverEnv.startDataNode(0);
        ((AbstractEnv) receiverEnv).checkClusterStatusWithoutUnknown();
        execute(receiverEnv, "set system to running");
        awaitRows(
            receiverEnv,
            "select * from " + device,
            "Time," + device + ".s1,",
            "1,101,",
            "2,202,",
            "3,303,");
        assertInt32Schema(receiverEnv, device);
        await().atMost(60, TimeUnit.SECONDS).until(() -> pendingFiles(pending).isEmpty());
        observer.releaseDataTransfers();
        awaitDrained(senderEnv, pipeName);
      } finally {
        if (!receiver.isAlive()) {
          receiverEnv.startDataNode(0);
        }
      }
    }
  }

  @Test
  public void testPartialReceiverBatchRetriesFailedRowsAfterSchemaRepair() throws Exception {
    // A2: a successful substatement must not acknowledge a sibling which still requires retry.
    pipeName = "partial_receiver_batch";
    database = "root.pipe_partial_receiver_batch";
    final String good = database + ".good";
    final String bad = database + ".bad";
    execute(
        senderEnv,
        "create database " + database + " with max_data_region_group_num=1",
        "create timeseries " + good + ".s1 INT32 encoding=RLE",
        "create timeseries " + bad + ".s1 INT32 encoding=RLE");
    execute(
        receiverEnv,
        "create timeseries " + good + ".s1 INT32 encoding=RLE",
        "set configuration \"enable_auto_create_schema\"=\"false\"");

    try (final PipeTransferObserver observer =
        new PipeTransferObserver(receiverEnv.getDataNodeWrapper(0))) {
      createPipe(
          senderEnv,
          pipeName,
          database,
          observer.getEndpoint(),
          Map.of(),
          Map.of(),
          Map.of(
              "sink.batch.enable", "true",
              "sink.batch.size-bytes", "1048576",
              "sink.batch.max-delay-ms", "5000",
              "sink.exception.conflict.resolve-strategy", "retry",
              "sink.exception.conflict.retry-max-time-seconds", "-1",
              "sink.exception.others.retry-max-time-seconds", "-1"));
      // One insertRecords RPC, one source region and one InsertRows event keep the two devices
      // together. This is a fully successful source write, not a source-side partial insert.
      try (final ISession session = senderEnv.getSessionConnection()) {
        session.insertRecords(
            List.of(good, bad),
            List.of(1L, 1L),
            List.of(List.of("s1"), List.of("s1")),
            List.of(List.of(TSDataType.INT32), List.of(TSDataType.INT32)),
            List.of(List.of((Object) 101), List.of((Object) 202)));
      }
      // Also prove both rows committed at the source, independently of the receiving batch.
      assertRows(senderEnv, "select * from " + good, "Time," + good + ".s1,", "1,101,");
      assertRows(senderEnv, "select * from " + bad, "Time," + bad + ".s1,", "1,202,");

      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                final Transfer batch = findMixedBatch(observer, good, bad);
                Assert.assertNotNull(
                    "No single receiver batch contained both business devices", batch);
                // The receiver collapses the missing-schema child into this retryable status.
                // A successful sibling must not turn it into an ACK.
                Assert.assertEquals(
                    TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode(),
                    batch.response.getStatus().getCode());
              });
      awaitRows(receiverEnv, "select * from " + good, "Time," + good + ".s1,", "1,101,");
      assertRows(receiverEnv, "count timeseries " + bad + ".**", "count(timeseries),", "0,");
      awaitBacklog(senderEnv, pipeName);

      execute(receiverEnv, "create timeseries " + bad + ".s1 INT32 encoding=RLE");
      // No source writes or flushes after repair: the same pending batch must make progress.
      awaitRows(receiverEnv, "select * from " + bad, "Time," + bad + ".s1,", "1,202,");
      assertRows(receiverEnv, "select * from " + good, "Time," + good + ".s1,", "1,101,");
      awaitDrained(senderEnv, pipeName);
      assertInt32Schema(receiverEnv, good);
      assertInt32Schema(receiverEnv, bad);
      // Only after the original backlog has recovered, check that later legitimate work is live.
      execute(senderEnv, "insert into " + bad + "(time,s1) values(2,303)");
      awaitRows(receiverEnv, "select * from " + bad, "Time," + bad + ".s1,", "1,202,", "2,303,");
      awaitDrained(senderEnv, pipeName);
    }
  }

  @Test
  public void testReadOnlyReceiverKeepsConnectionAndRetriesBusinessWrites() throws Exception {
    // A3: a read-only business rejection is reported over a live connection.
    pipeName = "readonly_receiver";
    database = "root.pipe_readonly_receiver";
    final String device = database + ".d1";
    execute(senderEnv, "create timeseries " + device + ".s1 INT32 encoding=RLE");
    execute(receiverEnv, "create timeseries " + device + ".s1 INT32 encoding=RLE");
    try (final PipeTransferObserver observer =
        new PipeTransferObserver(receiverEnv.getDataNodeWrapper(0))) {
      createPipe(
          senderEnv, pipeName, database, observer.getEndpoint(), Map.of(), Map.of(), Map.of());
      execute(senderEnv, "insert into " + device + "(time,s1) values(1,11)");
      awaitRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");
      awaitDrained(senderEnv, pipeName);

      execute(receiverEnv, "set system to readonly");
      try {
        // A JDBC query still works, and the exact write-rejection code is checked independently.
        assertRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");
        try (final Connection connection = receiverEnv.getConnection();
            final Statement statement = connection.createStatement()) {
          statement.execute("insert into " + device + "(time,s1) values(999,999)");
          Assert.fail("The receiver accepted a write while read-only");
        } catch (final SQLException e) {
          Assert.assertEquals(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), e.getErrorCode());
        }
        execute(senderEnv, "insert into " + device + "(time,s1) values(2,22)");
        await()
            .atMost(60, TimeUnit.SECONDS)
            .until(
                () ->
                    observer.getTransfers().stream()
                        .anyMatch(
                            t ->
                                containsStatus(
                                    t.response.getStatus(),
                                    TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
                                    "READ_ONLY")));
        awaitBacklog(senderEnv, pipeName);
        assertRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");
      } finally {
        execute(receiverEnv, "set system to running");
      }
      awaitRows(
          receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,", "2,22,");
      awaitDrained(senderEnv, pipeName);
      assertInt32Schema(receiverEnv, device);
    }
  }

  @Test
  public void testZeroOutputReleasesProgressAndRemainsLiveAfterRestart() throws Exception {
    // A4a: an input in an already subscribed, active region may produce no outputs. Its progress
    // must settle without another write, including after recovering the sender's checkpoint.
    pipeName = "zero_output";
    database = "root.pipe_zero_output";
    final String device = database + ".d1";
    auditFile = Files.createTempFile("pipe-zero-output-", ".audit");
    execute(senderEnv, "create timeseries " + device + ".s1 INT32 encoding=RLE");
    createPipe(
        senderEnv,
        pipeName,
        database,
        receiverEnv.getDataNodeWrapper(0).getIpAndPortString(),
        Map.of(),
        Map.of("processor", "boundary_processor", "audit-file", auditFile.toString()),
        Map.of());
    execute(senderEnv, "insert into " + device + "(time,s1) values(1,11)");
    awaitRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");
    awaitDrained(senderEnv, pipeName);
    execute(senderEnv, "insert into " + device + "(time,s1) values(2,-22)");
    // Identify the discarded row itself: completion of the earlier positive input's audit must
    // not satisfy this precondition.
    await()
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> Files.readAllLines(auditFile).contains("filtered:2,-22"));
    final ProgressIndex filteredProgress = auditedProgress("filtered-progress:2,-22:");
    Assert.assertNotEquals(MinimumProgressIndex.INSTANCE, filteredProgress);
    final int regionId = sourceRegionId(device);
    assertRows(senderEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,", "2,-22,");
    awaitDrained(senderEnv, pipeName);
    // Resource counters can reach zero even if the discarded input never reports progress.
    // Require its exact checkpoint before any later write can advance past that missing report.
    await()
        .pollInSameThread()
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> coordinatorProgress(regionId).isEqualOrAfter(filteredProgress));
    assertRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");

    restartSender();
    awaitDrained(senderEnv, pipeName);
    await()
        .pollInSameThread()
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> coordinatorProgress(regionId).isEqualOrAfter(filteredProgress));
    final AtomicLong recovered = new AtomicLong(auditCount());
    // Finite replay is allowed; repeatedly processing the discarded range after drain is not.
    await()
        .atMost(15, TimeUnit.SECONDS)
        .during(3, TimeUnit.SECONDS)
        .until(
            () -> {
              final long count = auditCount();
              return recovered.getAndSet(count) == count;
            });
    assertRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,");
    execute(senderEnv, "insert into " + device + "(time,s1) values(3,33)");
    awaitRows(receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,11,", "3,33,");
    awaitDrained(senderEnv, pipeName);
    assertInt32Schema(receiverEnv, device);
  }

  @Test
  public void testFanOutRetainsParentUntilBothOutputsRecoverAcrossSenderCrash() throws Exception {
    // A4b: acknowledging the first derived event must not commit past an unfinished sibling.
    pipeName = "fan_out";
    database = "root.pipe_fan_out";
    final String input = database + ".d1";
    final String first = input + "_first";
    final String second = input + "_second";
    auditFile = Files.createTempFile("pipe-fan-out-", ".audit");
    execute(senderEnv, "create timeseries " + input + ".s1 INT32 encoding=RLE");
    execute(
        receiverEnv,
        "create timeseries " + first + ".s1 INT32 encoding=RLE",
        "set configuration \"enable_auto_create_schema\"=\"false\"");
    try (final PipeTransferObserver observer =
        new PipeTransferObserver(receiverEnv.getDataNodeWrapper(0))) {
      createPipe(
          senderEnv,
          pipeName,
          database,
          observer.getEndpoint(),
          Map.of(),
          Map.of(
              "processor",
              "boundary_processor",
              "fan-out",
              "true",
              "audit-file",
              auditFile.toString()),
          Map.of(
              "sink.exception.conflict.resolve-strategy",
              "retry",
              "sink.exception.conflict.retry-max-time-seconds",
              "-1",
              "sink.exception.others.retry-max-time-seconds",
              "-1"));
      execute(senderEnv, "insert into " + input + "(time,s1) values(1,55)");
      awaitRows(receiverEnv, "select * from " + first, "Time," + first + ".s1,", "1,55,");
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(
              () ->
                  observer.getTransfers().stream()
                      .anyMatch(
                          t ->
                              containsStatus(
                                  t.response.getStatus(),
                                  TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION
                                      .getStatusCode())));
      awaitBacklog(senderEnv, pipeName);
      assertRows(receiverEnv, "count timeseries " + second + ".**", "count(timeseries),", "0,");
      // The sink can finish both RPCs before the processor appends its final audit entry.
      await()
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> Files.readAllLines(auditFile).contains("processed"));
      final ProgressIndex parentProgress = auditedProgress("parent-progress:");
      Assert.assertNotEquals(MinimumProgressIndex.INSTANCE, parentProgress);
      final int regionId = sourceRegionId(input);
      // The fixture configures 1-second metadata heartbeats. Observe two full rounds for the
      // DataNode and ConfigNode reporters plus a 1-second margin before killing the sender.
      // Otherwise an early parent commit could be hidden by crashing before it is collected.
      await()
          .pollInSameThread()
          .atMost(15, TimeUnit.SECONDS)
          .during(5, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assert.assertFalse(
                      "An unfinished sibling must retain the parent checkpoint",
                      coordinatorProgress(regionId).isEqualOrAfter(parentProgress)));
      restartSender();
      assertRows(senderEnv, "select * from " + input, "Time," + input + ".s1,", "1,55,");
      awaitBacklog(senderEnv, pipeName);
      execute(receiverEnv, "create timeseries " + second + ".s1 INT32 encoding=RLE");
      // No source write or flush follows the repair. Recovery must replay the incomplete parent.
      awaitRows(receiverEnv, "select * from " + first, "Time," + first + ".s1,", "1,55,");
      awaitRows(receiverEnv, "select * from " + second, "Time," + second + ".s1,", "1,55,");
      awaitDrained(senderEnv, pipeName);
      await()
          .pollInSameThread()
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> coordinatorProgress(regionId).isEqualOrAfter(parentProgress));
      assertInt32Schema(receiverEnv, first);
      assertInt32Schema(receiverEnv, second);
    }
  }

  private ProgressIndex auditedProgress(final String prefix) throws Exception {
    try (final Stream<String> lines = Files.lines(auditFile)) {
      return lines
          .filter(line -> line.startsWith(prefix))
          .map(
              line ->
                  ProgressIndexType.deserializeFrom(
                      ByteBuffer.wrap(Base64.getDecoder().decode(line.substring(prefix.length())))))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Missing progress audit " + prefix));
    }
  }

  private static int sourceRegionId(final String device) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new PartialPath(device).getIDeviceID().serialize(bytes);
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final TGetRegionIdResp response =
          client.getRegionId(
              new TGetRegionIdReq(TConsensusGroupType.DataRegion).setDevice(bytes.toByteArray()));
      Assert.assertEquals(success(), response.getStatus().getCode());
      Assert.assertEquals(1, response.getDataRegionIdListSize());
      return response.getDataRegionIdList().get(0).getId();
    }
  }

  private ProgressIndex coordinatorProgress(final int regionId) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final TGetAllPipeInfoResp response = client.getAllPipeInfo();
      Assert.assertEquals(success(), response.getStatus().getCode());
      for (final ByteBuffer serialized : response.getAllPipeInfo()) {
        final PipeMeta meta = PipeMeta.deserialize4Coordinator(serialized.duplicate());
        if (pipeName.equals(meta.getStaticMeta().getPipeName())) {
          final PipeTaskMeta task =
              meta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().get(regionId);
          Assert.assertNotNull("Missing source region task " + regionId, task);
          return task.getProgressIndex();
        }
      }
    }
    throw new AssertionError("Missing pipe " + pipeName);
  }

  private long auditCount() throws Exception {
    try (final Stream<String> lines = Files.lines(auditFile)) {
      return lines.count();
    }
  }

  private static void restartSender() throws Exception {
    final DataNodeWrapper sender = senderEnv.getDataNodeWrapper(0);
    sender.stopForcibly();
    try {
      Assert.assertFalse(sender.isAlive());
    } finally {
      senderEnv.startDataNode(0);
    }
    ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
  }

  @Test
  public void testLowVolumeTailBatchDrainsWithoutAnotherWriteOrFlush() throws Exception {
    // A5: one record is much smaller than 1 MiB; the timer must eventually emit this quiet tail.
    pipeName = "low_volume_tail";
    database = "root.pipe_low_volume_tail";
    final String device = database + ".d1";
    execute(senderEnv, "create timeseries " + device + ".s1 INT32 encoding=RLE");
    final int delaySeconds = 3;
    try (final PipeTransferObserver observer =
        new PipeTransferObserver(receiverEnv.getDataNodeWrapper(0))) {
      createPipe(
          senderEnv,
          pipeName,
          database,
          observer.getEndpoint(),
          Map.of(),
          Map.of(),
          Map.of(
              "sink.batch.enable",
              "true",
              "sink.batch.size-bytes",
              "1048576",
              "sink.batch.max-delay-ms",
              Long.toString(TimeUnit.SECONDS.toMillis(delaySeconds))));
      final long start = System.nanoTime();
      execute(senderEnv, "insert into " + device + "(time,s1) values(1,77)");
      await()
          .pollInSameThread()
          .atMost(delaySeconds + 30, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertRows(
                      receiverEnv, "select * from " + device, "Time," + device + ".s1,", "1,77,"));
      Assert.assertTrue(
          TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) <= delaySeconds + 30);
      // A query can see the inserted row before its RPC returns to the observer. Wait for the
      // sender's acknowledgement before inspecting the completed transfer record.
      awaitDrained(senderEnv, pipeName);
      Assert.assertTrue(
          "The record must have been sent in a sub-threshold batch",
          observer.getTransfers().stream()
              .anyMatch(t -> isBatch(t) && t.request.body.remaining() < 1048576));
      assertInt32Schema(receiverEnv, device);
    }
  }

  private static int success() {
    return TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static boolean containsStatus(final TSStatus status, final int code) {
    return status.getCode() == code
        || (status.isSetSubStatus()
            && status.getSubStatus().stream().anyMatch(s -> containsStatus(s, code)));
  }

  private static boolean containsStatus(
      final TSStatus status, final int code, final String messageFragment) {
    return (status.getCode() == code
            && status.isSetMessage()
            && status
                .getMessage()
                .toUpperCase(Locale.ENGLISH)
                .replace('-', '_')
                .contains(messageFragment.toUpperCase(Locale.ENGLISH)))
        || (status.isSetSubStatus()
            && status.getSubStatus().stream()
                .anyMatch(s -> containsStatus(s, code, messageFragment)));
  }

  private static boolean isBatch(final Transfer transfer) {
    return transfer.request.getType()
        == org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType
            .TRANSFER_TABLET_BATCH_V2
            .getType();
  }

  private static boolean isFileSeal(final Transfer transfer) {
    return transfer.request.getType()
        == org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType
            .TRANSFER_TS_FILE_SEAL_WITH_MOD
            .getType();
  }

  private static Transfer findMixedBatch(
      final PipeTransferObserver observer, final String good, final String bad) throws Exception {
    for (final Transfer transfer : observer.getTransfers()) {
      if (!isBatch(transfer) || transfer.response.getStatus().getCode() == success()) {
        continue;
      }
      final Set<String> devices = new HashSet<>();
      for (final InsertBaseStatement statement :
          PipeTransferTabletBatchReqV2.fromTPipeTransferReq(transfer.request.deepCopy())
              .constructStatements()) {
        if (statement instanceof InsertRowsStatement) {
          ((InsertRowsStatement) statement)
              .getInsertRowStatementList()
              .forEach(row -> devices.add(row.getDevicePath().getFullPath()));
        } else if (statement instanceof InsertMultiTabletsStatement) {
          ((InsertMultiTabletsStatement) statement)
              .getInsertTabletStatementList()
              .forEach(tablet -> devices.add(tablet.getDevicePath().getFullPath()));
        } else {
          devices.add(statement.getDevicePath().getFullPath());
        }
      }
      if (devices.contains(good) && devices.contains(bad)) {
        return transfer;
      }
    }
    return null;
  }

  private static Set<Path> pendingFiles(final Path directory) throws Exception {
    if (!Files.exists(directory)) {
      return Set.of();
    }
    try (final Stream<Path> files = Files.walk(directory)) {
      return new HashSet<>(
          files
              .filter(Files::isRegularFile)
              .filter(path -> path.toString().endsWith(".tsfile"))
              .toList());
    }
  }
}
