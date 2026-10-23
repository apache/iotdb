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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupCatalog;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecord;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecordType;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupSegmentReader;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.tool.pipe.PipeLogicalBackupTool;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeLogicalBackupIT extends AbstractPipeDualTreeModelManualIT {

  private static final String DELETE_DEVICE = "root.logical_backup_delete.d";
  private static final long CANARY_TIME = 2;
  private static final int CANARY_VALUE = 2000;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  protected void setupConfig() {
    super.setupConfig();
    for (final BaseEnv env : Arrays.asList(senderEnv, receiverEnv)) {
      env.getConfig().getConfigNodeJVMConfig().setInitHeapSize(128).setMaxHeapSize(256);
      env.getConfig().getDataNodeJVMConfig().setInitHeapSize(256).setMaxHeapSize(768);
    }
  }

  @Test
  public void testBoundedContinuousTableCapture() throws Exception {
    try (final Connection connection = senderEnv.getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database backup_db");
      statement.execute("use backup_db");
      statement.execute("create table measurements(device STRING TAG, reading INT32 FIELD)");
      statement.execute("create table other_table(device STRING TAG, reading INT32 FIELD)");
      statement.execute("insert into measurements(time,device,reading) values(1,'d1',10)");
      statement.execute("insert into measurements(time,device,reading) values(4,'d1',40)");
      statement.execute("insert into measurements(time,device,reading) values(2,'d2',220)");
      statement.execute("insert into measurements(time,reading) values(2,222)");
      statement.execute("insert into other_table(time,device,reading) values(2,'d1',200)");
      statement.execute("flush");
    }
    try (final Connection connection = receiverEnv.getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database backup_db");
      statement.execute("use backup_db");
      statement.execute("create table measurements(device STRING TAG, reading INT32 FIELD)");
      statement.execute("create table other_table(device STRING TAG, reading INT32 FIELD)");
      statement.execute("insert into measurements(time,device,reading) values(0,'d1',0)");
      statement.execute("insert into measurements(time,device,reading) values(4,'d1',400)");
      statement.execute("insert into measurements(time,device,reading) values(3,'d2',333)");
      statement.execute("insert into measurements(time,reading) values(3,334)");
      statement.execute("insert into other_table(time,device,reading) values(2,'d1',200)");
      statement.execute("flush");
    }
    final String pipeName = "logical_backup_continuous_table";
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
    final Path backup = parent.resolve("bounded");
    final CompletableFuture<Void> writes =
        CompletableFuture.runAsync(
            () -> {
              try {
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> isPipeRunning(senderEnv, pipeName));
                try (final Connection connection = senderEnv.getTableConnection();
                    final Statement statement = connection.createStatement()) {
                  statement.execute("use backup_db");
                  Awaitility.await()
                      .atMost(30, TimeUnit.SECONDS)
                      .pollInterval(1, TimeUnit.SECONDS)
                      .until(
                          () -> {
                            statement.execute(
                                "insert into measurements(time,device,reading) values(2,'d1',2000)");
                            return hasCommittedCanary(backup, "measurements", "reading");
                          });
                  statement.execute(
                      "insert into measurements(time,device,reading) values(3,'d1',30)");
                  statement.execute("delete from measurements where device = 'd1' and time <= 4");
                  statement.execute(
                      "insert into measurements(time,device,reading) values(3,'d1',300)");
                }
              } catch (final Exception e) {
                throw new CompletionException(e);
              }
            });
    runConnected(
        senderEnv,
        "dump",
        "--backup-dir",
        parent.toString(),
        "--backup-id",
        "bounded",
        "--pipe-name",
        pipeName,
        "--scope",
        "data",
        "--model",
        "table",
        "--include",
        "backup_db/measurements",
        "--start-time",
        "1",
        "--end-time",
        "3",
        "--mode",
        "continuous",
        "--until",
        Instant.now().plusSeconds(45).toString(),
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    writes.get(30, TimeUnit.SECONDS);
    final LogicalBackupCatalog catalog = LogicalBackupCatalog.load(backup.resolve("manifest.json"));
    Assert.assertEquals("SEALED", catalog.status);
    Assert.assertEquals("continuous", catalog.mode);
    try (final Connection connection = senderEnv.getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("use backup_db");
      try (final ResultSet result =
          statement.executeQuery(
              "select time,reading from measurements where device = 'd1' and time = 3")) {
        Assert.assertTrue("The source must retain the rewrite after deletion", result.next());
        Assert.assertEquals(3, result.getLong("time"));
        Assert.assertEquals(300, result.getInt("reading"));
        Assert.assertFalse(result.next());
      }
    }
    assertLogicalRequests(backup);
    assertTableDeleteIsFiltered(backup);
    final List<String> replayTrace = capturedRewriteAndDeletes(backup);
    runConnected(receiverEnv, "restore", "--input", backup.toString());
    try (final Connection connection = receiverEnv.getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("use backup_db");
      try (final ResultSet result = statement.executeQuery("select * from measurements")) {
        final Set<String> rows = new HashSet<>();
        while (result.next()) {
          Assert.assertTrue(
              rows.add(
                  result.getLong("time")
                      + ","
                      + result.getString("device")
                      + ","
                      + result.getInt("reading")));
        }
        Assert.assertEquals(
            "Archived rewrite and deletion order: " + replayTrace,
            new HashSet<>(
                Arrays.asList(
                    "0,d1,0",
                    "2,d2,220",
                    "2,null,222",
                    "3,d1,300",
                    "3,d2,333",
                    "3,null,334",
                    "4,d1,400")),
            rows);
      }
      try (final ResultSet result = statement.executeQuery("select * from other_table")) {
        Assert.assertTrue(result.next());
        Assert.assertEquals(2L, result.getLong("time"));
        Assert.assertEquals("d1", result.getString("device"));
        Assert.assertEquals(200, result.getInt("reading"));
        Assert.assertFalse(result.next());
      }
    }
  }

  @Test
  public void testDumpPackPreflightRestoreWithScopeAndTimeFilters() throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.backup",
            "create aligned timeseries root.backup.d(s1 INT32, s2 BOOLEAN)",
            "insert into root.backup.d(time,s1,s2) aligned values(1,10,true)",
            "insert into root.backup.d(time,s2) aligned values(2,true)",
            "insert into root.backup.d(time,s1,s2) aligned values(3,30,false)",
            "insert into root.backup.d(time,s1,s2) aligned values(4,40,true)",
            "create database root.unselected",
            "create timeseries root.unselected.d.s with datatype=INT32,encoding=PLAIN",
            "insert into root.unselected.d(time,s) values(2,100)",
            "create user logical_backup_excluded_user 'test_password'",
            "flush"),
        null);
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
    runConnected(
        senderEnv,
        "dump",
        "--backup-dir",
        parent.toString(),
        "--backup-id",
        "snapshot",
        "--scope",
        "config,schema,data",
        "--model",
        "tree",
        "--include",
        "root.backup.**",
        "--start-time",
        "2",
        "--end-time",
        "3",
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    final Path backup = parent.resolve("snapshot");
    final LogicalBackupCatalog catalog = LogicalBackupCatalog.load(backup.resolve("manifest.json"));
    Assert.assertEquals("SEALED", catalog.status);
    Assert.assertFalse(catalog.includeAuth);
    Assert.assertFalse(catalog.streams.isEmpty());
    assertLogicalRequests(backup);
    final Path zip = parent.resolve("snapshot.zip");
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(new String[] {"verify", "--input", backup.toString(), "--json"}));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"pack", "--input", backup.toString(), "--output", zip.toString()}));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"restore", "--input", zip.toString(), "--dry-run"}));
    runConnected(receiverEnv, "restore", "--input", zip.toString(), "--dry-run");
    try (final Connection connection = receiverEnv.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery("show timeseries root.backup.**")) {
      Assert.assertFalse(result.next());
    }
    final Path checkpoint = parent.resolve("restore-checkpoint.json");
    runConnected(
        receiverEnv, "restore", "--input", zip.toString(), "--checkpoint", checkpoint.toString());
    runConnected(
        receiverEnv,
        "restore",
        "--input",
        zip.toString(),
        "--checkpoint",
        checkpoint.toString(),
        "--resume");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.backup.**",
        "Time,root.backup.d.s1,root.backup.d.s2,",
        new HashSet<>(Arrays.asList("2,null,true,", "3,30,false,")));
    try (final Connection connection = receiverEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      try (final ResultSet result = statement.executeQuery("show timeseries root.unselected.**")) {
        Assert.assertFalse(result.next());
      }
      try (final ResultSet result = statement.executeQuery("list user")) {
        while (result.next()) {
          for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
            Assert.assertNotEquals("logical_backup_excluded_user", result.getString(column));
          }
        }
      }
    }
    final Path conflictCheckpoint = parent.resolve("restore-conflict-checkpoint.json");
    runConnectedExpectingFailureWithMessage(
        receiverEnv,
        conflictCheckpoint,
        "already been created as database",
        "restore",
        "--input",
        zip.toString(),
        "--checkpoint",
        conflictCheckpoint.toString());
    try (final Connection connection = receiverEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database root.backup");
    }
    runConnected(
        receiverEnv,
        "restore",
        "--input",
        zip.toString(),
        "--checkpoint",
        conflictCheckpoint.toString(),
        "--resume");
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.backup.**",
        "Time,root.backup.d.s1,root.backup.d.s2,",
        new HashSet<>(Arrays.asList("2,null,true,", "3,30,false,")));
    runConnectedExpectingFailureWithMessage(
        senderEnv,
        conflictCheckpoint,
        "checkpoint does not match the source or target",
        "restore",
        "--input",
        zip.toString(),
        "--checkpoint",
        conflictCheckpoint.toString(),
        "--resume");
  }

  @Test
  public void testTreeContinuousCaptureFiltersDeleteByPathAndTime() throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.logical_backup_delete",
            "create timeseries root.logical_backup_delete.d.s with datatype=INT32,encoding=PLAIN",
            "create timeseries root.logical_backup_delete.d.other with datatype=INT32,encoding=PLAIN",
            "insert into root.logical_backup_delete.d(time,s) values(1,10)",
            "insert into root.logical_backup_delete.d(time,s) values(4,40)",
            "insert into root.logical_backup_delete.d(time,other) values(2,200)",
            "flush"),
        null);
    try (final Connection connection = receiverEnv.getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("create database root.logical_backup_delete");
      statement.execute(
          "create timeseries root.logical_backup_delete.d.s with datatype=INT32,encoding=PLAIN");
      statement.execute(
          "create timeseries root.logical_backup_delete.d.other with datatype=INT32,encoding=PLAIN");
      statement.execute("insert into root.logical_backup_delete.d(time,s) values(0,0)");
      statement.execute("insert into root.logical_backup_delete.d(time,s) values(4,400)");
      statement.execute("insert into root.logical_backup_delete.d(time,other) values(2,200)");
      statement.execute("flush");
    }
    final String pipeName = "logical_backup_tree_delete";
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
    final Path backup = parent.resolve("tree-delete");
    final CompletableFuture<Void> writes =
        CompletableFuture.runAsync(
            () -> {
              try {
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> isPipeRunning(senderEnv, pipeName));
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(
                        () -> {
                          try (final Connection connection = senderEnv.getConnection();
                              final Statement statement = connection.createStatement()) {
                            statement.execute(
                                "insert into root.logical_backup_delete.d(time,s) values(2,2000)");
                          }
                          return hasCommittedCanary(backup, DELETE_DEVICE, "s");
                        });
                try (final Connection connection = senderEnv.getConnection();
                    final Statement statement = connection.createStatement()) {
                  statement.execute(
                      "insert into root.logical_backup_delete.d(time,s) values(3,30)");
                  statement.execute(
                      "delete from root.logical_backup_delete.d.s, "
                          + "root.logical_backup_delete.d.other where time <= 4");
                }
              } catch (final Exception e) {
                throw new CompletionException(e);
              }
            });
    runConnected(
        senderEnv,
        "dump",
        "--backup-dir",
        parent.toString(),
        "--backup-id",
        "tree-delete",
        "--pipe-name",
        pipeName,
        "--scope",
        "data",
        "--model",
        "tree",
        "--include",
        "root.logical_backup_delete.d.s",
        "--start-time",
        "1",
        "--end-time",
        "3",
        "--mode",
        "continuous",
        "--until",
        Instant.now().plusSeconds(45).toString(),
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    writes.get(30, TimeUnit.SECONDS);
    assertLogicalRequests(backup);
    assertTreeDeleteIsFiltered(backup);
    runConnected(receiverEnv, "restore", "--input", backup.toString());
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select s from root.logical_backup_delete.d",
        "Time,root.logical_backup_delete.d.s,",
        new HashSet<>(Arrays.asList("0,0,", "4,400,")),
        30);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select other from root.logical_backup_delete.d",
        "Time,root.logical_backup_delete.d.other,",
        new HashSet<>(Arrays.asList("2,200,")),
        30);
  }

  @Test
  public void testInspectPackAndDeprecatedAliasesKeepBackupPortable() throws Exception {
    TestUtils.executeNonQueries(
        senderEnv,
        Arrays.asList(
            "create database root.logical_backup_alias",
            "create timeseries root.logical_backup_alias.d.s with datatype=INT32,encoding=PLAIN",
            "insert into root.logical_backup_alias.d(time,s) values(1,10)",
            "flush"),
        null);
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
    runConnected(
        senderEnv,
        "dump",
        "--backup-dir",
        parent.toString(),
        "--backup-id",
        "portable",
        "--scope",
        "config,schema,data",
        "--model",
        "tree",
        "--include",
        "root.logical_backup_alias.**",
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    final Path backup = parent.resolve("portable");
    final byte[] manifestBeforePack = Files.readAllBytes(backup.resolve("manifest.json"));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"inspect", "--input", backup.toString(), "--json"}));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(new String[] {"verify", "--input", backup.toString(), "--json"}));
    final Path archive = parent.resolve("portable.zip");
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"pack", "--input", backup.toString(), "--output", archive.toString()}));
    final Path aliasArchive = parent.resolve("portable-alias.zip");
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {
              "export", "--input", backup.toString(), "--output", aliasArchive.toString()
            }));
    Assert.assertArrayEquals(
        manifestBeforePack, Files.readAllBytes(backup.resolve("manifest.json")));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"verify", "--input", archive.toString(), "--json"}));
    runConnected(receiverEnv, "import", "--input", aliasArchive.toString());
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "select * from root.logical_backup_alias.d",
        "Time,root.logical_backup_alias.d.s,",
        new HashSet<>(Arrays.asList("1,10,")));
  }

  private static void assertLogicalRequests(final Path backup) throws Exception {
    final Set<Short> logicalRequests =
        Set.of(
            PipeRequestType.TRANSFER_CONFIG_PLAN.getType(),
            PipeRequestType.TRANSFER_PLAN_NODE.getType(),
            PipeRequestType.TRANSFER_TABLET_RAW_V2.getType(),
            PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType());
    new LogicalBackupArchiveReader()
        .read(backup, false).stream()
            .flatMap(stream -> stream.getEventGroups().stream())
            .flatMap(group -> group.getRequests().stream())
            .forEach(
                record -> Assert.assertTrue(logicalRequests.contains(record.getRequestType())));
  }

  private static List<AbstractDeleteDataNode> readDeletes(final Path backup) throws Exception {
    final List<AbstractDeleteDataNode> deletes = new ArrayList<>();
    for (final LogicalBackupArchiveReader.BackupStream stream :
        new LogicalBackupArchiveReader().read(backup, false)) {
      if (!"data".equals(stream.getManifest().streamType)) {
        continue;
      }
      for (final LogicalBackupArchiveReader.EventGroup group : stream.getEventGroups()) {
        for (final LogicalBackupRecord request : group.getRequests()) {
          if (request.getRequestType() == PipeRequestType.TRANSFER_PLAN_NODE.getType()) {
            final PlanNode planNode =
                PipeTransferPlanNodeReq.fromTPipeTransferReq(request.toTPipeTransferReq())
                    .getPlanNode();
            if (planNode instanceof AbstractDeleteDataNode) {
              deletes.add((AbstractDeleteDataNode) planNode);
            }
          }
        }
      }
    }
    Assert.assertFalse(deletes.isEmpty());
    return deletes;
  }

  private static void assertTreeDeleteIsFiltered(final Path backup) throws Exception {
    for (final AbstractDeleteDataNode node : readDeletes(backup)) {
      Assert.assertTrue(node instanceof DeleteDataNode);
      final DeleteDataNode delete = (DeleteDataNode) node;
      Assert.assertEquals(1, delete.getPathList().size());
      Assert.assertEquals(
          "root.logical_backup_delete.d.s", delete.getPathList().get(0).getFullPath());
      Assert.assertEquals(1, delete.getDeleteStartTime());
      Assert.assertEquals(3, delete.getDeleteEndTime());
    }
  }

  private static void assertTableDeleteIsFiltered(final Path backup) throws Exception {
    for (final AbstractDeleteDataNode node : readDeletes(backup)) {
      Assert.assertTrue(node instanceof RelationalDeleteDataNode);
      final RelationalDeleteDataNode delete = (RelationalDeleteDataNode) node;
      Assert.assertEquals("backup_db", delete.getDatabaseName());
      Assert.assertFalse(delete.getModEntries().isEmpty());
      for (final TableDeletionEntry entry : delete.getModEntries()) {
        Assert.assertEquals("measurements", entry.getTableName());
        Assert.assertEquals(1, entry.getStartTime());
        Assert.assertEquals(3, entry.getEndTime());
      }
    }
  }

  private static boolean hasCommittedCanary(
      final Path backup, final String device, final String measurement) throws Exception {
    if (!Files.isDirectory(backup)) {
      return false;
    }
    final LogicalBackupSegmentReader reader =
        new LogicalBackupSegmentReader(LogicalBackupFormat.MAX_RECORD_BYTES);
    try (final Stream<Path> paths = Files.walk(backup)) {
      for (final Path segment :
          paths
              .filter(path -> path.getFileName().toString().endsWith(".pwal"))
              .filter(
                  path ->
                      path.getParent() != null
                          && path.getParent().getFileName().toString().startsWith("data-"))
              .toList()) {
        final LogicalBackupSegmentReader.ScanResult scan;
        try {
          scan = reader.scan(segment, true);
        } catch (final IOException e) {
          // The active segment may be changing while this poll reads it.
          continue;
        }
        final List<LogicalBackupRecord> requests = new ArrayList<>();
        boolean openEventGroup = false;
        for (final LogicalBackupRecord record : scan.getRecords()) {
          if (record.getRecordType() == LogicalBackupRecordType.EVENT_BEGIN) {
            requests.clear();
            openEventGroup = true;
          } else if (record.getRecordType() == LogicalBackupRecordType.PIPE_REQUEST
              && openEventGroup) {
            requests.add(record);
          } else if (record.getRecordType() == LogicalBackupRecordType.EVENT_COMMIT
              && openEventGroup) {
            if (requests.stream()
                .anyMatch(request -> isCanaryRequest(request, device, measurement))) {
              return true;
            }
            requests.clear();
            openEventGroup = false;
          }
        }
      }
    }
    return false;
  }

  private static boolean isCanaryRequest(
      final LogicalBackupRecord record, final String device, final String measurement) {
    return containsPoint(insertStatement(record), device, measurement, CANARY_TIME, CANARY_VALUE);
  }

  private static InsertBaseStatement insertStatement(final LogicalBackupRecord record) {
    if (record.getRequestType() == PipeRequestType.TRANSFER_TABLET_RAW_V2.getType()) {
      return PipeTransferTabletRawReqV2.fromTPipeTransferReq(record.toTPipeTransferReq())
          .constructStatement();
    }
    if (record.getRequestType() == PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType()) {
      return PipeTransferTabletInsertNodeReqV2.fromTPipeTransferReq(record.toTPipeTransferReq())
          .constructStatement();
    }
    return null;
  }

  private static List<String> capturedRewriteAndDeletes(final Path backup) throws Exception {
    final List<String> trace = new ArrayList<>();
    boolean capturedRewrite = false;
    for (final LogicalBackupArchiveReader.BackupStream stream :
        new LogicalBackupArchiveReader().read(backup, false)) {
      if (!"data".equals(stream.getManifest().streamType)) {
        continue;
      }
      for (final LogicalBackupArchiveReader.EventGroup group : stream.getEventGroups()) {
        for (final LogicalBackupRecord request : group.getRequests()) {
          final String position = stream.getManifest().streamId + "#" + request.getSequence();
          if (containsPoint(insertStatement(request), "measurements", "reading", 3, 300)) {
            capturedRewrite = true;
            trace.add(position + " INSERT d1@3=300");
          } else if (request.getRequestType() == PipeRequestType.TRANSFER_PLAN_NODE.getType()) {
            trace.add(
                position
                    + " "
                    + PipeTransferPlanNodeReq.fromTPipeTransferReq(request.toTPipeTransferReq())
                        .getPlanNode());
          }
        }
      }
    }
    Assert.assertTrue("The archive must contain the rewrite: " + trace, capturedRewrite);
    return trace;
  }

  private static boolean containsPoint(
      final InsertBaseStatement statement,
      final String device,
      final String measurement,
      final long expectedTime,
      final int expectedValue) {
    if (statement instanceof InsertTabletStatement) {
      final InsertTabletStatement tablet = (InsertTabletStatement) statement;
      return tablet.getDevicePath() != null
          && device.equals(tablet.getDevicePath().getFullPath())
          && containsPoint(
              tablet.getTimes(),
              tablet.getMeasurements(),
              tablet.getColumns(),
              measurement,
              expectedTime,
              expectedValue);
    }
    if (statement instanceof InsertRowStatement) {
      final InsertRowStatement row = (InsertRowStatement) statement;
      return row.getDevicePath() != null
          && device.equals(row.getDevicePath().getFullPath())
          && containsPoint(
              new long[] {row.getTime()},
              row.getMeasurements(),
              row.getValues(),
              measurement,
              expectedTime,
              expectedValue);
    }
    if (statement instanceof InsertRowsStatement) {
      final List<InsertRowStatement> rows =
          ((InsertRowsStatement) statement).getInsertRowStatementList();
      return rows != null
          && rows.stream()
              .anyMatch(
                  row -> containsPoint(row, device, measurement, expectedTime, expectedValue));
    }
    return false;
  }

  private static boolean containsPoint(
      final long[] times,
      final String[] measurements,
      final Object[] values,
      final String measurement,
      final long expectedTime,
      final int expectedValue) {
    if (times == null || measurements == null || values == null) {
      return false;
    }
    for (int column = 0; column < measurements.length; column++) {
      if (!measurement.equals(measurements[column])) {
        continue;
      }
      if (column >= values.length || values[column] == null) {
        continue;
      }
      final Object valuesColumn = values[column];
      if (valuesColumn instanceof int[]) {
        final int[] intValues = (int[]) valuesColumn;
        for (int row = 0; row < Math.min(times.length, intValues.length); row++) {
          if (times[row] == expectedTime && intValues[row] == expectedValue) {
            return true;
          }
        }
      } else if (valuesColumn instanceof Integer) {
        for (final long time : times) {
          if (time == expectedTime && ((Integer) valuesColumn) == expectedValue) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean isPipeRunning(final BaseEnv env, final String pipeName) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      return client
          .showPipe(new TShowPipeReq().setUserName(SessionConfig.DEFAULT_USER))
          .pipeInfoList
          .stream()
          .filter(pipe -> pipeName.equals(pipe.id))
          .map(TShowPipeInfo::getState)
          .anyMatch("RUNNING"::equals);
    }
  }

  private static void runConnected(final BaseEnv env, final String command, final String... options)
      throws Exception {
    final List<String> args =
        new ArrayList<>(
            Arrays.asList(
                command,
                "--host",
                env.getIP(),
                "--port",
                env.getPort(),
                "--user",
                "root",
                "--password-stdin"));
    args.addAll(Arrays.asList(options));
    final InputStream previous = System.in;
    try {
      System.setIn(new ByteArrayInputStream("root\n".getBytes(StandardCharsets.UTF_8)));
      Assert.assertEquals(0, PipeLogicalBackupTool.run(args.toArray(new String[0])));
    } finally {
      System.setIn(previous);
    }
  }

  private static void runConnectedExpectingFailureWithMessage(
      final BaseEnv env,
      final Path checkpoint,
      final String expectedMessage,
      final String command,
      final String... options) {
    final IOException failure =
        Assert.assertThrows(IOException.class, () -> runConnected(env, command, options));
    Assert.assertTrue(failure.getMessage(), failure.getMessage().contains(expectedMessage));
    Assert.assertTrue(Files.exists(checkpoint));
  }
}
