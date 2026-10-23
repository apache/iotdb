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

import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupCatalog;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.db.it.utils.TestUtils;
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

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeLogicalBackupIT extends AbstractPipeDualTreeModelManualIT {

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
      statement.execute("insert into other_table(time,device,reading) values(2,'d1',200)");
      statement.execute("flush");
    }
    final String pipeName = "logical_backup_continuous_table";
    final CompletableFuture<Void> writes =
        CompletableFuture.runAsync(
            () -> {
              try {
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                        () -> {
                          try (final Connection connection = senderEnv.getConnection();
                              final Statement statement = connection.createStatement();
                              final ResultSet result =
                                  statement.executeQuery("show pipe " + pipeName)) {
                            return result.next();
                          }
                        });
                try (final Connection connection = senderEnv.getTableConnection();
                    final Statement statement = connection.createStatement()) {
                  statement.execute("use backup_db");
                  statement.execute(
                      "insert into measurements(time,device,reading) values(2,'d1',20)");
                  statement.execute(
                      "insert into measurements(time,device,reading) values(3,'d1',30)");
                  statement.execute("delete from measurements where time <= 4");
                }
              } catch (final Exception e) {
                throw new CompletionException(e);
              }
            });
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
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
        Instant.now().plusSeconds(20).toString(),
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    writes.get(30, TimeUnit.SECONDS);
    final Path backup = parent.resolve("bounded");
    final LogicalBackupCatalog catalog = LogicalBackupCatalog.load(backup.resolve("manifest.json"));
    Assert.assertEquals("SEALED", catalog.status);
    Assert.assertEquals("continuous", catalog.mode);
    assertLogicalRequests(backup);
    runConnected(receiverEnv, "restore", "--input", backup.toString());
    try (final Connection connection = receiverEnv.getTableConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("use backup_db");
      try (final ResultSet result = statement.executeQuery("select * from measurements")) {
        Assert.assertTrue(result.next());
        Assert.assertEquals(0L, result.getLong("time"));
        Assert.assertEquals("d1", result.getString("device"));
        Assert.assertEquals(0, result.getInt("reading"));
        Assert.assertTrue(result.next());
        Assert.assertEquals(4L, result.getLong("time"));
        Assert.assertEquals("d1", result.getString("device"));
        Assert.assertEquals(400, result.getInt("reading"));
        Assert.assertFalse(result.next());
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
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create database root.logical_backup_delete",
            "create timeseries root.logical_backup_delete.d.s with datatype=INT32,encoding=PLAIN",
            "create timeseries root.logical_backup_delete.d.other with datatype=INT32,encoding=PLAIN",
            "insert into root.logical_backup_delete.d(time,s) values(0,0)",
            "insert into root.logical_backup_delete.d(time,s) values(4,400)",
            "insert into root.logical_backup_delete.d(time,other) values(2,200)",
            "flush"),
        null);
    final String pipeName = "logical_backup_tree_delete";
    final CompletableFuture<Void> writes =
        CompletableFuture.runAsync(
            () -> {
              try {
                Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(
                        () -> {
                          try (final Connection connection = senderEnv.getConnection();
                              final Statement statement = connection.createStatement();
                              final ResultSet result =
                                  statement.executeQuery("show pipe " + pipeName)) {
                            return result.next();
                          }
                        });
                try (final Connection connection = senderEnv.getConnection();
                    final Statement statement = connection.createStatement()) {
                  statement.execute(
                      "insert into root.logical_backup_delete.d(time,s) values(2,20)");
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
    final Path parent = temporaryFolder.getRoot().toPath().toAbsolutePath();
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
        "schema,data",
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
        Instant.now().plusSeconds(20).toString(),
        "--timeout-seconds",
        "180",
        "--poll-interval-seconds",
        "1");
    writes.get(30, TimeUnit.SECONDS);
    final Path backup = parent.resolve("tree-delete");
    assertLogicalRequests(backup);
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
}
