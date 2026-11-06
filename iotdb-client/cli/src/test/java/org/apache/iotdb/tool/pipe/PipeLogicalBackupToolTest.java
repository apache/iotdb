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

package org.apache.iotdb.tool.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupCatalog;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class PipeLogicalBackupToolTest {

  // CNSnapshotFileType.SCHEMA; ConfigNode is not a CLI dependency.
  private static final String CONFIG_SCHEMA_FILE_TYPE = "4";
  // ConfigPhysicalPlanType.CreateDatabase and CreateSchemaTemplate.
  private static final String CONFIG_SCHEMA_PLAN_TYPES = "200,800";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testBackupTemporaryDirectoryIsPrivate() throws Exception {
    final Path directory = PipeLogicalBackupTool.createPrivateBackupTempDirectory();
    try {
      Assert.assertEquals(
          Path.of(System.getProperty("user.home")).toAbsolutePath().normalize(),
          directory.getParent());
      final PosixFileAttributeView posixView =
          Files.getFileAttributeView(directory, PosixFileAttributeView.class);
      if (posixView != null) {
        Assert.assertEquals(
            EnumSet.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE),
            posixView.readAttributes().permissions());
      }
    } finally {
      Files.deleteIfExists(directory);
    }
  }

  @Test
  public void testDirectoryPackZipVerifyAndRestoreDryRun() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path source = root.resolve("source");
    writeBackup(source);

    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(new String[] {"verify", "--input", source.toString(), "--deep"}));

    final Path archive = root.resolve("backup.zip");
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"pack", "--input", source.toString(), "--output", archive.toString()}));
    Assert.assertTrue(Files.isRegularFile(archive));
    try (final ZipFile zip = new ZipFile(archive.toFile())) {
      Assert.assertNotNull(zip.getEntry(LogicalBackupFormat.MANIFEST_FILE_NAME));
      Assert.assertNotNull(zip.getEntry("legacy-stream/manifest.json"));
      Assert.assertNotNull(zip.getEntry("legacy-stream/segment-00000000000000000000.pwal"));
      Assert.assertNull(zip.getEntry(".backup.lock"));
      Assert.assertEquals(3, zip.size());
    }

    Assert.assertEquals(
        0, PipeLogicalBackupTool.run(new String[] {"verify", "--input", archive.toString()}));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"restore", "--input", archive.toString(), "--dry-run"}));

    final byte[] originalArchive = Files.readAllBytes(archive);
    try {
      PipeLogicalBackupTool.run(
          new String[] {"export", "--input", source.toString(), "--output", archive.toString()});
      Assert.fail();
    } catch (final java.nio.file.FileAlreadyExistsException expected) {
      Assert.assertArrayEquals(originalArchive, Files.readAllBytes(archive));
    }
  }

  @Test
  public void testPackPreservesIncompleteStatus() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path source = root.resolve("incomplete");
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = "data-1";
    manifest.streamType = "data";
    manifest.timestampPrecision = "ms";
    try (final LogicalBackupWriter writer =
        new LogicalBackupWriter(
            source, manifest, 4096, 1024, LogicalBackupWriter.FsyncPolicy.ALWAYS, 1, 1, false)) {
      writer.recordSkippedEvent(1, "unsupported event");
    }
    final Path archive = root.resolve("incomplete.zip");
    try {
      PipeLogicalBackupTool.run(
          new String[] {"pack", "--input", source.toString(), "--output", archive.toString()});
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertFalse(Files.exists(archive));
    }
    final PrintStream previous = System.out;
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(bytes, true, StandardCharsets.UTF_8));
      PipeLogicalBackupTool.run(
          new String[] {
            "pack",
            "--input",
            source.toString(),
            "--output",
            archive.toString(),
            "--allow-incomplete",
            "--json"
          });
      final com.google.gson.JsonObject json =
          new com.google.gson.Gson()
              .fromJson(bytes.toString(StandardCharsets.UTF_8), com.google.gson.JsonObject.class);
      Assert.assertEquals("INCOMPLETE", json.get("status").getAsString());
      bytes.reset();
      PipeLogicalBackupTool.run(
          new String[] {"verify", "--input", archive.toString(), "--allow-incomplete", "--json"});
      final com.google.gson.JsonObject verified =
          new com.google.gson.Gson()
              .fromJson(bytes.toString(StandardCharsets.UTF_8), com.google.gson.JsonObject.class);
      Assert.assertEquals(
          "INCOMPLETE", verified.getAsJsonObject("manifest").get("status").getAsString());
    } finally {
      System.setOut(previous);
    }
  }

  @Test
  public void testCheckpointSourceIdentityIncludesContent() throws Exception {
    final Path source = temporaryFolder.getRoot().toPath().resolve("source");
    writeBackup(source);
    final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
    final Map<String, String> before = PipeLogicalBackupTool.sourceStreamIdentities(streams);

    streams.get(0).getManifest().segments.get(0).sha256 = "different-content";
    final Map<String, String> after = PipeLogicalBackupTool.sourceStreamIdentities(streams);

    Assert.assertNotEquals(before, after);
  }

  @Test
  public void testConfigStreamDetection() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path data = root.resolve("data");
    final Path config = root.resolve("config");
    writeBackup(data);
    writeBackup(config, "config-0", "config", Collections.singletonList(request((short) 200)));

    Assert.assertFalse(
        PipeLogicalBackupTool.containsConfigStream(
            new LogicalBackupArchiveReader().read(data, false)));
    Assert.assertTrue(
        PipeLogicalBackupTool.containsConfigStream(
            new LogicalBackupArchiveReader().read(config, false)));
  }

  @Test
  public void testZipSlipIsRejected() throws Exception {
    final Path archive = temporaryFolder.getRoot().toPath().resolve("unsafe.zip");
    try (final ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
      zip.putNextEntry(new ZipEntry("../outside"));
      zip.write(1);
      zip.closeEntry();
    }

    try {
      PipeLogicalBackupTool.run(new String[] {"verify", "--input", archive.toString()});
      Assert.fail();
    } catch (final IOException expected) {
      // Expected.
    }
  }

  @Test
  public void testPlaintextPasswordOptionIsRejected() throws Exception {
    final Path source = temporaryFolder.getRoot().toPath().resolve("source");
    writeBackup(source);

    try {
      PipeLogicalBackupTool.run(
          new String[] {
            "import", "--source", source.toString(), "--dry-run", "--password", "secret"
          });
      Assert.fail();
    } catch (final ParseException expected) {
      // Expected.
    }
  }

  @Test
  public void testUnknownCommandsAreRejected() throws Exception {
    final Path source = temporaryFolder.getRoot().toPath().resolve("source");
    writeBackup(source);

    for (final String command : Arrays.asList("stats", "generate")) {
      try {
        PipeLogicalBackupTool.run(new String[] {command, "--input", source.toString()});
        Assert.fail();
      } catch (final ParseException expected) {
        // Expected.
      }
    }
  }

  @Test
  public void testRootManifestPackAndJsonVerification() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path source = root.resolve("backup");
    writeBackup(source.resolve("data-1"));
    final LogicalBackupCatalog catalog = new LogicalBackupCatalog();
    catalog.backupId = "backup";
    catalog.pipeName = "pipe";
    catalog.scope = Collections.singletonList("data");
    catalog.model = Collections.singletonList("tree");
    catalog.seal(source, new LogicalBackupArchiveReader().read(source, false));
    final byte[] originalManifest = Files.readAllBytes(source.resolve("manifest.json"));
    final Path archive = root.resolve("rooted.zip");
    PipeLogicalBackupTool.run(
        new String[] {"pack", "--input", source.toString(), "--output", archive.toString()});
    Assert.assertArrayEquals(originalManifest, Files.readAllBytes(source.resolve("manifest.json")));
    try (final ZipFile zip = new ZipFile(archive.toFile())) {
      Assert.assertNotNull(zip.getEntry("manifest.json"));
      Assert.assertNotNull(zip.getEntry("data-1/manifest.json"));
      Assert.assertEquals(3, zip.size());
    }
    final PrintStream previous = System.out;
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(bytes, true, StandardCharsets.UTF_8));
      PipeLogicalBackupTool.run(new String[] {"verify", "--input", archive.toString(), "--json"});
      final com.google.gson.JsonObject json =
          new com.google.gson.Gson()
              .fromJson(bytes.toString(StandardCharsets.UTF_8), com.google.gson.JsonObject.class);
      Assert.assertEquals("passed", json.get("checksum").getAsString());
      Assert.assertEquals("backup", json.getAsJsonObject("manifest").get("backupId").getAsString());
    } finally {
      System.setOut(previous);
    }
  }

  @Test
  public void testDeprecatedAliasesReportMigration() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path source = root.resolve("source");
    writeBackup(source);
    final PrintStream previous = System.err;
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      System.setErr(new PrintStream(bytes, true, StandardCharsets.UTF_8));
      PipeLogicalBackupTool.run(
          new String[] {
            "export",
            "--input",
            source.toString(),
            "--output",
            root.resolve("legacy.zip").toString()
          });
      PipeLogicalBackupTool.run(new String[] {"import", "--input", source.toString(), "--dry-run"});
    } finally {
      System.setErr(previous);
    }
    final String notices = bytes.toString(StandardCharsets.UTF_8);
    Assert.assertTrue(notices.contains("dump"));
    Assert.assertTrue(notices.contains("pack"));
    Assert.assertTrue(notices.contains("restore"));
  }

  @Test
  public void testCommandsRejectOptionsWithOtherResponsibilities() throws Exception {
    final List<String[]> invalid =
        Arrays.asList(
            new String[] {
              "pack", "--input", "backup", "--output", "backup.zip", "--host", "localhost"
            },
            new String[] {"dump", "--input", "backup"},
            new String[] {"dump", "--output", "backup.zip"},
            new String[] {"inspect", "--input", "backup", "--password-stdin"},
            new String[] {"restore", "--input", "backup", "--resume", "checkpoint.json"},
            new String[] {"restore", "--input", "backup", "--conflict", "overwrite"});
    for (final String[] args : invalid) {
      try {
        PipeLogicalBackupTool.run(args);
        Assert.fail(Arrays.toString(args));
      } catch (final ParseException expected) {
        // Command-specific validation must fail before opening files or connections.
      }
    }
  }

  @Test
  public void testDumpScopeAuthAndWriterOptionsAreExplicit() throws Exception {
    final LogicalBackupDumpOptions options =
        new LogicalBackupDumpOptions(
            PipeLogicalBackupTool.parse(
                "dump",
                new String[] {
                  "--scope",
                  "schema,data",
                  "--model",
                  "tree",
                  "--include",
                  "root.a.**",
                  "--fsync-policy",
                  "periodic",
                  "--fsync-period-ms",
                  "42"
                }),
            false);
    Assert.assertEquals("schema,data", options.source.get("source.inclusion"));
    Assert.assertEquals("auth", options.source.get("source.inclusion.exclusion"));
    Assert.assertEquals("false", options.source.get("source.capture.table"));
    Assert.assertEquals("root.a.**", options.source.get("source.path.inclusion"));
    Assert.assertEquals("42", options.sink.get("sink.fsync-period-ms"));
    Assert.assertFalse(options.catalog.includeAuth);
    final LogicalBackupDumpOptions auth =
        new LogicalBackupDumpOptions(
            PipeLogicalBackupTool.parse(
                "dump", new String[] {"--scope", "config", "--include-auth", "true"}),
            false);
    Assert.assertTrue(auth.source.get("source.inclusion").contains("auth"));
  }

  @Test
  public void testDumpTableExclusionsDoNotBroadenCapture() throws Exception {
    final Map<String, List<String>> cases =
        Map.of(
            "backup_skip/.*",
            List.of("backup_keep/measurements", "backup_keep/other_table"),
            ".*/other_table",
            List.of("backup_keep/measurements", "backup_skip/measurements"),
            ".*/.*",
            List.of());
    for (final Map.Entry<String, List<String>> testCase : cases.entrySet()) {
      final LogicalBackupDumpOptions options =
          new LogicalBackupDumpOptions(
              PipeLogicalBackupTool.parse(
                  "dump",
                  new String[] {
                    "--scope",
                    "schema,data",
                    "--model",
                    "table",
                    "--include",
                    "backup_.*/.*",
                    "--exclude",
                    testCase.getKey()
                  }),
              false);
      final TablePattern pattern =
          TablePattern.parsePipePatternFromSourceParameters(new PipeParameters(options.source));
      final List<String> captured = new ArrayList<>();
      for (final String database : List.of("backup_keep", "backup_skip", "unselected")) {
        for (final String table : List.of("measurements", "other_table")) {
          if (pattern.matchesDatabase(database) && pattern.matchesTable(table)) {
            captured.add(database + "/" + table);
          }
        }
      }
      Assert.assertEquals(testCase.getKey(), testCase.getValue(), captured);
    }
  }

  @Test
  public void testDumpTableExcludeAllWithoutInclusion() throws Exception {
    final LogicalBackupDumpOptions options =
        new LogicalBackupDumpOptions(
            PipeLogicalBackupTool.parse(
                "dump", new String[] {"--scope", "data", "--model", "table", "--exclude", ".*/.*"}),
            false);
    final TablePattern pattern =
        TablePattern.parsePipePatternFromSourceParameters(new PipeParameters(options.source));
    Assert.assertFalse(
        pattern.matchesDatabase("backup_db") && pattern.matchesTable("measurements"));
  }

  @Test
  public void testDumpRejectsAmbiguousOrUnboundedCaptureBeforeCreatingTask() throws Exception {
    for (final String[] args :
        Arrays.asList(
            new String[] {},
            new String[] {"--scope", "data", "--mode", "continuous"},
            new String[] {"--scope", "data", "--include-auth", "true"},
            new String[] {"--scope", "data", "--model", "tree,table", "--include", "root.a.**"},
            new String[] {"--scope", "data", "--segment-size-bytes", "1024"},
            new String[] {"--scope", "data", "--fsync-period-ms", "1000"},
            new String[] {"--scope", "data", "--mode", "continuous", "--stop-sequence", "100"})) {
      try {
        new LogicalBackupDumpOptions(PipeLogicalBackupTool.parse("dump", args), false);
        Assert.fail(Arrays.toString(args));
      } catch (final IOException expected) {
        // No unsupported option may broaden capture or leave an unbounded task.
      }
    }
  }

  @Test
  public void testPreflightSendsOnlyHandshakeRequests() throws Exception {
    final Path source = temporaryFolder.getRoot().toPath().resolve("source");
    writeBackup(source, "config-0", "config", Collections.singletonList(request((short) 200)));
    final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
    final IoTDBSyncClient client = Mockito.mock(IoTDBSyncClient.class);
    Mockito.when(client.pipeTransfer(Mockito.any()))
        .thenReturn(
            new TPipeTransferResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    PipeLogicalBackupTool.preflight(client, streams, "root", "secret");
    final org.mockito.ArgumentCaptor<TPipeTransferReq> requests =
        org.mockito.ArgumentCaptor.forClass(TPipeTransferReq.class);
    Mockito.verify(client, Mockito.times(2)).pipeTransfer(requests.capture());
    Assert.assertEquals(
        PipeRequestType.HANDSHAKE_DATANODE_V2.getType(), requests.getAllValues().get(0).getType());
    Assert.assertEquals(
        PipeRequestType.HANDSHAKE_CONFIGNODE_V2.getType(),
        requests.getAllValues().get(1).getType());
  }

  @Test
  public void testFreshRestoreRejectsIdempotentConflict() throws Exception {
    final IoTDBSyncClient client = Mockito.mock(IoTDBSyncClient.class);
    Mockito.when(client.pipeTransfer(Mockito.any()))
        .thenReturn(
            new TPipeTransferResp(
                new TSStatus(
                    TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())));
    try {
      PipeLogicalBackupTool.transfer(client, request(1), false);
      Assert.fail();
    } catch (final IOException expected) {
      // Only the uncertain request at a durable resume point may accept an idempotent conflict.
    }
  }

  @Test
  public void testTransferAcceptsIdempotentConflict() throws Exception {
    final IoTDBSyncClient client = Mockito.mock(IoTDBSyncClient.class);
    final TPipeTransferReq request = request(1);
    Mockito.when(client.pipeTransfer(request))
        .thenReturn(
            new TPipeTransferResp(
                new TSStatus(
                    TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())));

    PipeLogicalBackupTool.transfer(client, request, true);
  }

  @Test
  public void testImportCheckpointResumesAtRequestGranularity() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath();
    final Path source = root.resolve("source");
    writeBackup(source, Arrays.asList(request(1), request(2)));
    final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
    final BackupStream stream = streams.get(0);
    final Path checkpoint = root.resolve("import.checkpoint.json");
    final PipeLogicalBackupTool.ImportCheckpoint state =
        PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
    final List<Integer> transferredBodies = new ArrayList<>();
    final List<Boolean> replayPermissions = new ArrayList<>();
    final AtomicBoolean failCheckpointOnce = new AtomicBoolean(true);

    try {
      PipeLogicalBackupTool.importStream(
          checkpoint,
          state,
          stream,
          (request, allowIdempotentReplay) -> {
            transferredBodies.add((int) request.getBody()[0]);
            replayPermissions.add(allowIdempotentReplay);
          },
          checkpointState -> {
            if (checkpointState.inProgress != null
                && checkpointState.inProgress.nextRequestIndex == 1
                && failCheckpointOnce.getAndSet(false)) {
              throw new IOException("simulated checkpoint failure");
            }
            PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState);
          });
      Assert.fail();
    } catch (final IOException expected) {
      // The request succeeded, but advancing its checkpoint was interrupted.
    }

    Assert.assertEquals(Collections.singletonList(1), transferredBodies);
    final PipeLogicalBackupTool.ImportCheckpoint durableState =
        PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
    Assert.assertNotNull(durableState.inProgress);
    Assert.assertEquals(0, durableState.inProgress.nextRequestIndex);

    Assert.assertEquals(
        1,
        PipeLogicalBackupTool.importStream(
            checkpoint,
            durableState,
            stream,
            (request, allowIdempotentReplay) -> {
              transferredBodies.add((int) request.getBody()[0]);
              replayPermissions.add(allowIdempotentReplay);
            },
            checkpointState -> PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState)));
    Assert.assertEquals(Arrays.asList(1, 1, 2), transferredBodies);
    Assert.assertEquals(Arrays.asList(false, true, false), replayPermissions);
    Assert.assertNull(durableState.inProgress);
    Assert.assertEquals(
        Long.valueOf(stream.getEventGroups().get(0).getLastSequence()),
        durableState.appliedSequences.get(stream.getManifest().streamId));
  }

  @Test
  public void testSnapshotResumeRebuildsAcknowledgedPieces() throws Exception {
    for (final boolean config : Arrays.asList(false, true)) {
      final Path source = temporaryFolder.newFolder().toPath().resolve("backup");
      writeBackup(source, "snapshot", config ? "config" : "schema", snapshotRequests(config));
      final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
      final Path checkpoint = temporaryFolder.getRoot().toPath().resolve(config + ".checkpoint");
      final PipeLogicalBackupTool.ImportCheckpoint state =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      final IoTDBSyncClient firstConnection = snapshotClient(new AtomicBoolean(), config);
      final List<TPipeTransferReq> sent = new ArrayList<>();
      try {
        PipeLogicalBackupTool.importStream(
            checkpoint,
            state,
            streams.get(0),
            (request, allowIdempotentReplay) -> {
              if (sent.size() == 3) {
                throw new IOException("connection interrupted between snapshot pieces");
              }
              PipeLogicalBackupTool.transfer(firstConnection, request, allowIdempotentReplay);
              sent.add(request);
            },
            checkpointState -> PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState));
        Assert.fail();
      } catch (final IOException expected) {
        Assert.assertEquals(3, sent.size());
      }

      final PipeLogicalBackupTool.ImportCheckpoint durableState =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      Assert.assertEquals(3, durableState.inProgress.nextRequestIndex);
      final AtomicBoolean applied = new AtomicBoolean();
      // A fresh handshake has no staged files from the first connection.
      final IoTDBSyncClient freshConnection = snapshotClient(applied, config);
      final List<Boolean> replayPermissions = new ArrayList<>();
      Assert.assertEquals(
          1,
          PipeLogicalBackupTool.importStream(
              checkpoint,
              durableState,
              streams.get(0),
              (request, allowIdempotentReplay) -> {
                replayPermissions.add(allowIdempotentReplay);
                PipeLogicalBackupTool.transfer(freshConnection, request, allowIdempotentReplay);
              },
              checkpointState ->
                  PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState)));
      Assert.assertTrue(applied.get());
      Assert.assertEquals(Arrays.asList(false, false, false, true, false), replayPermissions);
      Mockito.verify(freshConnection, Mockito.times(5)).pipeTransfer(Mockito.any());
    }
  }

  @Test
  public void testSnapshotReconstructionInterruptionPreservesUncertainSeal() throws Exception {
    for (final boolean config : Arrays.asList(false, true)) {
      final Path source = temporaryFolder.newFolder().toPath().resolve("backup");
      writeBackup(source, "snapshot", config ? "config" : "schema", snapshotRequests(config));
      final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
      final Path checkpoint = temporaryFolder.getRoot().toPath().resolve(config + ".checkpoint");
      final PipeLogicalBackupTool.ImportCheckpoint state =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      final AtomicBoolean applied = new AtomicBoolean();
      final IoTDBSyncClient firstConnection = snapshotClient(applied, config);
      try {
        PipeLogicalBackupTool.importStream(
            checkpoint,
            state,
            streams.get(0),
            (request, allowIdempotentReplay) -> {
              PipeLogicalBackupTool.transfer(firstConnection, request, allowIdempotentReplay);
              if (applied.get()) {
                throw new IOException("seal response lost after applying the snapshot");
              }
            },
            checkpointState -> PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState));
        Assert.fail();
      } catch (final IOException expected) {
        Assert.assertTrue(applied.get());
      }

      final PipeLogicalBackupTool.ImportCheckpoint interruptedState =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      Assert.assertEquals(4, interruptedState.inProgress.nextRequestIndex);
      Assert.assertTrue(interruptedState.inProgress.uncertain);
      final IoTDBSyncClient interruptedConnection = snapshotClient(applied, config);
      try {
        PipeLogicalBackupTool.importStream(
            checkpoint,
            interruptedState,
            streams.get(0),
            (request, allowIdempotentReplay) -> {
              Assert.assertFalse(allowIdempotentReplay);
              Assert.assertEquals(
                  config
                      ? PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE.getType()
                      : PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE.getType(),
                  request.getType());
              PipeLogicalBackupTool.transfer(interruptedConnection, request, allowIdempotentReplay);
              throw new IOException("another interruption while reconstructing staged files");
            },
            checkpointState -> PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState));
        Assert.fail();
      } catch (final IOException expected) {
        // Reconstructing temporary files must not replace the durable uncertain seal.
      }
      PipeLogicalBackupTool.writeCheckpoint(checkpoint, interruptedState);
      final PipeLogicalBackupTool.ImportCheckpoint durableState =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      Assert.assertEquals(4, durableState.inProgress.nextRequestIndex);
      Assert.assertTrue(durableState.inProgress.uncertain);
      final IoTDBSyncClient freshConnection = snapshotClient(applied, config);
      final List<Boolean> replayPermissions = new ArrayList<>();
      Assert.assertEquals(
          1,
          PipeLogicalBackupTool.importStream(
              checkpoint,
              durableState,
              streams.get(0),
              (request, allowIdempotentReplay) -> {
                replayPermissions.add(allowIdempotentReplay);
                PipeLogicalBackupTool.transfer(freshConnection, request, allowIdempotentReplay);
              },
              checkpointState ->
                  PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState)));
      Mockito.verify(freshConnection, Mockito.times(5)).pipeTransfer(Mockito.any());
      Assert.assertEquals(Arrays.asList(false, false, false, false, true), replayPermissions);
    }
  }

  @Test
  public void testSnapshotResumeDoesNotReplayAcknowledgedSeal() throws Exception {
    for (final boolean config : new boolean[] {false, true}) {
      final Path source = temporaryFolder.newFolder().toPath().resolve("backup");
      writeBackup(source, "snapshot", config ? "config" : "schema", snapshotRequests(config));
      final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
      final Path checkpoint = temporaryFolder.getRoot().toPath().resolve(config + ".checkpoint");
      final PipeLogicalBackupTool.ImportCheckpoint state =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      final AtomicBoolean applied = new AtomicBoolean();
      final IoTDBSyncClient client = snapshotClient(applied, config);
      try {
        PipeLogicalBackupTool.importStream(
            checkpoint,
            state,
            streams.get(0),
            (request, allowIdempotentReplay) ->
                PipeLogicalBackupTool.transfer(client, request, allowIdempotentReplay),
            checkpointState -> {
              if (checkpointState.inProgress == null) {
                throw new IOException("group completion checkpoint interrupted");
              }
              PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState);
            });
        Assert.fail();
      } catch (final IOException expected) {
        Assert.assertTrue(applied.get());
      }
      final PipeLogicalBackupTool.ImportCheckpoint durableState =
          PipeLogicalBackupTool.readCheckpoint(checkpoint, streams, "localhost", 6667, "root");
      Assert.assertEquals(5, durableState.inProgress.nextRequestIndex);
      Assert.assertFalse(durableState.inProgress.uncertain);
      Assert.assertEquals(
          1,
          PipeLogicalBackupTool.importStream(
              checkpoint,
              durableState,
              streams.get(0),
              (request, allowIdempotentReplay) ->
                  Assert.fail("an acknowledged seal must not be sent again"),
              checkpointState ->
                  PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState)));
      Assert.assertNull(durableState.inProgress);
    }
  }

  private static List<TPipeTransferReq> snapshotRequests(final boolean config) throws IOException {
    if (config) {
      return Arrays.asList(
          configPiece("snapshot", 0, new byte[] {1, 2}),
          configPiece("snapshot", 2, new byte[] {3, 4}),
          configPiece("template", 0, new byte[] {5}),
          configPiece("template", 1, new byte[] {6}),
          ConfigSeal.create());
    }
    return Arrays.asList(
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq("mtree", 0, new byte[] {1, 2}),
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq("mtree", 2, new byte[] {3, 4}),
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq("tags", 0, new byte[] {5}),
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq("tags", 1, new byte[] {6}),
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
            "root.**", ".*", ".*", true, false, "mtree", 4, "tags", 2, null, 0, "root.db", ""));
  }

  private static TPipeTransferReq configPiece(
      final String fileName, final long offset, final byte[] bytes) throws IOException {
    return PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(fileName, offset, bytes)
        .setType(PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE.getType());
  }

  private static IoTDBSyncClient snapshotClient(final AtomicBoolean applied, final boolean config)
      throws Exception {
    final Map<String, ByteArrayOutputStream> stagedFiles = new LinkedHashMap<>();
    final IoTDBSyncClient client = Mockito.mock(IoTDBSyncClient.class);
    Mockito.when(client.pipeTransfer(Mockito.any()))
        .thenAnswer(
            invocation -> {
              final TPipeTransferReq request = invocation.getArgument(0);
              if (request.getType() == PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE.getType()
                  || request.getType()
                      == PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE.getType()) {
                Assert.assertEquals(
                    config
                        ? PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_PIECE.getType()
                        : PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE.getType(),
                    request.getType());
                // Both piece types use the shared PipeTransferFilePieceReq encoding.
                final PipeTransferSchemaSnapshotPieceReq piece =
                    PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(request.deepCopy());
                final ByteArrayOutputStream file =
                    stagedFiles.computeIfAbsent(
                        piece.getFileName(), key -> new ByteArrayOutputStream());
                if (file.size() != piece.getStartWritingOffset()) {
                  return new TPipeTransferResp(
                      new TSStatus(TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()));
                }
                file.write(piece.getFilePiece());
                return new TPipeTransferResp(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
              }
              final List<String> fileNames;
              final List<Long> fileLengths;
              if (config) {
                Assert.assertEquals(
                    PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL.getType(), request.getType());
                final PipeTransferFileSealReqV2 seal =
                    new ConfigSeal().translateFromTPipeTransferReq(request.deepCopy());
                Assert.assertEquals(CONFIG_SCHEMA_FILE_TYPE, seal.getParameters().get("fileType"));
                Assert.assertEquals(
                    CONFIG_SCHEMA_PLAN_TYPES, seal.getParameters().get(ColumnHeaderConstant.TYPE));
                Assert.assertEquals(
                    "root.**", seal.getParameters().get(ColumnHeaderConstant.PATH_PATTERN));
                Assert.assertEquals(
                    ".*", seal.getParameters().get(ColumnHeaderConstant.TABLE_NAME));
                Assert.assertEquals(
                    ".*", seal.getParameters().get(PipeTransferFileSealReqV2.DATABASE_PATTERN));
                Assert.assertEquals("", seal.getParameters().get(PipeTransferFileSealReqV2.TREE));
                Assert.assertFalse(
                    seal.getParameters().containsKey(PipeTransferFileSealReqV2.TABLE));
                Assert.assertEquals("", seal.getParameters().get("authUserName"));
                fileNames = seal.getFileNames();
                fileLengths = seal.getFileLengths();
                Assert.assertEquals(Arrays.asList("snapshot", "template"), fileNames);
                Assert.assertEquals(Arrays.asList(4L, 2L), fileLengths);
              } else {
                Assert.assertEquals(
                    PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL.getType(), request.getType());
                final PipeTransferSchemaSnapshotSealReq seal =
                    PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(request.deepCopy());
                fileNames = seal.getFileNames();
                fileLengths = seal.getFileLengths();
              }
              for (int index = 0; index < fileNames.size(); index++) {
                final ByteArrayOutputStream file = stagedFiles.get(fileNames.get(index));
                if (file == null || file.size() != fileLengths.get(index)) {
                  return new TPipeTransferResp(
                      new TSStatus(TSStatusCode.PIPE_TRANSFER_FILE_ERROR.getStatusCode()));
                }
              }
              Assert.assertArrayEquals(
                  new byte[] {1, 2, 3, 4},
                  stagedFiles.get(config ? "snapshot" : "mtree").toByteArray());
              Assert.assertArrayEquals(
                  new byte[] {5, 6}, stagedFiles.get(config ? "template" : "tags").toByteArray());
              return new TPipeTransferResp(
                  new TSStatus(
                      applied.getAndSet(true)
                          ? TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()
                          : TSStatusCode.SUCCESS_STATUS.getStatusCode()));
            });
    return client;
  }

  private static final class ConfigSeal extends PipeTransferFileSealReqV2 {
    private static TPipeTransferReq create() throws IOException {
      // Match PipeTransferConfigSnapshotSealReq without adding a ConfigNode dependency.
      final Map<String, String> parameters = new LinkedHashMap<>();
      parameters.put(ColumnHeaderConstant.PATH_PATTERN, "root.**");
      parameters.put(DATABASE_PATTERN, ".*");
      parameters.put(ColumnHeaderConstant.TABLE_NAME, ".*");
      parameters.put(TREE, "");
      parameters.put("fileType", CONFIG_SCHEMA_FILE_TYPE);
      parameters.put(ColumnHeaderConstant.TYPE, CONFIG_SCHEMA_PLAN_TYPES);
      parameters.put("authUserName", "");
      return new ConfigSeal()
          .convertToTPipeTransferReq(
              Arrays.asList("snapshot", "template"), Arrays.asList(4L, 2L), parameters);
    }

    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.TRANSFER_CONFIG_SNAPSHOT_SEAL;
    }
  }

  private static void writeBackup(final Path directory) throws Exception {
    writeBackup(directory, Collections.singletonList(request(3)));
  }

  private static void writeBackup(final Path directory, final List<TPipeTransferReq> requests)
      throws Exception {
    writeBackup(directory, "data-1", "data", requests);
  }

  private static void writeBackup(
      final Path directory,
      final String streamId,
      final String streamType,
      final List<TPipeTransferReq> requests)
      throws Exception {
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = streamId;
    manifest.streamType = streamType;
    manifest.timestampPrecision = "ms";
    try (final LogicalBackupWriter writer =
        new LogicalBackupWriter(
            directory, manifest, 4096, 1024, LogicalBackupWriter.FsyncPolicy.ALWAYS, 1, 1, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, requests, "metadata");
    }
  }

  private static TPipeTransferReq request(final int value) {
    return request((short) 10, value);
  }

  private static TPipeTransferReq request(final short type) {
    return request(type, 1);
  }

  private static TPipeTransferReq request(final short type, final int value) {
    return new TPipeTransferReq()
        .setVersion((byte) 1)
        .setType(type)
        .setBody(ByteBuffer.wrap(new byte[] {(byte) value}));
  }
}
