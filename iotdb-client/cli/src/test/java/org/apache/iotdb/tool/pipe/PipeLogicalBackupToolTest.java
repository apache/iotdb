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
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class PipeLogicalBackupToolTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDirectoryExportZipVerifyAndDryRun() throws Exception {
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
            new String[] {
              "export",
              "--input",
              source.toString(),
              "--output",
              archive.toString(),
              "--format=binary"
            }));
    Assert.assertTrue(Files.isRegularFile(archive));
    try (final ZipFile zip = new ZipFile(archive.toFile())) {
      Assert.assertNotNull(zip.getEntry(LogicalBackupFormat.MANIFEST_FILE_NAME));
      Assert.assertNotNull(zip.getEntry("segment-00000000000000000000.pwal"));
      Assert.assertNull(zip.getEntry(".backup.lock"));
      Assert.assertEquals(2, zip.size());
    }

    Assert.assertEquals(
        0, PipeLogicalBackupTool.run(new String[] {"verify", "--input", archive.toString()}));
    Assert.assertEquals(
        0,
        PipeLogicalBackupTool.run(
            new String[] {"import", "--input", archive.toString(), "--dry-run"}));

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
  public void testRemovedCommandAliasesAreRejected() throws Exception {
    final Path source = temporaryFolder.getRoot().toPath().resolve("source");
    writeBackup(source);

    for (final String command : Arrays.asList("restore", "stats")) {
      try {
        PipeLogicalBackupTool.run(new String[] {command, "--input", source.toString()});
        Assert.fail();
      } catch (final ParseException expected) {
        // Expected.
      }
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

    PipeLogicalBackupTool.transfer(client, request);
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
    final AtomicBoolean failCheckpointOnce = new AtomicBoolean(true);

    try {
      PipeLogicalBackupTool.importStream(
          checkpoint,
          state,
          stream,
          request -> transferredBodies.add((int) request.getBody()[0]),
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
            request -> transferredBodies.add((int) request.getBody()[0]),
            checkpointState -> PipeLogicalBackupTool.writeCheckpoint(checkpoint, checkpointState)));
    Assert.assertEquals(Arrays.asList(1, 1, 2), transferredBodies);
    Assert.assertNull(durableState.inProgress);
    Assert.assertEquals(
        Long.valueOf(stream.getEventGroups().get(0).getLastSequence()),
        durableState.appliedSequences.get(stream.getManifest().streamId));
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
