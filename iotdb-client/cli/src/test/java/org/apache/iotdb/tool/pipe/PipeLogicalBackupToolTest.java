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

import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

  private static void writeBackup(final Path directory) throws Exception {
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = "data-1";
    manifest.streamType = "data";
    manifest.timestampPrecision = "ms";
    try (final LogicalBackupWriter writer =
        new LogicalBackupWriter(
            directory, manifest, 4096, 1024, LogicalBackupWriter.FsyncPolicy.ALWAYS, 1, 1, false)) {
      writer.writeEvent(
          UUID.randomUUID(),
          1,
          Collections.singletonList(
              new TPipeTransferReq()
                  .setVersion((byte) 1)
                  .setType((short) 10)
                  .setBody(ByteBuffer.wrap(new byte[] {3}))),
          "metadata");
    }
  }
}
