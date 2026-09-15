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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LogicalBackupArchiveReaderTest {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final int MAX_RECORD_BYTES = 1024;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testReadCompleteArchiveAndOrderSchemaFirst() throws Exception {
    final Path root = temporaryFolder.newFolder().toPath();
    writeStream(root.resolve("data"), "data-stream", "data", 4096);
    writeStream(root.resolve("schema"), "schema-stream", "schema", 4096);

    final List<BackupStream> streams = new LogicalBackupArchiveReader().read(root, false);

    Assert.assertEquals(2, streams.size());
    Assert.assertEquals("schema-stream", streams.get(0).getManifest().streamId);
    Assert.assertEquals("data-stream", streams.get(1).getManifest().streamId);
    Assert.assertEquals(1, streams.get(0).getEventGroups().size());
    Assert.assertEquals(4, streams.get(0).getRecords().size());
    Assert.assertEquals(
        request(1),
        streams.get(0).getEventGroups().get(0).getRequests().get(0).toTPipeTransferReq());
  }

  @Test
  public void testManifestCounterMismatchIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    writeStream(directory, "stream", "data", 4096);
    final Path manifestPath = directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest manifest =
        GSON.fromJson(
            Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    manifest.operationCounts.clear();
    Files.writeString(manifestPath, GSON.toJson(manifest), StandardCharsets.UTF_8);

    assertReadFails(directory, false);
  }

  @Test
  public void testUnlistedSegmentIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    writeStream(directory, "stream", "data", 4096);
    Files.copy(onlySegment(directory), directory.resolve("unlisted.pwal"));

    assertReadFails(directory, false);
  }

  @Test
  public void testIncompleteLastSegmentRequiresExplicitOptIn() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    writeStream(directory, "stream", "data", 4096);
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      file.setLength(file.length() - LogicalBackupFormat.SEGMENT_FOOTER_SIZE);
    }

    assertReadFails(directory, false);
    final List<BackupStream> streams = new LogicalBackupArchiveReader().read(directory, true);
    Assert.assertEquals(1, streams.size());
    Assert.assertEquals(1, streams.get(0).getEventGroups().size());
  }

  @Test
  public void testUnsealedNonLastSegmentIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    writeStream(directory, "stream", "data", 500);
    final List<Path> segments = segments(directory);
    Assert.assertTrue(segments.size() > 1);
    try (final RandomAccessFile file = new RandomAccessFile(segments.get(0).toFile(), "rw")) {
      file.setLength(file.length() - LogicalBackupFormat.SEGMENT_FOOTER_SIZE);
    }

    assertReadFails(directory, true);
  }

  @Test
  public void testParentSegmentPathIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    writeStream(directory, "stream", "data", 4096);
    final Path manifestPath = directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest manifest =
        GSON.fromJson(
            Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    manifest.segments.get(0).file = "../outside.pwal";
    Files.writeString(manifestPath, GSON.toJson(manifest), StandardCharsets.UTF_8);

    assertReadFails(directory, false);
  }

  @Test
  public void testSkippedEventRequiresExplicitIncompleteOptIn() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final LogicalBackupManifest manifest = manifest("stream", "data");
    try (final LogicalBackupWriter writer = writer(directory, manifest, 4096)) {
      writer.recordSkippedEvent(1, "unsupported-event");
    }

    assertReadFails(directory, false);
    Assert.assertEquals(1, new LogicalBackupArchiveReader().read(directory, true).size());

    final Path manifestPath = directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest persistedManifest =
        GSON.fromJson(
            Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    persistedManifest.skippedEventCount = 0;
    Files.writeString(manifestPath, GSON.toJson(persistedManifest), StandardCharsets.UTF_8);
    assertReadFails(directory, false);
  }

  @Test
  public void testMixedBackupSessionsAreRejected() throws Exception {
    final Path root = temporaryFolder.newFolder().toPath();
    writeStream(root.resolve("data"), "data-stream", "data", 4096);
    writeStream(root.resolve("schema"), "schema-stream", "schema", 4096);
    final Path manifestPath =
        root.resolve("schema").resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest manifest =
        GSON.fromJson(
            Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    manifest.backupId = "another-backup";
    Files.writeString(manifestPath, GSON.toJson(manifest), StandardCharsets.UTF_8);

    assertReadFails(root, false);
  }

  @Test
  public void testNonReplayableRequestTypeIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, manifest("stream", "data"), 4096)) {
      writer.writeEvent(
          UUID.randomUUID(),
          1,
          Collections.singletonList(
              new TPipeTransferReq()
                  .setVersion((byte) 1)
                  .setType((short) 200)
                  .setBody(ByteBuffer.wrap(new byte[] {1}))),
          "metadata");
    }

    assertReadFails(directory, false);
  }

  @Test
  public void testDuplicateEventGroupIdIsRejected() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final UUID eventGroupId = UUID.randomUUID();
    try (final LogicalBackupWriter writer = writer(directory, manifest("stream", "data"), 4096)) {
      writer.writeEvent(eventGroupId, 1, Collections.singletonList(request(1)), "metadata");
      writer.writeEvent(UUID.randomUUID(), 2, Collections.singletonList(request(2)), "metadata");
      writer.writeEvent(eventGroupId, 3, Collections.singletonList(request(1)), "metadata");
    }

    assertReadFails(directory, false);
  }

  private static void writeStream(
      final Path directory, final String streamId, final String streamType, final long segmentSize)
      throws Exception {
    try (final LogicalBackupWriter writer =
        writer(directory, manifest(streamId, streamType), segmentSize)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "metadata");
      if (segmentSize < 1000) {
        writer.writeEvent(
            UUID.randomUUID(), 2, Collections.singletonList(request(new byte[20])), "metadata");
      }
    }
  }

  private static LogicalBackupManifest manifest(final String streamId, final String streamType) {
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = streamId;
    manifest.streamType = streamType;
    manifest.timestampPrecision = "ms";
    return manifest;
  }

  private static LogicalBackupWriter writer(
      final Path directory, final LogicalBackupManifest manifest, final long segmentSize)
      throws IOException {
    return new LogicalBackupWriter(
        directory,
        manifest,
        segmentSize,
        MAX_RECORD_BYTES,
        LogicalBackupWriter.FsyncPolicy.ALWAYS,
        1,
        1,
        false);
  }

  private static TPipeTransferReq request(final int value) {
    return request(new byte[] {(byte) value});
  }

  private static TPipeTransferReq request(final byte[] body) {
    return new TPipeTransferReq()
        .setVersion((byte) 1)
        .setType((short) 10)
        .setBody(ByteBuffer.wrap(body));
  }

  private static Path onlySegment(final Path directory) throws IOException {
    final List<Path> segments = segments(directory);
    Assert.assertEquals(1, segments.size());
    return segments.get(0);
  }

  private static List<Path> segments(final Path directory) throws IOException {
    try (final java.util.stream.Stream<Path> files = Files.list(directory)) {
      return files
          .filter(path -> path.getFileName().toString().endsWith(".pwal"))
          .sorted()
          .collect(java.util.stream.Collectors.toList());
    }
  }

  private static void assertReadFails(final Path source, final boolean allowIncomplete)
      throws IOException {
    try {
      new LogicalBackupArchiveReader().read(source, allowIncomplete);
      Assert.fail();
    } catch (final IOException expected) {
      // Expected.
    }
  }
}
