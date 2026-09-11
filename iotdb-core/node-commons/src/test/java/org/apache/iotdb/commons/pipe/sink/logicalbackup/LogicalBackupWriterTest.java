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
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LogicalBackupWriterTest {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final int MAX_RECORD_BYTES = 1024;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRoundTripAndDuplicateEvent() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final UUID eventId = UUID.randomUUID();
    final TPipeTransferReq request = request((byte) 1, (short) 2, new byte[] {3, 4, 5});

    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      Assert.assertEquals(
          0, writer.writeEvent(eventId, 123, Collections.singletonList(request), "m"));
      Assert.assertEquals(
          0, writer.writeEvent(eventId, 123, Collections.singletonList(request), "m"));
    }

    final LogicalBackupSegmentReader.ScanResult result = scanOnlySegment(directory);
    Assert.assertTrue(result.isSealed());
    Assert.assertEquals(4, result.getRecords().size());
    Assert.assertEquals(
        LogicalBackupRecordType.EVENT_BEGIN, result.getRecords().get(0).getRecordType());
    Assert.assertEquals(
        LogicalBackupRecordType.PIPE_REQUEST, result.getRecords().get(1).getRecordType());
    Assert.assertEquals(request, result.getRecords().get(1).toTPipeTransferReq());
    Assert.assertEquals(
        LogicalBackupRecordType.EVENT_COMMIT, result.getRecords().get(2).getRecordType());
    Assert.assertEquals(
        LogicalBackupRecordType.STREAM_END, result.getRecords().get(3).getRecordType());
    Assert.assertEquals(0, result.getFooter().getFirstSequence());
    Assert.assertEquals(3, result.getFooter().getLastSequence());
  }

  @Test
  public void testDuplicateEventWithDifferentDigestFails() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final UUID eventId = UUID.randomUUID();
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(eventId, 1, Collections.singletonList(request(1)), "");
      try {
        writer.writeEvent(eventId, 1, Collections.singletonList(request(2)), "");
        Assert.fail();
      } catch (final IOException expected) {
        Assert.assertTrue(expected.getMessage().contains(eventId.toString()));
      }
    }
  }

  @Test
  public void testOnlineRollbackAfterPartialEvent() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      final List<TPipeTransferReq> requests =
          new AbstractList<TPipeTransferReq>() {
            private int accesses;

            @Override
            public TPipeTransferReq get(final int index) {
              if (++accesses == 6) {
                throw new IllegalStateException();
              }
              return request(index + 1);
            }

            @Override
            public int size() {
              return 2;
            }
          };
      try {
        writer.writeEvent(UUID.randomUUID(), 1, requests, "");
        Assert.fail();
      } catch (final IllegalStateException expected) {
        // Expected.
      }
      Assert.assertEquals(
          0,
          writer.writeEvent(
              UUID.randomUUID(), 2, Collections.singletonList(request(3)), "after-rollback"));
    }

    final List<LogicalBackupRecord> records = scanOnlySegment(directory).getRecords();
    Assert.assertEquals(4, records.size());
    Assert.assertEquals(0, records.get(0).getSequence());
    Assert.assertEquals("after-rollback", records.get(0).getMetadata());
  }

  @Test
  public void testRecoverIncompleteEventAndRebuildCounts() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
    }
    final Path segment = onlySegment(directory);
    final long beginAndRequestLength =
        LogicalBackupFormat.SEGMENT_HEADER_SIZE + frameSize(0, 0) + frameSize(0, 1);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      file.setLength(beginAndRequestLength);
    }

    try (final LogicalBackupWriter writer = writer(directory, 4096, true)) {
      Assert.assertEquals(
          0, writer.writeEvent(UUID.randomUUID(), 2, Collections.singletonList(request(2)), ""));
      Assert.assertTrue(writer.getManifest().recovered);
      Assert.assertEquals(Long.valueOf(1), writer.getManifest().operationCounts.get("EVENT_BEGIN"));
      Assert.assertEquals(
          Long.valueOf(1), writer.getManifest().operationCounts.get("PIPE_REQUEST"));
      Assert.assertEquals(
          Long.valueOf(1), writer.getManifest().operationCounts.get("EVENT_COMMIT"));
    }
  }

  @Test
  public void testRecoverIncompleteFrame() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
    }
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      file.setLength(file.length() - LogicalBackupFormat.SEGMENT_FOOTER_SIZE - 2);
    }

    try (final LogicalBackupWriter writer = writer(directory, 4096, true)) {
      Assert.assertTrue(writer.getManifest().recovered);
      Assert.assertEquals(2, writer.getManifest().lastSequence);
    }
  }

  @Test
  public void testRecoverPartialFooter() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
    }
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      file.setLength(file.length() - LogicalBackupFormat.SEGMENT_FOOTER_SIZE + 8);
    }

    try (final LogicalBackupWriter writer = writer(directory, 4096, true)) {
      Assert.assertTrue(writer.getManifest().recovered);
      Assert.assertEquals(
          4, writer.writeEvent(UUID.randomUUID(), 2, Collections.singletonList(request(2)), ""));
    }
    Assert.assertTrue(
        new LogicalBackupSegmentReader(MAX_RECORD_BYTES)
            .scan(segments(directory).get(segments(directory).size() - 1), false)
            .isSealed());
  }

  @Test
  public void testRecoverCorruptFooterWhenManifestStillMarksSegmentActive() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
    }
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      final long lastByteOffset = file.length() - 1;
      file.seek(lastByteOffset);
      final int lastByte = file.readUnsignedByte();
      file.seek(lastByteOffset);
      file.writeByte(lastByte ^ 1);
    }
    final Path manifestPath = directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest manifest =
        GSON.fromJson(Files.readString(manifestPath), LogicalBackupManifest.class);
    manifest.status = "WRITING";
    manifest.closedAt = null;
    manifest.segments.get(0).status = "ACTIVE";
    manifest.segments.get(0).sizeBytes -= LogicalBackupFormat.SEGMENT_FOOTER_SIZE;
    manifest.segments.get(0).sha256 = null;
    Files.writeString(manifestPath, GSON.toJson(manifest));

    try (final LogicalBackupWriter writer = writer(directory, 4096, true)) {
      Assert.assertTrue(writer.getManifest().recovered);
    }
    Assert.assertTrue(scanOnlySegment(directory).isSealed());
  }

  @Test
  public void testChecksumDamageFails() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "metadata");
    }
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      final long payloadOffset =
          LogicalBackupFormat.SEGMENT_HEADER_SIZE
              + frameSize("metadata".length(), 0)
              + LogicalBackupFormat.RECORD_HEADER_SIZE
              + "metadata".length();
      file.seek(payloadOffset);
      file.writeByte(file.readByte() ^ 1);
    }
    try {
      new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(segment, false);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("CRC"));
    }
  }

  @Test
  public void testFooterSequenceDamageFails() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 4096, false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
    }
    final Path segment = onlySegment(directory);
    try (final RandomAccessFile file = new RandomAccessFile(segment.toFile(), "rw")) {
      final long footerOffset = file.length() - LogicalBackupFormat.SEGMENT_FOOTER_SIZE;
      final byte[] footer = new byte[LogicalBackupFormat.SEGMENT_FOOTER_SIZE];
      file.seek(footerOffset);
      file.readFully(footer);
      ByteBuffer.wrap(footer).order(ByteOrder.BIG_ENDIAN).putLong(Integer.BYTES + Long.BYTES, 99);
      ByteBuffer.wrap(footer)
          .order(ByteOrder.BIG_ENDIAN)
          .putInt(
              LogicalBackupFormat.SEGMENT_FOOTER_SIZE - Integer.BYTES,
              LogicalBackupFormat.crc32c(
                  footer, 0, LogicalBackupFormat.SEGMENT_FOOTER_SIZE - Integer.BYTES));
      file.seek(footerOffset);
      file.write(footer);
    }
    try {
      new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(segment, false);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("metadata"));
    }
  }

  @Test
  public void testSegmentRollover() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 500, false)) {
      writer.writeEvent(
          UUID.randomUUID(), 1, Collections.singletonList(request(new byte[20])), "m");
      writer.writeEvent(
          UUID.randomUUID(), 2, Collections.singletonList(request(new byte[20])), "m");
    }
    final List<Path> segments = segments(directory);
    Assert.assertEquals(2, segments.size());
    Assert.assertTrue(
        new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(segments.get(0), false).isSealed());
    Assert.assertTrue(
        new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(segments.get(1), false).isSealed());
  }

  @Test
  public void testSealedSegmentAdvancesDurableSequenceWithFsyncNone() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final LogicalBackupManifest manifest = manifest();
    try (final LogicalBackupWriter writer =
        new LogicalBackupWriter(
            directory,
            manifest,
            500,
            MAX_RECORD_BYTES,
            LogicalBackupWriter.FsyncPolicy.NONE,
            1,
            1,
            false)) {
      writer.writeEvent(
          UUID.randomUUID(), 1, Collections.singletonList(request(new byte[20])), "m");
      writer.writeEvent(
          UUID.randomUUID(), 2, Collections.singletonList(request(new byte[20])), "m");
      Assert.assertEquals(2, writer.getManifest().lastDurableSequence);
    }
  }

  @Test
  public void testControlRecordRollover() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter writer = writer(directory, 258, false)) {
      writer.writeControl(LogicalBackupRecordType.HEARTBEAT, 1, "first");
      writer.writeControl(LogicalBackupRecordType.HEARTBEAT, 2, "second");
      Assert.assertEquals(2, writer.getManifest().segments.size());
      Assert.assertEquals(1, writer.getManifest().lastSequence);
    }
  }

  @Test
  public void testDirectoryLock() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      try {
        writer(directory, 4096, true);
        Assert.fail();
      } catch (final IOException expected) {
        Assert.assertTrue(expected.getMessage().contains("locked"));
      }
    }
  }

  @Test
  public void testFailIfDirectoryExists() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath();
    try {
      writer(directory, 4096, false);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains(directory.toString()));
    }
  }

  @Test
  public void testAppendRejectsConfigurationMismatch() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      // Create the backup.
    }
    try {
      writer(directory, 8192, true);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("segmentSizeBytes"));
    }
  }

  @Test
  public void testAppendRejectsParentSegmentPath() throws Exception {
    final Path root = temporaryFolder.newFolder().toPath();
    final Path directory = root.resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      // Create the backup.
    }
    final Path manifestPath = directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    final LogicalBackupManifest manifest =
        GSON.fromJson(Files.readString(manifestPath), LogicalBackupManifest.class);
    final String originalSegment = manifest.segments.get(0).file;
    Files.copy(directory.resolve(originalSegment), root.resolve("outside.pwal"));
    manifest.segments.get(0).file = "../outside.pwal";
    Files.writeString(manifestPath, GSON.toJson(manifest));

    try {
      writer(directory, 4096, true);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("segment path"));
    }

    manifest.segments.get(0).file = originalSegment;
    Files.writeString(manifestPath, GSON.toJson(manifest));
    try (final LogicalBackupWriter ignored = writer(directory, 4096, true)) {
      // A failed append must release the directory lock.
    }
  }

  @Test
  public void testAppendAdoptsOrphanHeaderOnlySegmentAndClearsClosedState() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      // Create and seal the initial segment.
    }

    final Path orphan = directory.resolve(segmentFileName(1));
    Files.write(orphan, segmentHeader(1, 123));
    try (final LogicalBackupWriter writer = writer(directory, 4096, true)) {
      Assert.assertEquals("WRITING", writer.getManifest().status);
      Assert.assertNull(writer.getManifest().closedAt);
      Assert.assertEquals(2, writer.getManifest().segments.size());
      Assert.assertEquals(
          1, writer.writeEvent(UUID.randomUUID(), 2, Collections.singletonList(request(2)), ""));
    }

    final LogicalBackupSegmentReader.ScanResult scan =
        new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(orphan, false);
    Assert.assertTrue(scan.isSealed());
    Assert.assertEquals(123, scan.getCreatedAt());
  }

  @Test
  public void testAppendRejectsNonEmptyOrphanSegment() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      // Create and seal the initial segment.
    }

    final Path orphan = directory.resolve(segmentFileName(1));
    final byte[] invalidOrphan =
        Arrays.copyOf(segmentHeader(1, 123), LogicalBackupFormat.SEGMENT_HEADER_SIZE + 1);
    Files.write(orphan, invalidOrphan);
    try {
      writer(directory, 4096, true);
      Assert.fail();
    } catch (final IOException expected) {
      // The extra byte is an incomplete record tail and must never be adopted.
    }
  }

  @Test
  public void testAppendRejectsUnexpectedUnlistedSegment() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    try (final LogicalBackupWriter ignored = writer(directory, 4096, false)) {
      // Create and seal the initial segment.
    }

    final Path unexpected = directory.resolve(segmentFileName(99));
    Files.write(unexpected, segmentHeader(99, 123));
    try {
      writer(directory, 4096, true);
      Assert.fail();
    } catch (final IOException expected) {
      Assert.assertTrue(expected.getMessage().contains("Unlisted"));
    }
  }

  @Test
  public void testPeriodicFsyncAdvancesOnHeartbeat() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = "stream";
    manifest.streamType = "data";
    try (final LogicalBackupWriter writer =
        new LogicalBackupWriter(
            directory,
            manifest,
            4096,
            MAX_RECORD_BYTES,
            LogicalBackupWriter.FsyncPolicy.PERIODIC,
            1000,
            100,
            false)) {
      writer.writeEvent(UUID.randomUUID(), 1, Collections.singletonList(request(1)), "");
      Assert.assertEquals(-1, writer.getManifest().lastDurableSequence);
      Thread.sleep(150);
      writer.heartbeat();
      Assert.assertEquals(
          writer.getManifest().lastSequence, writer.getManifest().lastDurableSequence);
    }
  }

  @Test
  public void testMaxRecordBytesHasHardLimit() throws Exception {
    final Path directory = temporaryFolder.newFolder().toPath().resolve("backup");
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    try {
      new LogicalBackupWriter(
          directory,
          manifest,
          4096,
          LogicalBackupFormat.MAX_RECORD_BYTES + 1,
          LogicalBackupWriter.FsyncPolicy.ALWAYS,
          1,
          1,
          false);
      Assert.fail();
    } catch (final IOException expected) {
      // Expected.
    }
  }

  private static LogicalBackupWriter writer(
      final Path directory, final long segmentSize, final boolean append) throws IOException {
    return new LogicalBackupWriter(
        directory,
        manifest(),
        segmentSize,
        MAX_RECORD_BYTES,
        LogicalBackupWriter.FsyncPolicy.ALWAYS,
        1,
        1,
        append);
  }

  private static LogicalBackupManifest manifest() {
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = "backup";
    manifest.pipeName = "pipe";
    manifest.pipeCreationTime = 1;
    manifest.streamId = "stream";
    manifest.streamType = "data";
    return manifest;
  }

  private static TPipeTransferReq request(final int value) {
    return request(new byte[] {(byte) value});
  }

  private static TPipeTransferReq request(final byte[] body) {
    return request((byte) 1, (short) 2, body);
  }

  private static TPipeTransferReq request(final byte version, final short type, final byte[] body) {
    return new TPipeTransferReq().setVersion(version).setType(type).setBody(ByteBuffer.wrap(body));
  }

  private static LogicalBackupSegmentReader.ScanResult scanOnlySegment(final Path directory)
      throws IOException {
    return new LogicalBackupSegmentReader(MAX_RECORD_BYTES).scan(onlySegment(directory), false);
  }

  private static Path onlySegment(final Path directory) throws IOException {
    final List<Path> segments = segments(directory);
    Assert.assertEquals(1, segments.size());
    return segments.get(0);
  }

  private static List<Path> segments(final Path directory) throws IOException {
    try (final java.util.stream.Stream<Path> files = Files.list(directory)) {
      final Path[] segments =
          files
              .filter(path -> path.getFileName().toString().endsWith(".pwal"))
              .sorted()
              .toArray(Path[]::new);
      return Arrays.asList(segments);
    }
  }

  private static long frameSize(final int metadataBytes, final int payloadBytes) {
    return LogicalBackupFormat.RECORD_HEADER_SIZE + metadataBytes + payloadBytes + Integer.BYTES;
  }

  private static String segmentFileName(final long segmentId) {
    return String.format(java.util.Locale.ROOT, "segment-%020d.pwal", segmentId);
  }

  private static byte[] segmentHeader(final long segmentId, final long createdAt) {
    final ByteBuffer header =
        ByteBuffer.allocate(LogicalBackupFormat.SEGMENT_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
    header.putLong(LogicalBackupFormat.SEGMENT_MAGIC);
    header.putShort(LogicalBackupFormat.MAJOR_VERSION);
    header.putShort(LogicalBackupFormat.MINOR_VERSION);
    header.putLong(segmentId);
    header.putLong(createdAt);
    header.putInt(0);
    final byte[] bytes = header.array();
    ByteBuffer.wrap(bytes)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(
            LogicalBackupFormat.SEGMENT_HEADER_SIZE - Integer.BYTES,
            LogicalBackupFormat.crc32c(
                bytes, 0, LogicalBackupFormat.SEGMENT_HEADER_SIZE - Integer.BYTES));
    return bytes;
  }
}
