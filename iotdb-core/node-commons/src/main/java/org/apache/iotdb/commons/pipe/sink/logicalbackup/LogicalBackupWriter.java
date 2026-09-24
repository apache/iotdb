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

import org.apache.iotdb.commons.i18n.LogicalBackupMessages;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class LogicalBackupWriter implements Closeable {

  public enum FsyncPolicy {
    ALWAYS,
    BATCH,
    PERIODIC,
    NONE
  }

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private final Path directory;
  private final Path manifestPath;
  private final Path lockPath;
  private final int maxRecordBytes;
  private final long segmentSizeBytes;
  private final FsyncPolicy fsyncPolicy;
  private final int fsyncBatchOperations;
  private final long fsyncPeriodMs;
  private final LogicalBackupManifest manifest;
  private final LogicalBackupSegmentReader reader;

  private FileChannel channel;
  private FileChannel lockChannel;
  private FileLock directoryLock;
  private long segmentId;
  private long segmentCreatedAt;
  private long segmentFirstSequence = -1;
  private long segmentLastSequence = -1;
  private long segmentRecordCount;
  private MessageDigest segmentDigest;
  private long nextSequence;
  private int operationsSinceFsync;
  private long lastFsyncAt;
  private boolean closed;
  private boolean streamEnded;
  private boolean recoveredDuringOpen;

  public LogicalBackupWriter(
      final Path directory,
      final LogicalBackupManifest manifest,
      final long segmentSizeBytes,
      final int maxRecordBytes,
      final FsyncPolicy fsyncPolicy,
      final int fsyncBatchOperations,
      final long fsyncPeriodMs,
      final boolean append)
      throws IOException {
    this.directory =
        Objects.requireNonNull(directory, LogicalBackupMessages.EXCEPTION_DIRECTORY_5F8F22B8)
            .toAbsolutePath()
            .normalize();
    this.manifest =
        Objects.requireNonNull(manifest, LogicalBackupMessages.EXCEPTION_MANIFEST_7F5CB74A);
    this.segmentSizeBytes = segmentSizeBytes;
    this.maxRecordBytes = maxRecordBytes;
    this.fsyncPolicy =
        Objects.requireNonNull(fsyncPolicy, LogicalBackupMessages.EXCEPTION_FSYNC_POLICY_6D493614);
    if (segmentSizeBytes
            <= LogicalBackupFormat.SEGMENT_HEADER_SIZE + LogicalBackupFormat.SEGMENT_FOOTER_SIZE
        || maxRecordBytes <= 0
        || maxRecordBytes > LogicalBackupFormat.MAX_RECORD_BYTES
        || fsyncBatchOperations <= 0
        || fsyncPeriodMs <= 0) {
      throw new IOException(
          LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_WRITER_CONFIGURATION_707BCC99);
    }
    this.fsyncBatchOperations = fsyncBatchOperations;
    this.fsyncPeriodMs = fsyncPeriodMs;
    this.manifestPath = this.directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    this.lockPath = this.directory.resolve(".backup.lock");
    this.reader = new LogicalBackupSegmentReader(maxRecordBytes);
    try {
      prepareDirectory(append);
      acquireLock();
      if (Files.isSymbolicLink(manifestPath)) {
        throw symbolicLinkException(manifestPath);
      }
      if (Files.exists(manifestPath)) {
        if (!append) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_DIRECTORY_ALREADY_EXISTS_ARG_1C521D1D,
                  directory));
        }
        loadExistingManifest();
      } else {
        manifest.status = "WRITING";
        manifest.createdAt = Instant.now().toString();
        manifest.fsyncPolicy = fsyncPolicy.name().toLowerCase(Locale.ROOT);
        manifest.segmentSizeBytes = segmentSizeBytes;
        manifest.maxRecordBytes = maxRecordBytes;
        writeManifest();
      }
      manifest.status = manifest.recovered ? "RECOVERED" : "WRITING";
      manifest.closedAt = null;
      nextSequence = Math.max(0, manifest.lastSequence + 1);
      openOrCreateSegment();
      rebuildManifestFromSegments();
      if (recoveredDuringOpen) {
        manifest.lastDurableSequence = manifest.lastSequence;
      } else {
        manifest.lastDurableSequence =
            Math.max(-1, Math.min(manifest.lastDurableSequence, manifest.lastSequence));
      }
      updateActiveSegmentManifest();
      manifest.status = manifest.recovered ? "RECOVERED" : "WRITING";
      manifest.closedAt = null;
      writeManifest();
      lastFsyncAt = System.currentTimeMillis();
    } catch (final IOException | RuntimeException e) {
      if (channel != null) {
        try {
          channel.close();
        } catch (final IOException closeException) {
          e.addSuppressed(closeException);
        }
        channel = null;
      }
      releaseLock();
      throw e;
    }
  }

  private void prepareDirectory(final boolean append) throws IOException {
    rejectSymbolicLinksInDirectoryPath();
    if (append) {
      Files.createDirectories(directory);
      rejectSymbolicLinksInDirectoryPath();
      return;
    }
    final Path parent = directory.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
      rejectSymbolicLinksInDirectoryPath();
    }
    try {
      Files.createDirectory(directory);
    } catch (final FileAlreadyExistsException e) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_DIRECTORY_ALREADY_EXISTS_ARG_1C521D1D,
              directory),
          e);
    }
    rejectSymbolicLinksInDirectoryPath();
  }

  private void rejectSymbolicLinksInDirectoryPath() throws IOException {
    Path current = directory.getRoot();
    for (final Path part : directory) {
      current = current == null ? part : current.resolve(part);
      if (Files.isSymbolicLink(current)) {
        throw symbolicLinkException(current);
      }
    }
  }

  private static IOException symbolicLinkException(final Path path) {
    return new IOException(
        String.format(
            LogicalBackupMessages
                .EXCEPTION_SYMBOLIC_LINKS_ARE_NOT_ALLOWED_IN_LOGICAL_BACKUP_DIRECTORY_PATHS_ARG_7E428569,
            path));
  }

  public synchronized long writeEvent(
      final UUID eventGroupId,
      final long eventTime,
      final List<TPipeTransferReq> requests,
      final String metadata)
      throws IOException {
    ensureOpen();
    if (requests == null || requests.isEmpty()) {
      throw new IOException(
          LogicalBackupMessages
              .EXCEPTION_LOGICAL_BACKUP_EVENT_MUST_CONTAIN_AT_LEAST_ONE_REQUEST_0C278BAC);
    }
    Objects.requireNonNull(eventGroupId, LogicalBackupMessages.EXCEPTION_EVENT_GROUP_ID_C6F6268A);
    final String safeMetadata = metadata == null ? "" : metadata;
    final String eventDigest = computeEventDigest(requests);
    if (eventGroupId.toString().equals(manifest.lastEventGroupId)) {
      if (eventDigest.equals(manifest.lastEventDigest)) {
        updateActiveSegmentManifest();
        writeManifest();
        return manifest.lastEventFirstSequence;
      }
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_EVENT_ID_ARG_WAS_ALREADY_WRITTEN_WITH_A_DIFFERENT_DIGEST_6A297330,
              eventGroupId));
    }
    rollBeforeEventIfNecessary(requests, safeMetadata);
    final long eventStartPosition = channel.position();
    final long firstSequence = nextSequence;
    try {
      appendRecord(
          LogicalBackupRecordType.EVENT_BEGIN, eventGroupId, -1, eventTime, null, safeMetadata);
      int operationIndex = 0;
      for (final TPipeTransferReq request : requests) {
        if (request == null || request.getBody() == null) {
          throw new IOException(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_REQUEST_BODY_MUST_NOT_BE_NULL_EFFD92D9);
        }
        appendRecord(
            LogicalBackupRecordType.PIPE_REQUEST,
            eventGroupId,
            operationIndex++,
            eventTime,
            request,
            safeMetadata);
      }
      appendRecord(
          LogicalBackupRecordType.EVENT_COMMIT,
          eventGroupId,
          operationIndex,
          eventTime,
          null,
          safeMetadata);
    } catch (final IOException | RuntimeException e) {
      try {
        rollbackTo(eventStartPosition);
      } catch (final IOException rollbackException) {
        e.addSuppressed(rollbackException);
      }
      throw e;
    }
    if (forceIfNeeded()) {
      manifest.lastDurableSequence = manifest.lastSequence;
    }
    updateActiveSegmentManifest();
    manifest.lastEventGroupId = eventGroupId.toString();
    manifest.lastEventDigest = eventDigest;
    manifest.lastEventFirstSequence = firstSequence;
    writeManifest();
    return firstSequence;
  }

  public synchronized void writeControl(
      final LogicalBackupRecordType recordType, final long eventTime, final String metadata)
      throws IOException {
    ensureOpen();
    final String safeMetadata = metadata == null ? "" : metadata;
    rollBeforeControlIfNecessary(safeMetadata);
    final long recordStartPosition = channel.position();
    try {
      appendRecord(recordType, new UUID(0, 0), -1, eventTime, null, safeMetadata);
    } catch (final IOException | RuntimeException e) {
      try {
        rollbackTo(recordStartPosition);
      } catch (final IOException rollbackException) {
        e.addSuppressed(rollbackException);
      }
      throw e;
    }
    if (forceIfNeeded()) {
      manifest.lastDurableSequence = manifest.lastSequence;
    }
    updateActiveSegmentManifest();
    writeManifest();
  }

  public synchronized void recordSkippedEvent(final long eventTime, final String metadata)
      throws IOException {
    writeControl(LogicalBackupRecordType.SKIPPED_EVENT, eventTime, metadata);
    manifest.skippedEventCount++;
    writeManifest();
  }

  public synchronized LogicalBackupManifest getManifest() {
    return manifest;
  }

  public synchronized void heartbeat() throws IOException {
    ensureOpen();
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
        || directoryLock == null
        || !directoryLock.isValid()) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_UNAVAILABLE_ARG_85F090AD,
              directory));
    }
    if (forceIfNeeded()) {
      manifest.lastDurableSequence = manifest.lastSequence;
      updateActiveSegmentManifest();
      writeManifest();
    }
  }

  private void appendRecord(
      final LogicalBackupRecordType recordType,
      final UUID eventGroupId,
      final int operationIndex,
      final long eventTime,
      final TPipeTransferReq request,
      final String metadata)
      throws IOException {
    final byte[] metadataBytes = metadata.getBytes(StandardCharsets.UTF_8);
    final byte[] payload =
        request == null
            ? new byte[0]
            : java.util.Arrays.copyOf(request.getBody(), request.getBody().length);
    if ((long) metadataBytes.length + payload.length > maxRecordBytes) {
      throw new IOException(
          LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_RECORD_EXCEEDS_MAX_RECORD_BYTES_B9A9C996);
    }
    final long requiredBytes =
        LogicalBackupFormat.RECORD_HEADER_SIZE
            + metadataBytes.length
            + payload.length
            + Integer.BYTES;
    final long sequence = nextSequence++;
    final ByteBuffer header =
        ByteBuffer.allocate(LogicalBackupFormat.RECORD_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
    header.putInt(LogicalBackupFormat.RECORD_MAGIC);
    header.put(recordType.getCode());
    header.putLong(sequence);
    header.putLong(eventGroupId.getMostSignificantBits());
    header.putLong(eventGroupId.getLeastSignificantBits());
    header.putInt(operationIndex);
    header.putLong(eventTime);
    header.put(request == null ? (byte) 0 : request.getVersion());
    header.putShort(request == null ? (short) 0 : request.getType());
    header.putInt(metadataBytes.length);
    header.putInt(payload.length);
    header.putInt(0);
    final byte[] headerBytes = header.array();
    final int headerCrc =
        LogicalBackupFormat.crc32c(headerBytes, 0, LogicalBackupFormat.RECORD_HEADER_SIZE - 4);
    ByteBuffer.wrap(headerBytes)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(LogicalBackupFormat.RECORD_HEADER_SIZE - 4, headerCrc);
    final byte[] frameContent = new byte[metadataBytes.length + payload.length];
    System.arraycopy(metadataBytes, 0, frameContent, 0, metadataBytes.length);
    System.arraycopy(payload, 0, frameContent, metadataBytes.length, payload.length);
    final int frameCrc = LogicalBackupFormat.crc32c(frameContent, 0, frameContent.length);

    writeFully(ByteBuffer.wrap(headerBytes));
    writeFully(ByteBuffer.wrap(frameContent));
    writeFully(
        ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(frameCrc).flip());
    segmentDigest.update(headerBytes);
    segmentDigest.update(frameContent);
    segmentDigest.update(
        ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(frameCrc).array());
    if (segmentFirstSequence < 0) {
      segmentFirstSequence = sequence;
    }
    segmentLastSequence = sequence;
    segmentRecordCount++;
    manifest.firstSequence = manifest.firstSequence < 0 ? sequence : manifest.firstSequence;
    manifest.lastSequence = sequence;
    final String countKey = recordType.name();
    manifest.operationCounts.put(countKey, manifest.operationCounts.getOrDefault(countKey, 0L) + 1);
    operationsSinceFsync++;
    streamEnded = recordType == LogicalBackupRecordType.STREAM_END;
  }

  private void openOrCreateSegment() throws IOException {
    if (manifest.segments.isEmpty()) {
      segmentId = 0;
      validateSegmentFiles(segmentFileName(segmentId));
      openNewSegment();
      return;
    }
    final LogicalBackupManifest.Segment last = manifest.segments.get(manifest.segments.size() - 1);
    final Path segmentPath = resolveExistingSegmentPath(last.file);
    LogicalBackupSegmentReader.ScanResult scan = scanLastSegmentForAppend(segmentPath, last);
    if (scan.isSealed()) {
      if (!"SEALED".equals(last.status)) {
        manifest.recovered = true;
        recoveredDuringOpen = true;
      }
      segmentId = scan.getSegmentId() + 1;
      // Validate every existing sealed segment before creating a new file in the backup.
      rebuildManifestFromSegments();
      validateSegmentFiles(segmentFileName(segmentId));
      openNewSegment();
      return;
    }
    validateSegmentFiles(null);
    if (scan.hasIncompleteTail() || scan.hasOpenEventGroup()) {
      try (final FileChannel truncateChannel =
          FileChannel.open(segmentPath, StandardOpenOption.WRITE)) {
        truncateChannel.truncate(scan.getLastCommittedLength());
        truncateChannel.force(true);
      }
      manifest.recovered = true;
      manifest.status = "RECOVERED";
      recoveredDuringOpen = true;
      scan = reader.scan(segmentPath, false);
    }
    if (!scan.getRecords().isEmpty()) {
      nextSequence = scan.getRecords().get(scan.getRecords().size() - 1).getSequence() + 1;
      manifest.lastSequence = scan.getRecords().get(scan.getRecords().size() - 1).getSequence();
    }
    segmentId = scan.getSegmentId();
    openExistingSegment(segmentPath, scan);
  }

  private LogicalBackupSegmentReader.ScanResult scanLastSegmentForAppend(
      final Path segmentPath, final LogicalBackupManifest.Segment segment) throws IOException {
    try {
      return reader.scan(segmentPath, true);
    } catch (final IOException scanException) {
      final long recoverableLength =
          "SEALED".equals(segment.status)
              ? -1
              : findRecoverableTailStart(segmentPath, segment.sizeBytes);
      if (recoverableLength < LogicalBackupFormat.SEGMENT_HEADER_SIZE) {
        throw scanException;
      }
      try (final FileChannel truncateChannel =
          FileChannel.open(segmentPath, StandardOpenOption.WRITE)) {
        truncateChannel.truncate(recoverableLength);
        truncateChannel.force(true);
      }
      manifest.recovered = true;
      manifest.status = "RECOVERED";
      recoveredDuringOpen = true;
      return reader.scan(segmentPath, false);
    }
  }

  private static long findRecoverableTailStart(
      final Path segmentPath, final long manifestSegmentSize) throws IOException {
    final long fileSize = Files.size(segmentPath);
    if (fileSize
        >= LogicalBackupFormat.SEGMENT_HEADER_SIZE + LogicalBackupFormat.SEGMENT_FOOTER_SIZE) {
      final ByteBuffer magic = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
      try (final FileChannel readChannel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
        readChannel.position(fileSize - LogicalBackupFormat.SEGMENT_FOOTER_SIZE);
        while (magic.hasRemaining()) {
          if (readChannel.read(magic) < 0) {
            break;
          }
        }
      }
      if (!magic.hasRemaining() && magic.flip().getInt() == LogicalBackupFormat.FOOTER_MAGIC) {
        return fileSize - LogicalBackupFormat.SEGMENT_FOOTER_SIZE;
      }
    }
    if (manifestSegmentSize >= LogicalBackupFormat.SEGMENT_HEADER_SIZE
        && manifestSegmentSize < fileSize
        && fileSize - manifestSegmentSize <= LogicalBackupFormat.SEGMENT_FOOTER_SIZE) {
      return manifestSegmentSize;
    }
    return -1;
  }

  private void openExistingSegment(
      final Path segmentPath, final LogicalBackupSegmentReader.ScanResult scan) throws IOException {
    channel = FileChannel.open(segmentPath, StandardOpenOption.WRITE, StandardOpenOption.READ);
    channel.position(channel.size());
    segmentCreatedAt = scan.getCreatedAt();
    segmentDigest = LogicalBackupFormat.newSha256();
    try (final FileChannel digestChannel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
      final ByteBuffer digestBuffer = ByteBuffer.allocate(64 * 1024);
      long remaining = scan.getValidLength();
      while (remaining > 0) {
        digestBuffer.clear();
        digestBuffer.limit((int) Math.min(digestBuffer.capacity(), remaining));
        final int read = digestChannel.read(digestBuffer);
        if (read < 0) {
          break;
        }
        remaining -= read;
        segmentDigest.update(digestBuffer.array(), 0, read);
      }
    }
    segmentFirstSequence = -1;
    segmentLastSequence = -1;
    segmentRecordCount = 0;
    for (final LogicalBackupRecord record : scan.getRecords()) {
      segmentFirstSequence = segmentFirstSequence < 0 ? record.getSequence() : segmentFirstSequence;
      segmentLastSequence = record.getSequence();
      segmentRecordCount++;
    }
    streamEnded =
        !scan.getRecords().isEmpty()
            && scan.getRecords().get(scan.getRecords().size() - 1).getRecordType()
                == LogicalBackupRecordType.STREAM_END;
  }

  private void openNewSegment() throws IOException {
    final String fileName = segmentFileName(segmentId);
    final Path segmentPath = resolveSegmentPath(fileName);
    final Path temporarySegmentPath = resolveSegmentPath(fileName + ".tmp");
    if (Files.exists(segmentPath, LinkOption.NOFOLLOW_LINKS)) {
      adoptOrphanSegment(resolveExistingSegmentPath(fileName), fileName);
      return;
    }

    // A temporary segment is never exposed through the manifest and contains only a header, so it
    // is safe to discard after an interrupted creation attempt.
    Files.deleteIfExists(temporarySegmentPath);
    segmentCreatedAt = System.currentTimeMillis();
    final ByteBuffer header =
        ByteBuffer.allocate(LogicalBackupFormat.SEGMENT_HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
    header.putLong(LogicalBackupFormat.SEGMENT_MAGIC);
    header.putShort(LogicalBackupFormat.MAJOR_VERSION);
    header.putShort(LogicalBackupFormat.MINOR_VERSION);
    header.putLong(segmentId);
    header.putLong(segmentCreatedAt);
    header.putInt(0);
    final byte[] bytes = header.array();
    ByteBuffer.wrap(bytes)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(
            LogicalBackupFormat.SEGMENT_HEADER_SIZE - 4,
            LogicalBackupFormat.crc32c(bytes, 0, LogicalBackupFormat.SEGMENT_HEADER_SIZE - 4));
    try (final FileChannel temporaryChannel =
        FileChannel.open(
            temporarySegmentPath,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ)) {
      final ByteBuffer headerBuffer = ByteBuffer.wrap(bytes);
      while (headerBuffer.hasRemaining()) {
        temporaryChannel.write(headerBuffer);
      }
      temporaryChannel.force(true);
    }
    try {
      Files.move(temporarySegmentPath, segmentPath, StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(temporarySegmentPath, segmentPath);
    }

    final LogicalBackupSegmentReader.ScanResult scan = reader.scan(segmentPath, false);
    openExistingSegment(segmentPath, scan);
    addActiveSegmentToManifest(fileName);
  }

  private void adoptOrphanSegment(final Path segmentPath, final String fileName)
      throws IOException {
    final LogicalBackupSegmentReader.ScanResult scan = reader.scan(segmentPath, false);
    if (scan.getSegmentId() != segmentId
        || scan.isSealed()
        || !scan.getRecords().isEmpty()
        || scan.getValidLength() != LogicalBackupFormat.SEGMENT_HEADER_SIZE) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_UNLISTED_LOGICAL_BACKUP_SEGMENT_ARG_FF548C0B,
              segmentPath));
    }
    openExistingSegment(segmentPath, scan);
    addActiveSegmentToManifest(fileName);
  }

  private void addActiveSegmentToManifest(final String fileName) throws IOException {
    final LogicalBackupManifest.Segment segment = new LogicalBackupManifest.Segment();
    segment.file = fileName;
    segment.segmentId = segmentId;
    segment.status = "ACTIVE";
    manifest.segments.add(segment);
    writeManifest();
  }

  private void validateSegmentFiles(final String allowedOrphanFile) throws IOException {
    final Set<String> listedFiles = new HashSet<>();
    for (final LogicalBackupManifest.Segment segment : manifest.segments) {
      listedFiles.add(segment.file);
    }
    try (final DirectoryStream<Path> segmentFiles = Files.newDirectoryStream(directory, "*.pwal")) {
      for (final Path segmentFile : segmentFiles) {
        final String fileName = segmentFile.getFileName().toString();
        if (!listedFiles.contains(fileName) && !fileName.equals(allowedOrphanFile)) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages.EXCEPTION_UNLISTED_LOGICAL_BACKUP_SEGMENT_ARG_FF548C0B,
                  segmentFile));
        }
      }
    }
  }

  private static String segmentFileName(final long id) {
    return String.format(Locale.ROOT, "segment-%020d.pwal", id);
  }

  private void sealSegment() throws IOException {
    if (channel == null || segmentRecordCount == 0) {
      return;
    }
    final byte[] digest = segmentDigest.digest();
    final ByteBuffer footer =
        ByteBuffer.allocate(LogicalBackupFormat.SEGMENT_FOOTER_SIZE).order(ByteOrder.BIG_ENDIAN);
    footer.putInt(LogicalBackupFormat.FOOTER_MAGIC);
    footer.putLong(segmentId);
    footer.putLong(segmentFirstSequence);
    footer.putLong(segmentLastSequence);
    footer.putLong(segmentRecordCount);
    footer.putLong(channel.position());
    footer.putLong(segmentCreatedAt);
    footer.putLong(System.currentTimeMillis());
    footer.put(digest);
    footer.putInt(0);
    final byte[] footerBytes = footer.array();
    ByteBuffer.wrap(footerBytes)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(
            LogicalBackupFormat.SEGMENT_FOOTER_SIZE - 4,
            LogicalBackupFormat.crc32c(
                footerBytes, 0, LogicalBackupFormat.SEGMENT_FOOTER_SIZE - 4));
    writeFully(ByteBuffer.wrap(footerBytes));
    channel.force(true);
    final LogicalBackupManifest.Segment segment =
        manifest.segments.get(manifest.segments.size() - 1);
    segment.firstSequence = segmentFirstSequence;
    segment.lastSequence = segmentLastSequence;
    segment.recordCount = segmentRecordCount;
    segment.sizeBytes = channel.position();
    segment.sha256 = LogicalBackupFormat.toHex(digest);
    segment.status = "SEALED";
    manifest.lastDurableSequence = segmentLastSequence;
    channel.close();
    channel = null;
    writeManifest();
  }

  private boolean forceIfNeeded() throws IOException {
    if (channel == null) {
      return false;
    }
    final long now = System.currentTimeMillis();
    final boolean shouldForce =
        fsyncPolicy == FsyncPolicy.ALWAYS
            || (fsyncPolicy == FsyncPolicy.BATCH && operationsSinceFsync >= fsyncBatchOperations)
            || (fsyncPolicy == FsyncPolicy.PERIODIC && now - lastFsyncAt >= fsyncPeriodMs);
    if (shouldForce) {
      channel.force(true);
      operationsSinceFsync = 0;
      lastFsyncAt = now;
    }
    return shouldForce;
  }

  private void writeManifest() throws IOException {
    final Path temporary =
        manifestPath.resolveSibling(LogicalBackupFormat.MANIFEST_FILE_NAME + ".tmp");
    final byte[] manifestBytes = GSON.toJson(manifest).getBytes(StandardCharsets.UTF_8);
    Files.deleteIfExists(temporary);
    try (final FileChannel manifestChannel =
        FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      final ByteBuffer manifestBuffer = ByteBuffer.wrap(manifestBytes);
      while (manifestBuffer.hasRemaining()) {
        manifestChannel.write(manifestBuffer);
      }
      manifestChannel.force(true);
    }
    try {
      Files.move(
          temporary,
          manifestPath,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(temporary, manifestPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private void loadExistingManifest() throws IOException {
    final LogicalBackupManifest existing;
    try {
      existing =
          GSON.fromJson(
              Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    } catch (final JsonParseException | NullPointerException e) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7,
              manifestPath),
          e);
    }
    if (existing == null) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7,
              manifestPath));
    }
    if (!LogicalBackupFormat.FORMAT_NAME.equals(existing.formatName)
        || !LogicalBackupFormat.FORMAT_VERSION.equals(existing.formatVersion)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_MANIFEST_FORMAT_ARG_5005F99E,
              manifestPath));
    }
    validateManifestIdentity("backupId", manifest.backupId, existing.backupId);
    validateManifestIdentity("streamId", manifest.streamId, existing.streamId);
    validateManifestIdentity("pipeName", manifest.pipeName, existing.pipeName);
    validateManifestIdentity("streamType", manifest.streamType, existing.streamType);
    validateManifestIdentity("sourceClusterId", manifest.sourceClusterId, existing.sourceClusterId);
    validateManifestIdentity("sourceVersion", manifest.sourceVersion, existing.sourceVersion);
    validateManifestIdentity(
        "timestampPrecision", manifest.timestampPrecision, existing.timestampPrecision);
    validateManifestIdentity("sinkTaskId", manifest.sinkTaskId, existing.sinkTaskId);
    validateManifestIdentity(
        "fsyncPolicy", fsyncPolicy.name().toLowerCase(Locale.ROOT), existing.fsyncPolicy);
    validateManifestConfiguration("segmentSizeBytes", segmentSizeBytes, existing.segmentSizeBytes);
    validateManifestConfiguration("maxRecordBytes", maxRecordBytes, existing.maxRecordBytes);
    if (manifest.pipeCreationTime != 0 && manifest.pipeCreationTime != existing.pipeCreationTime) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1,
              "pipeCreationTime",
              manifest.pipeCreationTime,
              existing.pipeCreationTime));
    }
    if (manifest.regionId != existing.regionId) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1,
              "regionId",
              manifest.regionId,
              existing.regionId));
    }
    manifest.status = existing.status;
    manifest.backupId = existing.backupId;
    manifest.pipeName = existing.pipeName;
    manifest.pipeCreationTime = existing.pipeCreationTime;
    manifest.sourceClusterId = existing.sourceClusterId;
    manifest.sourceVersion = existing.sourceVersion;
    manifest.timestampPrecision = existing.timestampPrecision;
    manifest.streamId = existing.streamId;
    manifest.regionId = existing.regionId;
    manifest.sinkTaskId = existing.sinkTaskId;
    manifest.streamType = existing.streamType;
    manifest.firstSequence = existing.firstSequence;
    manifest.lastSequence = existing.lastSequence;
    manifest.lastDurableSequence = existing.lastDurableSequence;
    manifest.recovered = existing.recovered;
    manifest.segments = existing.segments == null ? new ArrayList<>() : existing.segments;
    manifest.operationCounts =
        existing.operationCounts == null
            ? new java.util.LinkedHashMap<>()
            : existing.operationCounts;
    manifest.createdAt = existing.createdAt;
    manifest.closedAt = existing.closedAt;
    manifest.fsyncPolicy = existing.fsyncPolicy;
    manifest.segmentSizeBytes = existing.segmentSizeBytes;
    manifest.maxRecordBytes = existing.maxRecordBytes;
    manifest.lastEventGroupId = existing.lastEventGroupId;
    manifest.lastEventDigest = existing.lastEventDigest;
    manifest.lastEventFirstSequence = existing.lastEventFirstSequence;
    manifest.skippedEventCount = existing.skippedEventCount;
  }

  private void validateManifestIdentity(
      final String field, final String expected, final String actual) throws IOException {
    if (expected != null && !Objects.equals(expected, actual)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1,
              field,
              expected,
              actual));
    }
  }

  private void validateManifestConfiguration(
      final String field, final long expected, final long actual) throws IOException {
    if (expected != actual) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1,
              field,
              expected,
              actual));
    }
  }

  private void acquireLock() throws IOException {
    try {
      if (Files.isSymbolicLink(lockPath)) {
        throw symbolicLinkException(lockPath);
      }
      lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      directoryLock = lockChannel.tryLock();
      if (directoryLock == null) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_LOCKED_ARG_A4366800,
                directory));
      }
      lockChannel.truncate(0);
      lockChannel.write(
          StandardCharsets.UTF_8.encode(Long.toString(ProcessHandle.current().pid())));
      lockChannel.force(true);
    } catch (final OverlappingFileLockException e) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_LOCKED_ARG_A4366800,
              directory),
          e);
    }
  }

  private void releaseLock() {
    try {
      if (directoryLock != null) {
        directoryLock.release();
        directoryLock = null;
      }
    } catch (final IOException ignored) {
      // Best effort during failure/close.
    }
    try {
      if (lockChannel != null) {
        lockChannel.close();
        lockChannel = null;
      }
    } catch (final IOException ignored) {
      // Best effort during failure/close.
    }
  }

  private void writeFully(final ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private void ensureOpen() throws IOException {
    if (closed || channel == null) {
      throw new IOException(
          LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_WRITER_IS_CLOSED_EE463BBB);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      if (channel != null) {
        if (!streamEnded) {
          writeControl(LogicalBackupRecordType.STREAM_END, System.currentTimeMillis(), "");
        }
        sealSegment();
      }
      manifest.status = manifest.recovered ? "RECOVERED" : "SEALED";
      manifest.closedAt = Instant.now().toString();
      manifest.lastDurableSequence = manifest.lastSequence;
      writeManifest();
    } finally {
      if (channel != null) {
        channel.close();
        channel = null;
      }
      closed = true;
      releaseLock();
    }
  }

  private void rollBeforeEventIfNecessary(
      final List<TPipeTransferReq> requests, final String metadata) throws IOException {
    final long metadataBytes = metadata.getBytes(StandardCharsets.UTF_8).length;
    long requiredBytes = frameSize(metadataBytes, 0) * 2;
    for (final TPipeTransferReq request : requests) {
      if (request == null || request.getBody() == null) {
        throw new IOException(
            LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_REQUEST_BODY_MUST_NOT_BE_NULL_EFFD92D9);
      }
      requiredBytes += frameSize(metadataBytes, request.getBody().length);
    }
    if (channel.position() > LogicalBackupFormat.SEGMENT_HEADER_SIZE
        && channel.position() + requiredBytes + LogicalBackupFormat.SEGMENT_FOOTER_SIZE
            > segmentSizeBytes) {
      sealSegment();
      segmentId++;
      openNewSegment();
    }
  }

  private void rollBeforeControlIfNecessary(final String metadata) throws IOException {
    final long requiredBytes = frameSize(metadata.getBytes(StandardCharsets.UTF_8).length, 0);
    if (channel.position() > LogicalBackupFormat.SEGMENT_HEADER_SIZE
        && channel.position() + requiredBytes + LogicalBackupFormat.SEGMENT_FOOTER_SIZE
            > segmentSizeBytes) {
      sealSegment();
      segmentId++;
      openNewSegment();
    }
  }

  private static long frameSize(final long metadataBytes, final long payloadBytes) {
    return LogicalBackupFormat.RECORD_HEADER_SIZE + metadataBytes + payloadBytes + Integer.BYTES;
  }

  private static String computeEventDigest(final List<TPipeTransferReq> requests)
      throws IOException {
    final MessageDigest digest = LogicalBackupFormat.newSha256();
    for (final TPipeTransferReq request : requests) {
      if (request == null || request.getBody() == null) {
        throw new IOException(
            LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_REQUEST_BODY_MUST_NOT_BE_NULL_EFFD92D9);
      }
      updateEventDigest(digest, request.getVersion(), request.getType(), request.getBody());
    }
    return LogicalBackupFormat.toHex(digest.digest());
  }

  private static void updateEventDigest(
      final MessageDigest digest,
      final byte requestVersion,
      final short requestType,
      final byte[] body) {
    digest.update(requestVersion);
    digest.update(
        ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(requestType).array());
    digest.update(body);
  }

  private void rollbackTo(final long position) throws IOException {
    final Path segmentPath = currentSegmentPath();
    channel.truncate(position);
    channel.force(true);
    channel.close();
    channel = null;
    final LogicalBackupSegmentReader.ScanResult scan = reader.scan(segmentPath, false);
    openExistingSegment(segmentPath, scan);
    rebuildManifestFromSegments();
    manifest.recovered = true;
    manifest.status = "RECOVERED";
    manifest.lastDurableSequence = manifest.lastSequence;
    operationsSinceFsync = 0;
    lastFsyncAt = System.currentTimeMillis();
    updateActiveSegmentManifest();
    writeManifest();
  }

  private void rebuildManifestFromSegments() throws IOException {
    manifest.firstSequence = -1;
    manifest.lastSequence = -1;
    manifest.lastEventGroupId = null;
    manifest.lastEventDigest = null;
    manifest.lastEventFirstSequence = -1;
    manifest.skippedEventCount = 0;
    manifest.operationCounts = new LinkedHashMap<>();

    long expectedSequence = -1;
    long expectedSegmentId = -1;
    for (int segmentIndex = 0; segmentIndex < manifest.segments.size(); segmentIndex++) {
      final LogicalBackupManifest.Segment segment = manifest.segments.get(segmentIndex);
      final Path segmentPath = resolveExistingSegmentPath(segment.file);
      final LogicalBackupSegmentReader.ScanResult scan = reader.scan(segmentPath, false);
      if (scan.getSegmentId() != segment.segmentId) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_ID_MISMATCH_ARG_9FE7E88A,
                segmentPath));
      }
      if ((expectedSegmentId >= 0 && scan.getSegmentId() != expectedSegmentId)
          || (!scan.isSealed() && segmentIndex != manifest.segments.size() - 1)) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_ID_MISMATCH_ARG_9FE7E88A,
                segmentPath));
      }
      expectedSegmentId = scan.getSegmentId() + 1;
      if (scan.isSealed() && "SEALED".equals(segment.status)) {
        final LogicalBackupSegmentReader.Footer footer = scan.getFooter();
        if (segment.firstSequence != footer.getFirstSequence()
            || segment.lastSequence != footer.getLastSequence()
            || segment.recordCount != footer.getRecordCount()
            || segment.sizeBytes != scan.getValidLength()
            || !LogicalBackupFormat.toHex(footer.getDigest()).equals(segment.sha256)) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_SEGMENT_METADATA_MISMATCH_ARG_376FD0B3,
                  segmentPath));
        }
      }

      segment.firstSequence = -1;
      segment.lastSequence = -1;
      segment.recordCount = scan.getRecords().size();
      segment.sizeBytes = scan.getValidLength();
      segment.status = scan.isSealed() ? "SEALED" : "ACTIVE";
      segment.sha256 =
          scan.isSealed() ? LogicalBackupFormat.toHex(scan.getFooter().getDigest()) : null;

      MessageDigest eventDigest = null;
      UUID eventGroupId = null;
      long eventFirstSequence = -1;
      for (final LogicalBackupRecord record : scan.getRecords()) {
        if (expectedSequence >= 0 && record.getSequence() != expectedSequence) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEQUENCE_GAP_IN_ARG_D8698149,
                  segmentPath));
        }
        expectedSequence = record.getSequence() + 1;
        segment.firstSequence =
            segment.firstSequence < 0 ? record.getSequence() : segment.firstSequence;
        segment.lastSequence = record.getSequence();
        manifest.firstSequence =
            manifest.firstSequence < 0 ? record.getSequence() : manifest.firstSequence;
        manifest.lastSequence = record.getSequence();
        manifest.operationCounts.merge(record.getRecordType().name(), 1L, Long::sum);
        if (record.getRecordType() == LogicalBackupRecordType.SKIPPED_EVENT) {
          manifest.skippedEventCount++;
        } else if (record.getRecordType() == LogicalBackupRecordType.EVENT_BEGIN) {
          eventDigest = LogicalBackupFormat.newSha256();
          eventGroupId = record.getEventGroupId();
          eventFirstSequence = record.getSequence();
        } else if (record.getRecordType() == LogicalBackupRecordType.PIPE_REQUEST) {
          updateEventDigest(
              eventDigest,
              record.getRequestVersion(),
              record.getRequestType(),
              record.getPayload());
        } else if (record.getRecordType() == LogicalBackupRecordType.EVENT_COMMIT) {
          manifest.lastEventGroupId = eventGroupId.toString();
          manifest.lastEventDigest = LogicalBackupFormat.toHex(eventDigest.digest());
          manifest.lastEventFirstSequence = eventFirstSequence;
          eventDigest = null;
          eventGroupId = null;
          eventFirstSequence = -1;
        }
      }
    }
    nextSequence = Math.max(0, manifest.lastSequence + 1);
    streamEnded =
        manifest.lastSequence >= 0
            && !manifest.segments.isEmpty()
            && isLastRecordStreamEnd(reader.scan(currentSegmentPath(), false).getRecords());
  }

  private static boolean isLastRecordStreamEnd(final List<LogicalBackupRecord> records) {
    return !records.isEmpty()
        && records.get(records.size() - 1).getRecordType() == LogicalBackupRecordType.STREAM_END;
  }

  private Path currentSegmentPath() throws IOException {
    if (manifest.segments.isEmpty()) {
      throw new IOException(
          LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_HAS_NO_SEGMENTS_B48D5F15);
    }
    return resolveSegmentPath(manifest.segments.get(manifest.segments.size() - 1).file);
  }

  private Path resolveSegmentPath(final String file) throws IOException {
    final Path relativePath;
    try {
      relativePath = file == null ? null : Path.of(file);
    } catch (final RuntimeException e) {
      throw invalidSegmentPath(file, e);
    }
    final Path normalizedDirectory = directory.toAbsolutePath().normalize();
    if (relativePath == null || relativePath.isAbsolute() || relativePath.getNameCount() != 1) {
      throw invalidSegmentPath(file, null);
    }
    final Path resolved = normalizedDirectory.resolve(relativePath).normalize();
    if (!resolved.startsWith(normalizedDirectory)
        || !normalizedDirectory.equals(resolved.getParent())) {
      throw invalidSegmentPath(file, null);
    }
    return resolved;
  }

  private Path resolveExistingSegmentPath(final String file) throws IOException {
    final Path resolved = resolveSegmentPath(file);
    if (Files.isSymbolicLink(resolved)
        || !Files.isRegularFile(resolved, LinkOption.NOFOLLOW_LINKS)) {
      throw invalidSegmentPath(file, null);
    }
    return resolved;
  }

  private static IOException invalidSegmentPath(final String file, final Throwable cause) {
    final IOException exception =
        new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_PATH_ARG_6485A845,
                file));
    if (cause != null) {
      exception.initCause(cause);
    }
    return exception;
  }

  private void updateActiveSegmentManifest() throws IOException {
    if (channel == null || manifest.segments.isEmpty()) {
      return;
    }
    final LogicalBackupManifest.Segment segment =
        manifest.segments.get(manifest.segments.size() - 1);
    segment.firstSequence = segmentFirstSequence;
    segment.lastSequence = segmentLastSequence;
    segment.recordCount = segmentRecordCount;
    segment.sizeBytes = channel.position();
    segment.status = "ACTIVE";
  }
}
