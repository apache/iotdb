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
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogicalBackupArchiveReader {

  private static final Gson GSON = new Gson();
  private static final Set<Short> REPLAYABLE_REQUEST_TYPES =
      Set.of(
          PipeRequestType.TRANSFER_TABLET_INSERT_NODE_V2.getType(),
          PipeRequestType.TRANSFER_TABLET_RAW_V2.getType(),
          PipeRequestType.TRANSFER_PLAN_NODE.getType(),
          PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_PIECE.getType(),
          PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL.getType());

  public List<BackupStream> read(final Path source, final boolean allowIncomplete)
      throws IOException {
    final Path normalizedSource = source.toAbsolutePath().normalize();
    final List<Path> manifests = discoverManifests(normalizedSource);
    if (manifests.isEmpty()) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_NO_LOGICAL_BACKUP_MANIFEST_FOUND_UNDER_ARG_4ACEA70E,
              source));
    }
    final List<BackupStream> streams = new ArrayList<>();
    final Set<String> streamIds = new HashSet<>();
    LogicalBackupManifest archiveIdentity = null;
    for (final Path manifest : manifests) {
      final BackupStream stream = readStream(manifest, allowIncomplete);
      if (archiveIdentity == null) {
        archiveIdentity = stream.getManifest();
      } else {
        validateArchiveIdentity(archiveIdentity, stream.getManifest());
      }
      if (!streamIds.add(stream.getManifest().streamId)) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_DUPLICATE_LOGICAL_BACKUP_STREAM_ID_ARG_1BF0F6EE,
                stream.getManifest().streamId));
      }
      streams.add(stream);
    }
    streams.sort(
        Comparator.comparingInt(
                (BackupStream stream) ->
                    "schema".equalsIgnoreCase(stream.getManifest().streamType) ? 0 : 1)
            .thenComparing((BackupStream stream) -> stream.getManifest().streamId));
    return Collections.unmodifiableList(streams);
  }

  private static void validateArchiveIdentity(
      final LogicalBackupManifest expected, final LogicalBackupManifest actual) throws IOException {
    validateArchiveIdentityField("backupId", expected.backupId, actual.backupId);
    validateArchiveIdentityField("pipeName", expected.pipeName, actual.pipeName);
    validateArchiveIdentityField(
        "pipeCreationTime", expected.pipeCreationTime, actual.pipeCreationTime);
    validateArchiveIdentityField(
        "sourceClusterId", expected.sourceClusterId, actual.sourceClusterId);
    validateArchiveIdentityField("sourceVersion", expected.sourceVersion, actual.sourceVersion);
    validateArchiveIdentityField(
        "timestampPrecision", expected.timestampPrecision, actual.timestampPrecision);
  }

  private static void validateArchiveIdentityField(
      final String field, final Object expected, final Object actual) throws IOException {
    if (!java.util.Objects.equals(expected, actual)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1,
              field,
              expected,
              actual));
    }
  }

  private BackupStream readStream(final Path manifestPath, final boolean allowIncomplete)
      throws IOException {
    final LogicalBackupManifest manifest;
    try {
      manifest =
          GSON.fromJson(
              Files.readString(manifestPath, StandardCharsets.UTF_8), LogicalBackupManifest.class);
    } catch (final JsonParseException | NullPointerException e) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7,
              manifestPath),
          e);
    }
    validateManifest(manifest, manifestPath);
    if (!allowIncomplete && manifest.skippedEventCount > 0) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_CONTAINS_SKIPPED_EVENTS_ARG_E69FD599,
              manifestPath));
    }

    final Path streamDirectory = manifestPath.getParent().toAbsolutePath().normalize();
    final LogicalBackupSegmentReader segmentReader =
        new LogicalBackupSegmentReader(manifest.maxRecordBytes);
    final List<LogicalBackupRecord> records = new ArrayList<>();
    final Map<String, Long> operationCounts = new LinkedHashMap<>();
    final Set<Path> listedSegments = new HashSet<>();
    long expectedSequence = -1;
    long expectedSegmentId = 0;

    for (int index = 0; index < manifest.segments.size(); index++) {
      final LogicalBackupManifest.Segment segment = manifest.segments.get(index);
      if (segment == null) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_PATH_ARG_6485A845,
                "null"));
      }
      final Path segmentPath = resolveSegmentPath(streamDirectory, segment.file);
      if (segmentPath == null
          || Files.isSymbolicLink(segmentPath)
          || !Files.isRegularFile(segmentPath, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_PATH_ARG_6485A845,
                segment.file));
      }
      listedSegments.add(segmentPath);
      final boolean lastSegment = index == manifest.segments.size() - 1;
      final LogicalBackupSegmentReader.ScanResult scan =
          segmentReader.scan(segmentPath, allowIncomplete && lastSegment);
      if (scan.getSegmentId() != segment.segmentId || scan.getSegmentId() != expectedSegmentId++) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_ID_MISMATCH_ARG_9FE7E88A,
                segmentPath));
      }
      if (!scan.isSealed() && (!allowIncomplete || !lastSegment)) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_IS_NOT_SEALED_ARG_937793FF,
                segmentPath));
      }
      if (scan.isSealed()) {
        validateSegmentManifest(segment, scan, segmentPath);
      }
      for (final LogicalBackupRecord record : scan.getRecords()) {
        validateReplayableRequest(record);
      }

      final List<LogicalBackupRecord> committedRecords =
          scan.hasOpenEventGroup() ? removeOpenEventGroup(scan.getRecords()) : scan.getRecords();
      for (final LogicalBackupRecord record : committedRecords) {
        if (expectedSequence >= 0 && record.getSequence() != expectedSequence) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_SEQUENCE_IS_NOT_CONTINUOUS_ACROSS_SEGMENTS_ARG_8547BC6E,
                  manifestPath));
        }
        expectedSequence = record.getSequence() + 1;
        records.add(record);
        operationCounts.merge(record.getRecordType().name(), 1L, Long::sum);
      }
    }

    if (!allowIncomplete) {
      validateUnlistedSegments(streamDirectory, listedSegments);
      final long skippedEventCount =
          operationCounts.getOrDefault(LogicalBackupRecordType.SKIPPED_EVENT.name(), 0L);
      if (skippedEventCount > 0) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_CONTAINS_SKIPPED_EVENTS_ARG_E69FD599,
                manifestPath));
      }
      if (skippedEventCount != manifest.skippedEventCount
          || !operationCounts.equals(manifest.operationCounts)
          || (records.isEmpty()
              ? manifest.firstSequence != -1 || manifest.lastSequence != -1
              : records.get(0).getSequence() != manifest.firstSequence
                  || records.get(records.size() - 1).getSequence() != manifest.lastSequence)) {
        throw new IOException(
            String.format(
                LogicalBackupMessages
                    .EXCEPTION_LOGICAL_BACKUP_MANIFEST_COUNTERS_DO_NOT_MATCH_SEGMENT_CONTENTS_ARG_946818BB,
                manifestPath));
      }
    }
    return new BackupStream(manifestPath, manifest, records, toEventGroups(records));
  }

  private static void validateReplayableRequest(final LogicalBackupRecord record)
      throws IOException {
    if (record.getRecordType() != LogicalBackupRecordType.PIPE_REQUEST) {
      return;
    }
    if (record.getRequestVersion() != IoTDBSinkRequestVersion.VERSION_1.getVersion()) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_REQUEST_VERSION_ARG_IS_NOT_SUPPORTED_393457D7,
              record.getRequestVersion()));
    }
    if (!REPLAYABLE_REQUEST_TYPES.contains(record.getRequestType())) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_IS_NOT_ALLOWED_7F1CDD38,
              record.getRequestType()));
    }
  }

  private static List<Path> discoverManifests(final Path source) throws IOException {
    if (!Files.isSymbolicLink(source)
        && Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)
        && LogicalBackupFormat.MANIFEST_FILE_NAME.equals(source.getFileName().toString())) {
      return Collections.singletonList(source);
    }
    if (!Files.isDirectory(source)) {
      return Collections.emptyList();
    }
    try (final Stream<Path> paths = Files.walk(source)) {
      return paths
          .filter(path -> !Files.isSymbolicLink(path))
          .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
          .filter(
              path -> LogicalBackupFormat.MANIFEST_FILE_NAME.equals(path.getFileName().toString()))
          .sorted()
          .collect(Collectors.toList());
    }
  }

  private static Path resolveSegmentPath(final Path streamDirectory, final String segmentFile) {
    if (segmentFile == null) {
      return null;
    }
    final Path relativePath;
    try {
      relativePath = Path.of(segmentFile);
    } catch (final RuntimeException e) {
      return null;
    }
    if (relativePath.isAbsolute() || relativePath.getNameCount() != 1) {
      return null;
    }
    final Path segmentPath = streamDirectory.resolve(relativePath).normalize();
    return segmentPath.startsWith(streamDirectory)
            && segmentPath.getParent().equals(streamDirectory)
        ? segmentPath
        : null;
  }

  private static void validateManifest(
      final LogicalBackupManifest manifest, final Path manifestPath) throws IOException {
    if (manifest == null
        || !LogicalBackupFormat.FORMAT_NAME.equals(manifest.formatName)
        || !LogicalBackupFormat.FORMAT_VERSION.equals(manifest.formatVersion)
        || manifest.streamId == null
        || manifest.backupId == null
        || manifest.timestampPrecision == null
        || manifest.maxRecordBytes <= 0
        || manifest.maxRecordBytes > LogicalBackupFormat.MAX_RECORD_BYTES
        || manifest.skippedEventCount < 0) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7,
              manifestPath));
    }
    if (manifest.segments == null || manifest.segments.isEmpty()) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_MANIFEST_HAS_NO_SEGMENTS_ARG_AFB5C138,
              manifestPath));
    }
    if (manifest.operationCounts == null) {
      manifest.operationCounts = new LinkedHashMap<>();
    }
  }

  private static void validateSegmentManifest(
      final LogicalBackupManifest.Segment segment,
      final LogicalBackupSegmentReader.ScanResult scan,
      final Path segmentPath)
      throws IOException {
    final LogicalBackupSegmentReader.Footer footer = scan.getFooter();
    if (!"SEALED".equals(segment.status)
        || segment.firstSequence != footer.getFirstSequence()
        || segment.lastSequence != footer.getLastSequence()
        || segment.recordCount != footer.getRecordCount()
        || segment.sizeBytes != scan.getValidLength()
        || !LogicalBackupFormat.toHex(footer.getDigest()).equals(segment.sha256)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_METADATA_MISMATCH_ARG_376FD0B3,
              segmentPath));
    }
  }

  private static void validateUnlistedSegments(
      final Path streamDirectory, final Set<Path> listedSegments) throws IOException {
    try (final Stream<Path> paths = Files.list(streamDirectory)) {
      final Path unlisted =
          paths
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().endsWith(".pwal"))
              .map(path -> path.toAbsolutePath().normalize())
              .filter(path -> !listedSegments.contains(path))
              .findFirst()
              .orElse(null);
      if (unlisted != null) {
        throw new IOException(
            String.format(
                LogicalBackupMessages.EXCEPTION_UNLISTED_LOGICAL_BACKUP_SEGMENT_ARG_FF548C0B,
                unlisted));
      }
    }
  }

  private static List<LogicalBackupRecord> removeOpenEventGroup(
      final List<LogicalBackupRecord> records) {
    for (int i = records.size() - 1; i >= 0; i--) {
      if (records.get(i).getRecordType() == LogicalBackupRecordType.EVENT_BEGIN) {
        return records.subList(0, i);
      }
    }
    return records;
  }

  private static List<EventGroup> toEventGroups(final List<LogicalBackupRecord> records)
      throws IOException {
    final List<EventGroup> groups = new ArrayList<>();
    final Set<UUID> eventGroupIds = new HashSet<>();
    UUID eventGroupId = null;
    long firstSequence = -1;
    String metadata = "";
    final List<LogicalBackupRecord> requests = new ArrayList<>();
    for (final LogicalBackupRecord record : records) {
      if (record.getRecordType() == LogicalBackupRecordType.EVENT_BEGIN) {
        eventGroupId = record.getEventGroupId();
        if (!eventGroupIds.add(eventGroupId)) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_DUPLICATE_LOGICAL_BACKUP_EVENT_GROUP_ID_ARG_15B04C89,
                  eventGroupId));
        }
        firstSequence = record.getSequence();
        metadata = record.getMetadata();
        requests.clear();
      } else if (record.getRecordType() == LogicalBackupRecordType.PIPE_REQUEST) {
        requests.add(record);
      } else if (record.getRecordType() == LogicalBackupRecordType.EVENT_COMMIT) {
        groups.add(
            new EventGroup(
                eventGroupId,
                firstSequence,
                record.getSequence(),
                metadata,
                new ArrayList<>(requests)));
        eventGroupId = null;
        requests.clear();
      }
    }
    return Collections.unmodifiableList(groups);
  }

  public static class BackupStream {
    private final Path manifestPath;
    private final LogicalBackupManifest manifest;
    private final List<LogicalBackupRecord> records;
    private final List<EventGroup> eventGroups;

    private BackupStream(
        final Path manifestPath,
        final LogicalBackupManifest manifest,
        final List<LogicalBackupRecord> records,
        final List<EventGroup> eventGroups) {
      this.manifestPath = manifestPath;
      this.manifest = manifest;
      this.records = Collections.unmodifiableList(records);
      this.eventGroups = eventGroups;
    }

    public Path getManifestPath() {
      return manifestPath;
    }

    public LogicalBackupManifest getManifest() {
      return manifest;
    }

    public List<LogicalBackupRecord> getRecords() {
      return records;
    }

    public List<EventGroup> getEventGroups() {
      return eventGroups;
    }
  }

  public static class EventGroup {
    private final UUID eventGroupId;
    private final long firstSequence;
    private final long lastSequence;
    private final String metadata;
    private final List<LogicalBackupRecord> requests;

    private EventGroup(
        final UUID eventGroupId,
        final long firstSequence,
        final long lastSequence,
        final String metadata,
        final List<LogicalBackupRecord> requests) {
      this.eventGroupId = eventGroupId;
      this.firstSequence = firstSequence;
      this.lastSequence = lastSequence;
      this.metadata = metadata;
      this.requests = Collections.unmodifiableList(requests);
    }

    public UUID getEventGroupId() {
      return eventGroupId;
    }

    public long getFirstSequence() {
      return firstSequence;
    }

    public long getLastSequence() {
      return lastSequence;
    }

    public String getMetadata() {
      return metadata;
    }

    public List<LogicalBackupRecord> getRequests() {
      return requests;
    }
  }
}
