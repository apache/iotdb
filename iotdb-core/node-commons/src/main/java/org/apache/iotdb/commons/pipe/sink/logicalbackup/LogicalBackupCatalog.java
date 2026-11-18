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
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The portable, authoritative root manifest of a backup task. */
public class LogicalBackupCatalog {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  public String formatName = LogicalBackupFormat.FORMAT_NAME;
  public String formatVersion = LogicalBackupFormat.FORMAT_VERSION;
  public String backupId;
  public String pipeName;
  public String status = "WRITING";
  public String sourceClusterId;
  public String sourceVersion;
  public String timestampPrecision;
  public List<String> scope = new ArrayList<>();
  public List<String> model = new ArrayList<>();
  public Map<String, String> filters = new LinkedHashMap<>();
  public Map<String, String> captureOptions = new LinkedHashMap<>();
  public boolean includeAuth;
  public String mode = "snapshot";
  public String createdAt = Instant.now().toString();
  public String capturedAt;
  public String sealedAt;
  public Map<String, Long> sourceHighWatermark = new LinkedHashMap<>();
  public Map<String, Long> operationCounts = new LinkedHashMap<>();
  public long skippedEventCount;
  public boolean recovered;
  public List<StreamEntry> streams = new ArrayList<>();

  public static class StreamEntry {
    public String streamId;
    public String streamType;
    public FileEntry manifest;
    public List<FileEntry> segments = new ArrayList<>();
  }

  public static class FileEntry {
    public String file;
    public long sizeBytes;
    public String sha256;
  }

  public void seal(final Path root, final List<BackupStream> backupStreams) throws IOException {
    collect(root, backupStreams);
    write(root);
  }

  public void collect(final Path root, final List<BackupStream> backupStreams) throws IOException {
    streams.clear();
    operationCounts.clear();
    sourceHighWatermark.clear();
    skippedEventCount = 0;
    recovered = false;
    for (final BackupStream stream : backupStreams) {
      final LogicalBackupManifest manifest = stream.getManifest();
      if (!backupId.equals(manifest.backupId)) {
        throw invalid(root);
      }
      if (streams.isEmpty()) {
        sourceClusterId = manifest.sourceClusterId;
        sourceVersion = manifest.sourceVersion;
        timestampPrecision = manifest.timestampPrecision;
      }
      if (!java.util.Objects.equals(pipeName, manifest.pipeName)
          || !java.util.Objects.equals(sourceClusterId, manifest.sourceClusterId)
          || !java.util.Objects.equals(sourceVersion, manifest.sourceVersion)
          || !java.util.Objects.equals(timestampPrecision, manifest.timestampPrecision)
          || !captureOptions.equals(manifest.captureOptions)) {
        throw invalid(root);
      }
      final StreamEntry entry = new StreamEntry();
      entry.streamId = manifest.streamId;
      entry.streamType = manifest.streamType;
      entry.manifest = describe(root, stream.getManifestPath());
      for (final LogicalBackupManifest.Segment segment : manifest.segments) {
        entry.segments.add(
            describe(root, stream.getManifestPath().getParent().resolve(segment.file)));
      }
      streams.add(entry);
      manifest.operationCounts.forEach(
          (key, value) -> operationCounts.merge(key, value, Long::sum));
      sourceHighWatermark.put(manifest.streamId, manifest.lastDurableSequence);
      skippedEventCount += manifest.skippedEventCount;
      recovered |= manifest.recovered;
    }
    capturedAt = Instant.now().toString();
    sealedAt = capturedAt;
    status = skippedEventCount > 0 ? "INCOMPLETE" : "SEALED";
  }

  public void write(final Path root) throws IOException {
    rejectSymbolicLinks(root);
    Files.createDirectories(root);
    final Path target = root.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    if (Files.isSymbolicLink(target)) {
      throw invalid(target);
    }
    final Path temporary = Files.createTempFile(root, ".manifest-", ".tmp");
    try {
      final ByteBuffer bytes = StandardCharsets.UTF_8.encode(GSON.toJson(this));
      try (final FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        while (bytes.hasRemaining()) {
          channel.write(bytes);
        }
        channel.force(true);
      }
      try {
        Files.move(
            temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final java.nio.file.AtomicMoveNotSupportedException e) {
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  /** Returns null for a legacy stream manifest; malformed root manifests always fail. */
  public static LogicalBackupCatalog load(final Path manifest) throws IOException {
    rejectSymbolicLinks(manifest);
    if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    if (Files.size(manifest) > 16 * 1024 * 1024) {
      throw invalid(manifest);
    }
    try {
      final com.google.gson.JsonObject json =
          GSON.fromJson(Files.readString(manifest), com.google.gson.JsonObject.class);
      if (json == null) {
        throw invalid(manifest);
      }
      if (json.has("streamId")) {
        return null;
      }
      final LogicalBackupCatalog catalog = GSON.fromJson(json, LogicalBackupCatalog.class);
      if (!LogicalBackupFormat.FORMAT_NAME.equals(catalog.formatName)
          || !LogicalBackupFormat.FORMAT_VERSION.equals(catalog.formatVersion)
          || catalog.backupId == null
          || catalog.scope == null
          || catalog.model == null
          || catalog.filters == null
          || catalog.captureOptions == null
          || catalog.streams == null
          || catalog.operationCounts == null
          || catalog.sourceHighWatermark == null
          || catalog.skippedEventCount < 0
          || catalog.scope.isEmpty()
          || !List.of("config", "schema", "data").containsAll(catalog.scope)
          || catalog.model.isEmpty()
          || !List.of("tree", "table").containsAll(catalog.model)
          || !List.of("WRITING", "RECOVERED", "SEALED", "INCOMPLETE").contains(catalog.status)) {
        throw invalid(manifest);
      }
      return catalog;
    } catch (final JsonParseException | IllegalStateException e) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_INVALID_LOGICAL_BACKUP_ROOT_MANIFEST_FILE_CHECKSUM_OR_STATE_ARG_3EDC33F3,
              manifest),
          e);
    }
  }

  public static FileEntry describe(final Path root, final Path path) throws IOException {
    final Path normalizedRoot = root.toAbsolutePath().normalize();
    final Path normalizedPath = path.toAbsolutePath().normalize();
    if (!normalizedPath.startsWith(normalizedRoot)) {
      throw invalid(path);
    }
    final FileEntry entry = new FileEntry();
    entry.file = normalizedRoot.relativize(normalizedPath).toString().replace('\\', '/');
    resolve(normalizedRoot, entry.file);
    entry.sizeBytes = Files.size(normalizedPath);
    entry.sha256 = sha256(normalizedPath);
    return entry;
  }

  public static Path resolve(final Path root, final String relative) throws IOException {
    if (relative == null
        || relative.isBlank()
        || relative.indexOf('\\') >= 0
        || relative.indexOf(':') >= 0
        || relative.startsWith("/")
        || relative.chars().anyMatch(Character::isISOControl)) {
      throw invalid(root);
    }
    for (final String part : relative.split("/", -1)) {
      if (part.isEmpty() || ".".equals(part) || "..".equals(part)) {
        throw invalid(root);
      }
    }
    final Path normalizedRoot = root.toAbsolutePath().normalize();
    final Path path = normalizedRoot.resolve(relative).normalize();
    if (!path.startsWith(normalizedRoot) || path.equals(normalizedRoot)) {
      throw invalid(path);
    }
    rejectSymbolicLinks(path);
    return path;
  }

  public static void rejectSymbolicLinks(final Path path) throws IOException {
    final Path absolute = path.toAbsolutePath().normalize();
    Path current = absolute.getRoot();
    for (final Path part : absolute) {
      current = current.resolve(part);
      if (Files.isSymbolicLink(current)) {
        throw invalid(current);
      }
    }
  }

  public static String sha256(final Path path) throws IOException {
    final MessageDigest digest = LogicalBackupFormat.newSha256();
    try (final InputStream input = Files.newInputStream(path)) {
      final byte[] buffer = new byte[64 * 1024];
      int length;
      while ((length = input.read(buffer)) != -1) {
        digest.update(buffer, 0, length);
      }
    }
    return LogicalBackupFormat.toHex(digest.digest());
  }

  static IOException invalid(final Path path) {
    return new IOException(
        String.format(
            LogicalBackupMessages
                .EXCEPTION_INVALID_LOGICAL_BACKUP_ROOT_MANIFEST_FILE_CHECKSUM_OR_STATE_ARG_3EDC33F3,
            path));
  }
}
