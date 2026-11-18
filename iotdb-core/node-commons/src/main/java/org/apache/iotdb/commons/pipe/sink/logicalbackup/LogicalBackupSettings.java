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
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/** Settings shared by ConfigRegion, SchemaRegion and DataRegion backup writers. */
public final class LogicalBackupSettings {

  public final Path root;
  public final String backupId;
  public final String resume;
  public final long segmentSize;
  public final int maxRecord;
  public final LogicalBackupWriter.FsyncPolicy fsync;
  public final int fsyncBatch;
  public final long fsyncPeriod;
  public final boolean includeHeartbeat;
  public final String unsupported;
  public final Map<String, String> captureOptions;

  public LogicalBackupSettings(final PipeParameters parameters, final String defaultBackupId)
      throws IOException {
    try {
      final String directory = value(parameters, "dir", "");
      if (directory.isBlank()) {
        throw invalid("dir");
      }
      root = Path.of(directory).toAbsolutePath().normalize();
      LogicalBackupCatalog.rejectSymbolicLinks(root);
      backupId = value(parameters, "backup-id", defaultBackupId);
      validateBackupId(backupId);
      resume = value(parameters, "resume", "fail-if-exists").toLowerCase(Locale.ROOT);
      if (!List.of("fail-if-exists", "append", "new").contains(resume)) {
        throw invalid("resume");
      }
      segmentSize =
          Long.parseLong(
              value(
                  parameters,
                  "segment-size-bytes",
                  Long.toString(PipeSinkConstant.LOGICAL_BACKUP_SEGMENT_SIZE_BYTES_DEFAULT_VALUE)));
      maxRecord =
          Integer.parseInt(
              value(
                  parameters,
                  "max-record-bytes",
                  Integer.toString(
                      PipeSinkConstant.LOGICAL_BACKUP_MAX_RECORD_BYTES_DEFAULT_VALUE)));
      fsync =
          LogicalBackupWriter.FsyncPolicy.valueOf(
              value(parameters, "fsync-policy", "always").toUpperCase(Locale.ROOT));
      fsyncBatch = Integer.parseInt(value(parameters, "fsync-batch-operations", "1000"));
      fsyncPeriod = Long.parseLong(value(parameters, "fsync-period-ms", "1000"));
      includeHeartbeat = booleanValue(parameters, "include-heartbeat");
      unsupported = value(parameters, "on-unsupported-event", "fail");
      if (maxRecord <= 0
          || maxRecord > LogicalBackupFormat.MAX_RECORD_BYTES
          || segmentSize < maxRecord
          || fsyncBatch <= 0
          || fsyncPeriod <= 0
          || !List.of("fail", "skip").contains(unsupported)
          || (has(parameters, "fsync-batch-operations")
              && fsync != LogicalBackupWriter.FsyncPolicy.BATCH)
          || (has(parameters, "fsync-period-ms")
              && fsync != LogicalBackupWriter.FsyncPolicy.PERIODIC)) {
        throw invalid("writer");
      }
      captureOptions =
          new Gson()
              .fromJson(
                  value(parameters, "capture-options", "{}"),
                  new TypeToken<LinkedHashMap<String, String>>() {}.getType());
      if (captureOptions == null) {
        throw invalid("capture-options");
      }
      if (!List.of(
                  "scope",
                  "model",
                  "mode",
                  "include-auth",
                  "include",
                  "exclude",
                  "start-time",
                  "end-time",
                  "until",
                  "segment-size-bytes",
                  "max-record-bytes",
                  "fsync-policy",
                  "on-unsupported",
                  "include-heartbeat",
                  "fsync-batch-operations",
                  "fsync-period-ms")
              .containsAll(captureOptions.keySet())
          || captureOptions.containsValue(null)) {
        throw invalid("capture-options");
      }
    } catch (final IllegalArgumentException | com.google.gson.JsonParseException e) {
      throw new IOException(
          LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_WRITER_SETTINGS_4E94FAAB, e);
    }
  }

  public void requireSharedResume(final PipeParameters parameters) throws IOException {
    if (!has(parameters, "resume") || !"append".equals(resume)) {
      throw new IOException(
          LogicalBackupMessages
              .EXCEPTION_CONFIGREGION_AND_DATAREGION_BACKUPS_REQUIRE_EXPLICIT_SINK_RESUME_APPEND_AND_A_SHARED_BACKUP_DIRECTORY_BECBAB90);
    }
  }

  public LogicalBackupWriter open(
      final String streamType, final int regionId, final LogicalBackupManifest manifest)
      throws IOException {
    Path directory = root.resolve(backupId).resolve(streamType + "-" + regionId);
    if ("new".equals(resume)) {
      directory = directory.resolveSibling(directory.getFileName() + "-new-" + UUID.randomUUID());
    }
    manifest.captureOptions = new LinkedHashMap<>(captureOptions);
    return new LogicalBackupWriter(
        directory,
        manifest,
        segmentSize,
        maxRecord,
        fsync,
        fsyncBatch,
        fsyncPeriod,
        "append".equals(resume));
  }

  public static void validateBackupId(final String value) throws IOException {
    if (value == null
        || !value.matches("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
        || value.endsWith(".")
        || value.matches("(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\\..*)?")) {
      throw invalid("backup-id");
    }
  }

  private static boolean booleanValue(final PipeParameters parameters, final String key)
      throws IOException {
    final String value = value(parameters, key, "false");
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      throw invalid(key);
    }
    return Boolean.parseBoolean(value);
  }

  private static boolean has(final PipeParameters parameters, final String key) {
    return parameters.hasAnyAttributes("connector." + key, "sink." + key);
  }

  private static String value(
      final PipeParameters parameters, final String key, final String fallback) throws IOException {
    if (parameters.getAttribute().containsKey("connector." + key)
        && parameters.getAttribute().containsKey("sink." + key)) {
      throw invalid(key);
    }
    return parameters.getStringOrDefault(
        Arrays.asList("connector." + key, "sink." + key), fallback);
  }

  private static IOException invalid(final String key) {
    return new IOException(
        String.format(
            LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_SETTING_ARG_6D261EEB, key));
  }
}
