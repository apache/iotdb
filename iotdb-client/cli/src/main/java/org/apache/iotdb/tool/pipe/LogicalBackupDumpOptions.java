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

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupCatalog;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupSettings;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Converts the public dump contract to source and sink parameters before any remote mutation. */
final class LogicalBackupDumpOptions {

  final LogicalBackupCatalog catalog = new LogicalBackupCatalog();
  final Map<String, String> source = new LinkedHashMap<>();
  final Map<String, String> sink = new LinkedHashMap<>();
  final String resume;
  final long until;

  LogicalBackupDumpOptions(final CommandLine line, final boolean legacy) throws IOException {
    catalog.scope =
        choices(
            line.getOptionValue("scope", legacy ? "config,schema,data" : ""),
            List.of("config", "schema", "data"),
            "scope");
    catalog.model =
        choices(line.getOptionValue("model", "tree,table"), List.of("tree", "table"), "model");
    catalog.mode = line.getOptionValue("mode", "snapshot");
    if (!List.of("snapshot", "continuous").contains(catalog.mode)) {
      throw invalid("mode");
    }
    final String includeAuth = line.getOptionValue("include-auth", "false");
    if (!List.of("true", "false").contains(includeAuth)) {
      throw invalid("include-auth");
    }
    catalog.includeAuth = Boolean.parseBoolean(includeAuth);
    if (catalog.includeAuth && !catalog.scope.contains("config")) {
      throw invalid("include-auth");
    }
    if (line.hasOption("stop-sequence")) {
      throw new IOException(
          CliMessages
              .EXCEPTION_STOP_SEQUENCE_REQUIRES_A_STABLE_SOURCE_SEQUENCE_CAPABILITY_USE_UNTIL_WITH_THIS_SERVER_VERSION_8323B09E);
    }
    if ("continuous".equals(catalog.mode)) {
      if (!line.hasOption("until")) {
        throw invalid("until");
      }
      try {
        until = Instant.parse(line.getOptionValue("until")).toEpochMilli();
      } catch (final java.time.DateTimeException e) {
        throw invalid("until");
      }
      if (until <= System.currentTimeMillis() && !"append".equals(line.getOptionValue("resume"))) {
        throw invalid("until");
      }
    } else {
      if (line.hasOption("until")) {
        throw invalid("until");
      }
      until = -1;
    }
    resume = line.getOptionValue("resume", "fail-if-exists");
    if (!List.of("fail-if-exists", "append", "new").contains(resume)) {
      throw invalid("resume");
    }
    catalog.captureOptions.put("scope", String.join(",", catalog.scope));
    catalog.captureOptions.put("model", String.join(",", catalog.model));
    catalog.captureOptions.put("mode", catalog.mode);
    catalog.captureOptions.put("include-auth", includeAuth);
    for (final String key : List.of("include", "exclude", "start-time", "end-time", "until")) {
      if (line.hasOption(key)) {
        catalog.filters.put(key, line.getOptionValue(key));
        catalog.captureOptions.put(key, line.getOptionValue(key));
      }
    }
    catalog.filters.put("time-boundary", "closed-closed");
    final List<String> inclusion = new ArrayList<>();
    if (catalog.scope.contains("config")) {
      inclusion.add("schema.database");
      inclusion.add("schema.ttl");
      inclusion.add("schema.timeseries.template.create");
      inclusion.add("schema.timeseries.template.set");
      inclusion.add("schema.timeseries.template.unset");
      inclusion.add("schema.timeseries.template.alter");
      inclusion.add("schema.timeseries.template.drop");
    }
    if (catalog.scope.contains("schema")) {
      inclusion.add("schema");
    }
    if (catalog.scope.contains("data")) {
      inclusion.add("data");
    }
    if (catalog.includeAuth) {
      inclusion.add("auth");
    }
    source.put("source", "iotdb-source");
    source.put("source.inclusion", String.join(",", inclusion));
    source.put("source.inclusion.exclusion", catalog.includeAuth ? "" : "auth");
    source.put("source.skipif", "");
    source.put("source.capture.tree", Boolean.toString(catalog.model.contains("tree")));
    source.put("source.capture.table", Boolean.toString(catalog.model.contains("table")));
    source.put("source.mode", "snapshot".equals(catalog.mode) ? "snapshot" : "live");
    source.put("source.realtime.enable", Boolean.toString("continuous".equals(catalog.mode)));
    source.put("source.logical-backup.snapshot", Boolean.toString("snapshot".equals(catalog.mode)));
    if (until > 0) {
      source.put("source.logical-backup.until", Long.toString(until));
      source.put("source.realtime.mode", "forced-log");
    }
    // Decode TsFiles so the source applies exact path/time filters to logical rows.
    source.put("source.mods.enable", "true");
    for (final String key : List.of("start-time", "end-time")) {
      if (line.hasOption(key)) {
        source.put("source." + key, line.getOptionValue(key));
      }
    }
    applyObjectFilters(line);
    sink.put("sink", "logical-backup-sink");
    sink.put("sink.resume", "append");
    final Map<String, String> defaults = new LinkedHashMap<>();
    defaults.put("segment-size-bytes", "268435456");
    defaults.put("max-record-bytes", "67108864");
    defaults.put("fsync-policy", "always");
    defaults.put("on-unsupported", "fail");
    defaults.put("include-heartbeat", "false");
    for (final Map.Entry<String, String> entry : defaults.entrySet()) {
      catalog.captureOptions.put(
          entry.getKey(), line.getOptionValue(entry.getKey(), entry.getValue()));
      sink.put(
          "sink."
              + ("on-unsupported".equals(entry.getKey()) ? "on-unsupported-event" : entry.getKey()),
          catalog.captureOptions.get(entry.getKey()));
    }
    for (final String key : List.of("fsync-batch-operations", "fsync-period-ms")) {
      if (line.hasOption(key)) {
        sink.put("sink." + key, line.getOptionValue(key));
        catalog.captureOptions.put(key, line.getOptionValue(key));
      }
    }
    sink.put("sink.capture-options", new Gson().toJson(catalog.captureOptions));
    // Reuse the same validation as all three server-side writers.
    final Map<String, String> validation = new LinkedHashMap<>(sink);
    validation.put("sink.dir", line.getOptionValue("backup-dir", "."));
    new LogicalBackupSettings(new PipeParameters(validation), "backup");
  }

  private void applyObjectFilters(final CommandLine line) throws IOException {
    if (!line.hasOption("include") && !line.hasOption("exclude")) {
      return;
    }
    if (catalog.model.size() != 1) {
      throw new IOException(
          CliMessages
              .EXCEPTION_OBJECT_FILTERS_REQUIRE_MODEL_TREE_OR_MODEL_TABLE_SPLIT_MIXED_MODEL_FILTERED_DUMPS_INTO_SEPARATE_BACKUPS_0488699E);
    }
    if (catalog.model.contains("tree")) {
      if (line.hasOption("include")) {
        source.put("source.path.inclusion", line.getOptionValue("include"));
      }
      if (line.hasOption("exclude")) {
        source.put("source.path.exclusion", line.getOptionValue("exclude"));
      }
      final TreePattern pattern =
          TreePattern.parsePipePatternFromSourceParameters(new PipeParameters(source));
      if (!pattern.isLegal()
          || ((catalog.scope.contains("config") || catalog.scope.contains("schema"))
              && !(pattern instanceof IoTDBTreePatternOperations
                  && ((IoTDBTreePatternOperations) pattern).isPrefixOrFullPath()))) {
        throw invalid("include/exclude");
      }
      return;
    }
    // TablePattern accepts independent database/table regular expressions. Reject correlated
    // exclusions rather than silently backing up their Cartesian product.
    final String[] include = tableFilter(line.getOptionValue("include", ".*/.*"));
    final String[] exclude =
        line.hasOption("exclude") ? tableFilter(line.getOptionValue("exclude")) : null;
    if (exclude != null && !".*".equals(exclude[0]) && !".*".equals(exclude[1])) {
      throw new IOException(
          CliMessages
              .EXCEPTION_TABLE_FILTERS_USE_DATABASE_REGEX_TABLE_REGEX_EXCLUSIONS_MUST_USE_FOR_EITHER_DATABASE_OR_TABLE_42F0FA23);
    }
    for (int index = 0; index < 2; index++) {
      String regex = include[index];
      if (exclude != null && !".*".equals(exclude[index])) {
        regex = "(?!(?:" + exclude[index] + ")$)(?:" + regex + ")";
      }
      source.put(index == 0 ? "source.database-name" : "source.table-name", regex);
    }
  }

  private static String[] tableFilter(final String filter) throws IOException {
    final String[] parts = filter.split("/", -1);
    if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
      throw new IOException(
          CliMessages
              .EXCEPTION_TABLE_FILTERS_USE_DATABASE_REGEX_TABLE_REGEX_EXCLUSIONS_MUST_USE_FOR_EITHER_DATABASE_OR_TABLE_42F0FA23);
    }
    try {
      Pattern.compile(parts[0]);
      Pattern.compile(parts[1]);
    } catch (final java.util.regex.PatternSyntaxException e) {
      throw invalid("include/exclude");
    }
    return parts;
  }

  private static List<String> choices(
      final String value, final List<String> allowed, final String key) throws IOException {
    final List<String> values = Arrays.asList(value.split(",", -1));
    if (values.isEmpty()
        || !allowed.containsAll(values)
        || values.stream().distinct().count() != values.size()) {
      throw invalid(key);
    }
    // Canonical ordering makes equivalent requests compare equal when resuming.
    final List<String> canonical = new ArrayList<>(allowed);
    canonical.retainAll(values);
    return canonical;
  }

  String createSql(final String pipeName, final String root, final String backupId) {
    final Map<String, String> sinkOptions = new LinkedHashMap<>(sink);
    sinkOptions.put("sink.dir", root);
    sinkOptions.put("sink.backup-id", backupId);
    return "CREATE PIPE "
        + pipeName
        + " WITH SOURCE ("
        + sqlOptions(source)
        + ") WITH SINK ("
        + sqlOptions(sinkOptions)
        + ")";
  }

  private static String sqlOptions(final Map<String, String> options) {
    final List<String> values = new ArrayList<>();
    options.forEach((key, value) -> values.add("'" + key + "'='" + value.replace("'", "''") + "'"));
    return String.join(",", values);
  }

  private static IOException invalid(final String key) {
    return new IOException(
        String.format(CliMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_DUMP_OPTION_ARG_894E11E0, key));
  }
}
