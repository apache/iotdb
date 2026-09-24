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
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.BackupStream;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupArchiveReader.EventGroup;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecord;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.session.Session;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/** Command line inspector, verifier, archiver and importer for Pipe logical backups. */
public final class PipeLogicalBackupTool {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final String COMMAND_INSPECT = "inspect";
  private static final String COMMAND_VERIFY = "verify";
  private static final String COMMAND_EXPORT = "export";
  private static final String COMMAND_IMPORT = "import";
  private static final String OPTION_INPUT = "input";
  private static final String OPTION_OUTPUT = "output";
  private static final String OPTION_RESUME = "resume";
  private static final String OPTION_FORMAT = "format";
  private static final String OPTION_DEEP = "deep";
  // Compatibility aliases for the initial command-line prototype.
  private static final String OPTION_SOURCE = "source";
  private static final String OPTION_TARGET = "target";
  private static final String OPTION_HOST = "host";
  private static final String OPTION_PORT = "port";
  private static final String OPTION_USER = "user";
  private static final String OPTION_PASSWORD_STDIN = "password-stdin";
  private static final String OPTION_PASSWORD_ENV = "password-env";
  private static final String OPTION_CHECKPOINT = "checkpoint";
  private static final String OPTION_DRY_RUN = "dry-run";
  private static final String OPTION_ALLOW_INCOMPLETE = "allow-incomplete";
  private static final String OPTION_JSON = "json";
  private static final String OPTION_BACKUP_DIR = "backup-dir";
  private static final String OPTION_PIPE_NAME = "pipe-name";
  private static final String OPTION_BACKUP_ID = "backup-id";
  private static final String OPTION_TIMEOUT_SECONDS = "timeout-seconds";
  private static final String OPTION_POLL_INTERVAL_SECONDS = "poll-interval-seconds";
  private static final int MAX_ARCHIVE_ENTRIES = 1_000_000;
  private static final long MAX_ARCHIVE_BYTES = 4L * 1024 * 1024 * 1024 * 1024;
  private static final String CHECKPOINT_FORMAT_NAME =
      "iotdb-pipe-logical-backup-import-checkpoint";
  private static final String CHECKPOINT_FORMAT_VERSION = "1.0";

  private PipeLogicalBackupTool() {}

  public static void main(final String[] args) {
    int exitCode = 0;
    try {
      exitCode = run(args);
    } catch (final Exception e) {
      System.err.println(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_COMMAND_FAILED_ARG_9973B0C0, e.getMessage()));
      exitCode = 1;
    }
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(final String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return 0;
    }
    final String command = args[0].toLowerCase(java.util.Locale.ROOT);
    final CommandLine line = parse(command, java.util.Arrays.copyOfRange(args, 1, args.length));
    switch (command) {
      case COMMAND_INSPECT:
        return inspect(line);
      case COMMAND_VERIFY:
        return verify(line);
      case COMMAND_EXPORT:
        return export(line);
      case COMMAND_IMPORT:
        return importBackup(line);
      default:
        throw new ParseException(
            String.format(
                CliMessages.EXCEPTION_UNKNOWN_LOGICAL_BACKUP_COMMAND_ARG_79275619, command));
    }
  }

  private static CommandLine parse(final String command, final String[] args)
      throws ParseException {
    final Options options = new Options();
    addOption(options, OPTION_INPUT, true, false);
    addOption(options, OPTION_OUTPUT, true, false);
    addOption(options, OPTION_RESUME, true, false);
    addOption(options, OPTION_FORMAT, true, false);
    addOption(options, OPTION_DEEP, false, false);
    addOption(options, OPTION_SOURCE, true, false);
    addOption(options, OPTION_TARGET, true, false);
    addOption(options, OPTION_HOST, true, false);
    addOption(options, OPTION_PORT, true, false);
    addOption(options, OPTION_USER, true, false);
    addOption(options, OPTION_PASSWORD_STDIN, false, false);
    addOption(options, OPTION_PASSWORD_ENV, true, false);
    addOption(options, OPTION_CHECKPOINT, true, false);
    addOption(options, OPTION_DRY_RUN, false, false);
    addOption(options, OPTION_ALLOW_INCOMPLETE, false, false);
    addOption(options, OPTION_JSON, false, false);
    addOption(options, OPTION_BACKUP_DIR, true, false);
    addOption(options, OPTION_PIPE_NAME, true, false);
    addOption(options, OPTION_BACKUP_ID, true, false);
    addOption(options, OPTION_TIMEOUT_SECONDS, true, false);
    addOption(options, OPTION_POLL_INTERVAL_SECONDS, true, false);
    final CommandLine line = new DefaultParser().parse(options, args);
    final String input = optionValue(line, OPTION_INPUT, OPTION_SOURCE);
    if ((input == null || input.isBlank())
        && (!COMMAND_EXPORT.equals(command) || !line.hasOption(OPTION_BACKUP_DIR))) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA, OPTION_INPUT));
    }
    final String output = optionValue(line, OPTION_OUTPUT, OPTION_TARGET);
    if (COMMAND_EXPORT.equals(command) && (output == null || output.isBlank())) {
      throw new ParseException(
          CliMessages.EXCEPTION_OUTPUT_IS_REQUIRED_FOR_LOGICAL_BACKUP_EXPORT_603340B1);
    }
    final String format = line.getOptionValue(OPTION_FORMAT, "binary");
    if (COMMAND_EXPORT.equals(command) && !"binary".equalsIgnoreCase(format)) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EXPORT_FORMAT_ARG_A6D7DEB1, format));
    }
    return line;
  }

  private static void addOption(
      final Options options, final String name, final boolean hasArg, final boolean required) {
    options.addOption(Option.builder().longOpt(name).hasArg(hasArg).required(required).build());
  }

  private static int inspect(final CommandLine line) throws IOException {
    final List<BackupStream> streams = read(line);
    if (line.hasOption(OPTION_JSON)) {
      final List<Map<String, Object>> output = new ArrayList<>();
      for (final BackupStream stream : streams) {
        final Map<String, Object> item = new HashMap<>();
        item.put("manifest", stream.getManifest());
        item.put("recordCount", stream.getRecords().size());
        item.put("eventGroupCount", stream.getEventGroups().size());
        output.add(item);
      }
      System.out.println(GSON.toJson(output));
      return 0;
    }
    for (final BackupStream stream : streams) {
      System.out.println(
          String.format(
              CliMessages.LOG_STREAM_ARG_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_DE6BBAD0,
              stream.getManifest().streamId,
              stream.getRecords().size(),
              stream.getEventGroups().size()));
    }
    return 0;
  }

  private static int verify(final CommandLine line) throws IOException {
    final List<BackupStream> streams = read(line);
    long recordCount = 0;
    long eventCount = 0;
    for (final BackupStream stream : streams) {
      recordCount += stream.getRecords().size();
      eventCount += stream.getEventGroups().size();
    }
    System.out.println(
        String.format(
            CliMessages
                .LOG_LOGICAL_BACKUP_VERIFIED_ARG_STREAMS_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_5210360B,
            streams.size(),
            recordCount,
            eventCount));
    return 0;
  }

  private static int export(final CommandLine line) throws Exception {
    if (!line.hasOption(OPTION_INPUT) && !line.hasOption(OPTION_SOURCE)) {
      return managedExport(line);
    }
    final Path source = Path.of(input(line)).toAbsolutePath().normalize();
    final Path target =
        Path.of(optionValue(line, OPTION_OUTPUT, OPTION_TARGET)).toAbsolutePath().normalize();
    final List<BackupStream> streams = read(line);
    return exportArchive(source, target, streams);
  }

  private static int managedExport(final CommandLine line) throws Exception {
    final String host = required(line, OPTION_HOST);
    final int port = Integer.parseInt(required(line, OPTION_PORT));
    final String user = line.getOptionValue(OPTION_USER, "root");
    final String password = password(line);
    final Path backupRoot = Path.of(required(line, OPTION_BACKUP_DIR)).toAbsolutePath().normalize();
    final Path target =
        Path.of(optionValue(line, OPTION_OUTPUT, OPTION_TARGET)).toAbsolutePath().normalize();
    final String pipeName =
        line.getOptionValue(
            OPTION_PIPE_NAME,
            "logical_backup_export_" + UUID.randomUUID().toString().replace('-', '_'));
    validateIdentifier(pipeName, OPTION_PIPE_NAME);
    final String backupId = line.getOptionValue(OPTION_BACKUP_ID, pipeName);
    validateIdentifier(backupId, OPTION_BACKUP_ID);
    final long timeoutSeconds = longOption(line, OPTION_TIMEOUT_SECONDS, 3600L);
    final long pollIntervalSeconds = longOption(line, OPTION_POLL_INTERVAL_SECONDS, 2L);
    if (timeoutSeconds <= 0 || pollIntervalSeconds <= 0) {
      throw new IOException(
          CliMessages
              .EXCEPTION_LOGICAL_BACKUP_EXPORT_TIMEOUT_AND_POLL_INTERVAL_MUST_BE_POSITIVE_38622769);
    }

    final String createSql =
        "CREATE PIPE "
            + pipeName
            + " WITH SOURCE ('source'='iotdb-source','source.mode'='snapshot','source.inclusion'='all',"
            + "'source.capture.tree'='true','source.capture.table'='true',"
            + "'source.realtime.enable'='false') WITH SINK ('sink'='logical-backup-sink','sink.dir'='"
            + sqlString(backupRoot.toString())
            + "','sink.backup-id'='"
            + sqlString(backupId)
            + "','sink.resume'='append')";
    boolean created = false;
    try (final Session session =
        new Session.Builder().host(host).port(port).username(user).password(password).build()) {
      session.open();
      session.executeNonQueryStatement(createSql);
      created = true;
      session.executeNonQueryStatement("START PIPE " + pipeName);
      waitForSnapshotPipe(session, pipeName, timeoutSeconds, pollIntervalSeconds);
      final Path source = backupRoot.resolve(backupId).normalize();
      final List<BackupStream> streams = new LogicalBackupArchiveReader().read(source, false);
      return exportArchive(source, target, streams);
    } finally {
      if (created) {
        try (final Session cleanup =
            new Session.Builder().host(host).port(port).username(user).password(password).build()) {
          cleanup.open();
          try {
            cleanup.executeNonQueryStatement("STOP PIPE " + pipeName);
          } catch (final Exception ignored) {
            // A completed snapshot pipe may already have been auto-dropped.
          }
          try {
            cleanup.executeNonQueryStatement("DROP PIPE " + pipeName);
          } catch (final Exception ignored) {
            // Keep the original export failure when cleanup races with auto-drop.
          }
        } catch (final Exception ignored) {
          // The archive is complete once the generated pipe disappears; cleanup is best effort.
        }
      }
    }
  }

  private static int exportArchive(
      final Path source, final Path target, final List<BackupStream> streams) throws IOException {
    final Path normalizedSource = source.toAbsolutePath().normalize();
    final Path normalizedTarget = target.toAbsolutePath().normalize();
    final Path root;
    if (Files.isDirectory(normalizedSource)) {
      root = normalizedSource;
    } else if (Files.isRegularFile(normalizedSource)
        && "manifest.json".equals(normalizedSource.getFileName().toString())) {
      root = normalizedSource.getParent();
    } else {
      throw new IOException(
          String.format(
              CliMessages
                  .EXCEPTION_LOGICAL_BACKUP_EXPORT_SOURCE_MUST_BE_A_DIRECTORY_OR_MANIFEST_ARG_0BBDE9F0,
              normalizedSource));
    }
    if (normalizedTarget.startsWith(root)) {
      throw new IOException(
          CliMessages.EXCEPTION_LOGICAL_BACKUP_EXPORT_TARGET_MUST_NOT_BE_INSIDE_SOURCE_A499F994);
    }
    if (Files.exists(normalizedTarget, LinkOption.NOFOLLOW_LINKS)) {
      throw new java.nio.file.FileAlreadyExistsException(normalizedTarget.toString());
    }
    final Path parent = normalizedTarget.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    final Path temporary = Files.createTempFile(parent, "pipe-logical-backup-", ".tmp");
    boolean published = false;
    try {
      try (final ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(temporary))) {
        for (final Path file : exportFiles(root, streams)) {
          final String entryName = root.relativize(file).toString().replace('\\', '/');
          zip.putNextEntry(new ZipEntry(entryName));
          Files.copy(file, zip);
          zip.closeEntry();
        }
      }
      try (final java.nio.channels.FileChannel archiveChannel =
          java.nio.channels.FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        archiveChannel.force(true);
      }
      try {
        Files.move(temporary, normalizedTarget, StandardCopyOption.ATOMIC_MOVE);
      } catch (final java.nio.file.AtomicMoveNotSupportedException e) {
        Files.move(temporary, normalizedTarget);
      }
      published = true;
    } finally {
      if (!published) {
        Files.deleteIfExists(temporary);
      }
    }
    System.out.println(
        String.format(
            CliMessages.LOG_LOGICAL_BACKUP_EXPORTED_FROM_ARG_TO_ARG_B3E8D280,
            normalizedSource,
            normalizedTarget));
    return 0;
  }

  private static void waitForSnapshotPipe(
      final Session session,
      final String pipeName,
      final long timeoutSeconds,
      final long pollSeconds)
      throws Exception {
    final long deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      final PipeStatus status = readPipeStatus(session, pipeName);
      if (status == null) {
        return;
      } else {
        System.out.println(
            String.format(
                CliMessages
                    .LOG_LOGICAL_BACKUP_EXPORT_ARG_STATE_ARG_REMAINING_ARG_ESTIMATED_SECONDS_ARG_6256B85B,
                pipeName,
                status.state,
                status.remainingEvents,
                status.estimatedSeconds));
        if (status.state != null
            && (status.state.equalsIgnoreCase("ERROR")
                || status.state.equalsIgnoreCase("STOPPED"))) {
          throw new IOException(
              String.format(
                  CliMessages.EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_FAILED_ARG_F52EB190,
                  pipeName,
                  status.exceptionMessage));
        }
      }
      Thread.sleep(pollSeconds * 1000L);
    }
    throw new IOException(
        String.format(
            CliMessages
                .EXCEPTION_LOGICAL_BACKUP_EXPORT_PIPE_ARG_TIMED_OUT_AFTER_ARG_SECONDS_76CF4B4B,
            pipeName,
            timeoutSeconds));
  }

  private static PipeStatus readPipeStatus(final Session session, final String pipeName)
      throws Exception {
    try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW PIPE " + pipeName)) {
      final SessionDataSet.DataIterator iterator = dataSet.iterator();
      if (!iterator.next()) {
        return null;
      }
      final PipeStatus status = new PipeStatus();
      status.state = value(iterator, dataSet, "State");
      status.remainingEvents = value(iterator, dataSet, "RemainingEventCount");
      status.estimatedSeconds = value(iterator, dataSet, "EstimatedRemainingSeconds");
      status.exceptionMessage = value(iterator, dataSet, "ExceptionMessage");
      return status;
    }
  }

  private static String value(
      final SessionDataSet.DataIterator iterator, final SessionDataSet dataSet, final String name)
      throws Exception {
    for (final String column : dataSet.getColumnNames()) {
      if (column.equalsIgnoreCase(name)) {
        return iterator.getString(column);
      }
    }
    return "Unknown";
  }

  private static long longOption(final CommandLine line, final String name, final long fallback)
      throws ParseException {
    try {
      return Long.parseLong(line.getOptionValue(name, Long.toString(fallback)));
    } catch (final NumberFormatException e) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_MUST_BE_AN_INTEGER_C9B397BE, name));
    }
  }

  private static void validateIdentifier(final String value, final String option)
      throws IOException {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_HAS_INVALID_IDENTIFIER_ARG_BB015204,
              option,
              value));
    }
  }

  private static String sqlString(final String value) {
    return value.replace("'", "''");
  }

  private static final class PipeStatus {
    private String state;
    private String remainingEvents;
    private String estimatedSeconds;
    private String exceptionMessage;
  }

  private static List<Path> exportFiles(final Path root, final List<BackupStream> streams)
      throws IOException {
    final Set<Path> files = new LinkedHashSet<>();
    for (final BackupStream stream : streams) {
      final Path manifestPath = stream.getManifestPath().toAbsolutePath().normalize();
      if (!manifestPath.startsWith(root)) {
        throw new IOException(
            String.format(
                CliMessages
                    .EXCEPTION_LOGICAL_BACKUP_EXPORT_SOURCE_MUST_BE_A_DIRECTORY_OR_MANIFEST_ARG_0BBDE9F0,
                root));
      }
      files.add(manifestPath);
      final Path streamDirectory = manifestPath.getParent();
      for (final LogicalBackupManifest.Segment segment : stream.getManifest().segments) {
        files.add(streamDirectory.resolve(segment.file).normalize());
      }
    }
    final List<Path> sortedFiles = new ArrayList<>(files);
    sortedFiles.sort(Comparator.comparing(Path::toString));
    return sortedFiles;
  }

  private static int importBackup(final CommandLine line) throws Exception {
    final List<BackupStream> streams = read(line);
    final boolean dryRun = line.hasOption(OPTION_DRY_RUN);
    if (dryRun) {
      System.out.println(CliMessages.LOG_DRY_RUN_COMPLETED_NO_DATA_WAS_WRITTEN_38AE244B);
      for (final BackupStream stream : streams) {
        System.out.println(
            String.format(
                CliMessages.LOG_STREAM_ARG_ARG_RECORDS_ARG_COMMITTED_EVENT_GROUPS_DE6BBAD0,
                stream.getManifest().streamId,
                stream.getRecords().size(),
                stream.getEventGroups().size()));
      }
      return 0;
    }
    final String host = required(line, OPTION_HOST);
    final int port = Integer.parseInt(required(line, OPTION_PORT));
    final String user = line.getOptionValue(OPTION_USER, "root");
    final String password = password(line);
    final Path checkpoint = checkpointPath(line);
    final ImportCheckpoint checkpointState = readCheckpoint(checkpoint, streams, host, port, user);
    try (final IoTDBSyncClient client =
        new IoTDBSyncClient(
            new ThriftClientProperty.Builder().setConnectionTimeoutMs(20_000).build(),
            host,
            port,
            false,
            "",
            "")) {
      handshake(client, streams.get(0).getManifest().timestampPrecision, user, password);
      if (containsConfigStream(streams)) {
        configNodeHandshake(
            client, streams.get(0).getManifest().timestampPrecision, user, password);
      }
      long importedGroups = 0;
      for (final BackupStream stream : streams) {
        importedGroups +=
            importStream(
                checkpoint,
                checkpointState,
                stream,
                request -> transfer(client, request),
                state -> writeCheckpoint(checkpoint, state));
      }
      writeCheckpoint(checkpoint, checkpointState);
      System.out.println(
          String.format(
              CliMessages
                  .LOG_LOGICAL_BACKUP_IMPORT_COMPLETED_ARG_EVENT_GROUPS_CHECKPOINT_ARG_16F6A72D,
              importedGroups,
              checkpoint));
    }
    return 0;
  }

  static boolean containsConfigStream(final List<BackupStream> streams) {
    return streams.stream()
        .anyMatch(stream -> "config".equalsIgnoreCase(stream.getManifest().streamType));
  }

  static long importStream(
      final Path checkpoint,
      final ImportCheckpoint checkpointState,
      final BackupStream stream,
      final RequestTransfer requestTransfer,
      final CheckpointPersister checkpointPersister)
      throws Exception {
    final String streamId = stream.getManifest().streamId;
    final long lastApplied = checkpointState.appliedSequences.getOrDefault(streamId, -1L);
    long importedGroups = 0;
    for (final EventGroup group : stream.getEventGroups()) {
      if (group.getLastSequence() <= lastApplied) {
        continue;
      }

      int nextRequestIndex = 0;
      if (checkpointState.inProgress == null) {
        checkpointState.inProgress = ImportProgress.start(streamId, group);
        checkpointPersister.persist(checkpointState);
      } else if (checkpointState.inProgress.matches(streamId, group)) {
        nextRequestIndex = checkpointState.inProgress.nextRequestIndex;
      } else {
        throw invalidCheckpoint(checkpoint);
      }

      final List<LogicalBackupRecord> requests = group.getRequests();
      if (nextRequestIndex < 0 || nextRequestIndex > requests.size()) {
        throw invalidCheckpoint(checkpoint);
      }
      for (int index = nextRequestIndex; index < requests.size(); index++) {
        requestTransfer.transfer(requests.get(index).toTPipeTransferReq());
        checkpointState.inProgress.nextRequestIndex = index + 1;
        checkpointPersister.persist(checkpointState);
      }

      checkpointState.appliedSequences.put(streamId, group.getLastSequence());
      checkpointState.inProgress = null;
      checkpointPersister.persist(checkpointState);
      importedGroups++;
    }
    return importedGroups;
  }

  private static List<BackupStream> read(final CommandLine line) throws IOException {
    final String source = input(line);
    try (final BackupInput input = BackupInput.open(Path.of(source))) {
      return new LogicalBackupArchiveReader()
          .read(input.getPath(), line.hasOption(OPTION_ALLOW_INCOMPLETE));
    }
  }

  private static String input(final CommandLine line) throws IOException {
    final String value = optionValue(line, OPTION_INPUT, OPTION_SOURCE);
    if (value == null || value.isBlank()) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA, OPTION_INPUT));
    }
    return value;
  }

  private static String optionValue(
      final CommandLine line, final String preferredOption, final String compatibilityOption) {
    return line.hasOption(preferredOption)
        ? line.getOptionValue(preferredOption)
        : line.getOptionValue(compatibilityOption);
  }

  private static String required(final CommandLine line, final String option) throws IOException {
    final String value = line.getOptionValue(option);
    if (value == null || value.isBlank()) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA, option));
    }
    return value;
  }

  private static String password(final CommandLine line) throws IOException {
    final boolean fromStandardInput = line.hasOption(OPTION_PASSWORD_STDIN);
    final boolean fromEnvironment = line.hasOption(OPTION_PASSWORD_ENV);
    if (fromStandardInput == fromEnvironment) {
      throw new IOException(
          CliMessages
              .EXCEPTION_SPECIFY_EXACTLY_ONE_OF_PASSWORD_STDIN_AND_PASSWORD_ENV_FOR_LOGICAL_BACKUP_IMPORT_A96813D9);
    }
    if (fromStandardInput) {
      final BufferedReader reader =
          new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      final String value = reader.readLine();
      if (value == null) {
        throw new IOException(
            CliMessages.EXCEPTION_NO_PASSWORD_WAS_READ_FROM_STANDARD_INPUT_6294AB8E);
      }
      return value;
    }
    if (fromEnvironment) {
      final String value = System.getenv(line.getOptionValue(OPTION_PASSWORD_ENV));
      if (value == null) {
        throw new IOException(
            CliMessages.EXCEPTION_LOGICAL_BACKUP_PASSWORD_ENVIRONMENT_VARIABLE_IS_NOT_SET_616738A2);
      }
      return value;
    }
    throw new IOException(
        CliMessages
            .EXCEPTION_SPECIFY_EXACTLY_ONE_OF_PASSWORD_STDIN_AND_PASSWORD_ENV_FOR_LOGICAL_BACKUP_IMPORT_A96813D9);
  }

  private static void handshake(
      final IoTDBSyncClient client,
      final String timestampPrecision,
      final String user,
      final String password)
      throws IOException, TException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, getClusterId());
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, timestampPrecision);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID, "-1");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, user);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME, "pipe-logical-backup");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, "sync");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE, "false");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST, "true");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF, "false");
    final TPipeTransferResp response =
        client.pipeTransfer(PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params));
    if (response == null
        || response.getStatus() == null
        || response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_HANDSHAKE_FAILED_ARG_7CDD4697,
              response == null ? "null" : response.getStatus()));
    }
  }

  private static void configNodeHandshake(
      final IoTDBSyncClient client,
      final String timestampPrecision,
      final String user,
      final String password)
      throws IOException, TException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, getClusterId());
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION, timestampPrecision);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID, "-1");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, user);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME, "pipe-logical-backup");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, "sync");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE, "false");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST, "true");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF, "false");
    final TPipeTransferResp response =
        client.pipeTransfer(
            PipeTransferHandshakeV2Req.toTPipeTransferReq(
                PipeRequestType.HANDSHAKE_CONFIGNODE_V2, params));
    if (response == null
        || response.getStatus() == null
        || response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_HANDSHAKE_FAILED_ARG_7CDD4697,
              response == null ? "null" : response.getStatus()));
    }
  }

  static void transfer(final IoTDBSyncClient client, final TPipeTransferReq request)
      throws TException, IOException {
    final TPipeTransferResp response = client.pipeTransfer(request);
    if (response == null
        || response.getStatus() == null
        || (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && response.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
            && response.getStatus().getCode()
                != TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode())) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_FAILED_ARG_75EE2D11,
              request.getType(),
              response == null ? "null" : response.getStatus()));
    }
  }

  private static Path checkpointPath(final CommandLine line) throws IOException {
    final String configuredCheckpoint = optionValue(line, OPTION_RESUME, OPTION_CHECKPOINT);
    if (configuredCheckpoint != null) {
      return Path.of(configuredCheckpoint);
    }
    final Path source = Path.of(input(line)).toAbsolutePath().normalize();
    return source.resolveSibling(source.getFileName() + ".import.checkpoint.json");
  }

  static ImportCheckpoint readCheckpoint(
      final Path checkpoint,
      final List<BackupStream> streams,
      final String host,
      final int port,
      final String user)
      throws IOException {
    final Map<String, String> sourceStreams = sourceStreamIdentities(streams);
    if (!Files.exists(checkpoint)) {
      final ImportCheckpoint state = new ImportCheckpoint();
      state.formatName = CHECKPOINT_FORMAT_NAME;
      state.formatVersion = CHECKPOINT_FORMAT_VERSION;
      state.sourceStreams = sourceStreams;
      state.targetHost = host;
      state.targetPort = port;
      state.targetUser = user;
      return state;
    }
    final ImportCheckpoint state;
    try {
      state =
          GSON.fromJson(
              Files.readString(checkpoint, StandardCharsets.UTF_8), ImportCheckpoint.class);
    } catch (final JsonParseException | NullPointerException e) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_IS_INVALID_ARG_71E82F4C, checkpoint),
          e);
    }
    if (state == null
        || !CHECKPOINT_FORMAT_NAME.equals(state.formatName)
        || !CHECKPOINT_FORMAT_VERSION.equals(state.formatVersion)
        || state.sourceStreams == null
        || state.appliedSequences == null) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_IS_INVALID_ARG_71E82F4C, checkpoint));
    }
    if (!sourceStreams.equals(state.sourceStreams)
        || !host.equals(state.targetHost)
        || port != state.targetPort
        || !user.equals(state.targetUser)
        || !checkpointStateIsValid(streams, state)) {
      throw new IOException(
          String.format(
              CliMessages
                  .EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_DOES_NOT_MATCH_THE_SOURCE_OR_TARGET_ARG_B978184D,
              checkpoint));
    }
    return state;
  }

  static Map<String, String> sourceStreamIdentities(final List<BackupStream> streams) {
    final Map<String, String> identities = new LinkedHashMap<>();
    for (final BackupStream stream : streams) {
      final LogicalBackupManifest manifest = stream.getManifest();
      final MessageDigest digest = LogicalBackupFormat.newSha256();
      updateDigest(digest, manifest.backupId);
      updateDigest(digest, manifest.streamId);
      updateDigest(digest, manifest.pipeName);
      updateDigest(digest, manifest.pipeCreationTime);
      updateDigest(digest, manifest.sourceClusterId);
      updateDigest(digest, manifest.regionId);
      updateDigest(digest, manifest.streamType);
      updateDigest(digest, manifest.segments.size());
      for (final LogicalBackupManifest.Segment segment : manifest.segments) {
        updateDigest(digest, segment.file);
        updateDigest(digest, segment.segmentId);
        updateDigest(digest, segment.firstSequence);
        updateDigest(digest, segment.lastSequence);
        updateDigest(digest, segment.recordCount);
        updateDigest(digest, segment.sizeBytes);
        updateDigest(digest, segment.sha256);
        updateDigest(digest, segment.status);
      }
      updateDigest(digest, stream.getRecords().size());
      for (final LogicalBackupRecord record : stream.getRecords()) {
        digest.update(record.getRecordType().getCode());
        digest.update(
            ByteBuffer.allocate(Long.BYTES * 4 + Integer.BYTES + Byte.BYTES + Short.BYTES)
                .putLong(record.getSequence())
                .putLong(record.getEventGroupId().getMostSignificantBits())
                .putLong(record.getEventGroupId().getLeastSignificantBits())
                .putInt(record.getOperationIndex())
                .putLong(record.getEventTime())
                .put(record.getRequestVersion())
                .putShort(record.getRequestType())
                .array());
        updateDigest(digest, record.getMetadata());
        updateDigest(digest, record.getPayload());
      }
      identities.put(manifest.streamId, LogicalBackupFormat.toHex(digest.digest()));
    }
    return identities;
  }

  private static void updateDigest(final MessageDigest digest, final String value) {
    if (value == null) {
      digest.update((byte) 0);
      return;
    }
    digest.update((byte) 1);
    updateDigest(digest, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void updateDigest(final MessageDigest digest, final byte[] value) {
    updateDigest(digest, value.length);
    digest.update(value);
  }

  private static void updateDigest(final MessageDigest digest, final int value) {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
  }

  private static void updateDigest(final MessageDigest digest, final long value) {
    digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
  }

  private static boolean checkpointStateIsValid(
      final List<BackupStream> streams, final ImportCheckpoint state) {
    final Map<String, Long> appliedSequences = state.appliedSequences;
    if (appliedSequences.size() > streams.size()) {
      return false;
    }
    boolean foundIncompleteStream = false;
    boolean matchedInProgress = false;
    for (final BackupStream stream : streams) {
      final String streamId = stream.getManifest().streamId;
      final Long applied = appliedSequences.getOrDefault(streamId, -1L);
      if (applied == null || applied < -1) {
        return false;
      }

      int appliedGroupIndex = -1;
      if (applied != -1) {
        for (int index = 0; index < stream.getEventGroups().size(); index++) {
          if (stream.getEventGroups().get(index).getLastSequence() == applied) {
            appliedGroupIndex = index;
            break;
          }
        }
        if (appliedGroupIndex < 0) {
          return false;
        }
      }

      final boolean streamComplete = appliedGroupIndex == stream.getEventGroups().size() - 1;
      if (foundIncompleteStream && appliedGroupIndex >= 0) {
        return false;
      }
      if (!streamComplete && !foundIncompleteStream) {
        foundIncompleteStream = true;
        if (state.inProgress != null) {
          final EventGroup nextGroup = stream.getEventGroups().get(appliedGroupIndex + 1);
          if (!state.inProgress.matches(streamId, nextGroup)
              || state.inProgress.nextRequestIndex < 0
              || state.inProgress.nextRequestIndex > nextGroup.getRequests().size()) {
            return false;
          }
          matchedInProgress = true;
        }
      }
    }
    return (state.inProgress == null || matchedInProgress)
        && appliedSequences.keySet().stream()
            .allMatch(
                streamId ->
                    streams.stream()
                        .anyMatch(stream -> stream.getManifest().streamId.equals(streamId)));
  }

  private static IOException invalidCheckpoint(final Path checkpoint) {
    return new IOException(
        String.format(
            CliMessages.EXCEPTION_LOGICAL_BACKUP_CHECKPOINT_IS_INVALID_ARG_71E82F4C, checkpoint));
  }

  static void writeCheckpoint(final Path checkpoint, final ImportCheckpoint checkpointState)
      throws IOException {
    final Path parent = checkpoint.toAbsolutePath().normalize().getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    final Path temporary = checkpoint.resolveSibling(checkpoint.getFileName() + ".tmp");
    Files.deleteIfExists(temporary);
    final ByteBuffer checkpointBytes = StandardCharsets.UTF_8.encode(GSON.toJson(checkpointState));
    try (final java.nio.channels.FileChannel checkpointChannel =
        java.nio.channels.FileChannel.open(
            temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      while (checkpointBytes.hasRemaining()) {
        checkpointChannel.write(checkpointBytes);
      }
      checkpointChannel.force(true);
    }
    try {
      Files.move(
          temporary,
          checkpoint,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (final java.nio.file.AtomicMoveNotSupportedException e) {
      Files.move(temporary, checkpoint, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static String getClusterId() {
    final byte[] bytes = new byte[16];
    new SecureRandom().nextBytes(bytes);
    return "PIPE-LOGICAL-BACKUP-" + UUID.nameUUIDFromBytes(bytes);
  }

  private static void printUsage() {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        new PrintWriter(System.out),
        120,
        CliMessages.LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_6D62F9CE,
        CliMessages
            .LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_EXPORT_CAN_USE_BACKUP_DIR_TO_CREATE_MONITOR_AND_CLEAN_UP_A_SNAPSHOT_PIPE_AUTOMATICALLY_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_F5B223CB,
        optionsForHelp(),
        2,
        2,
        "",
        true);
  }

  private static Options optionsForHelp() {
    final Options options = new Options();
    addOption(options, OPTION_INPUT, true, false);
    addOption(options, OPTION_OUTPUT, true, false);
    addOption(options, OPTION_RESUME, true, false);
    addOption(options, OPTION_FORMAT, true, false);
    addOption(options, OPTION_DEEP, false, false);
    addOption(options, OPTION_HOST, true, false);
    addOption(options, OPTION_PORT, true, false);
    addOption(options, OPTION_USER, true, false);
    addOption(options, OPTION_PASSWORD_STDIN, false, false);
    addOption(options, OPTION_PASSWORD_ENV, true, false);
    addOption(options, OPTION_CHECKPOINT, true, false);
    addOption(options, OPTION_DRY_RUN, false, false);
    addOption(options, OPTION_ALLOW_INCOMPLETE, false, false);
    addOption(options, OPTION_JSON, false, false);
    addOption(options, OPTION_BACKUP_DIR, true, false);
    addOption(options, OPTION_PIPE_NAME, true, false);
    addOption(options, OPTION_BACKUP_ID, true, false);
    addOption(options, OPTION_TIMEOUT_SECONDS, true, false);
    addOption(options, OPTION_POLL_INTERVAL_SECONDS, true, false);
    return options;
  }

  static class ImportCheckpoint {
    String formatName;
    String formatVersion;
    Map<String, String> sourceStreams = new LinkedHashMap<>();
    String targetHost;
    int targetPort;
    String targetUser;
    Map<String, Long> appliedSequences = new LinkedHashMap<>();
    ImportProgress inProgress;
  }

  static class ImportProgress {
    String streamId;
    String eventGroupId;
    long lastSequence;
    int nextRequestIndex;

    static ImportProgress start(final String streamId, final EventGroup group) {
      final ImportProgress progress = new ImportProgress();
      progress.streamId = streamId;
      progress.eventGroupId = group.getEventGroupId().toString();
      progress.lastSequence = group.getLastSequence();
      return progress;
    }

    boolean matches(final String candidateStreamId, final EventGroup group) {
      return candidateStreamId.equals(streamId)
          && group.getEventGroupId().toString().equals(eventGroupId)
          && group.getLastSequence() == lastSequence;
    }
  }

  @FunctionalInterface
  interface RequestTransfer {
    void transfer(TPipeTransferReq request) throws Exception;
  }

  @FunctionalInterface
  interface CheckpointPersister {
    void persist(ImportCheckpoint state) throws IOException;
  }

  private static class BackupInput implements AutoCloseable {
    private final Path path;
    private final Path temporaryDirectory;

    private BackupInput(final Path path, final Path temporaryDirectory) {
      this.path = path;
      this.temporaryDirectory = temporaryDirectory;
    }

    private static BackupInput open(final Path source) throws IOException {
      final Path normalizedSource = source.toAbsolutePath().normalize();
      if (!Files.isRegularFile(normalizedSource)
          || "manifest.json".equals(normalizedSource.getFileName().toString())) {
        return new BackupInput(normalizedSource, null);
      }
      final Path temporaryDirectory = Files.createTempDirectory("iotdb-logical-backup-");
      try {
        extract(normalizedSource, temporaryDirectory);
        return new BackupInput(temporaryDirectory, temporaryDirectory);
      } catch (final IOException | RuntimeException e) {
        deleteRecursively(temporaryDirectory);
        throw e;
      }
    }

    private static void extract(final Path archive, final Path target) throws IOException {
      int entryCount = 0;
      long totalBytes = 0;
      final byte[] buffer = new byte[64 * 1024];
      try (final ZipInputStream zip = new ZipInputStream(Files.newInputStream(archive))) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          if (++entryCount > MAX_ARCHIVE_ENTRIES) {
            throw new IOException(
                CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432);
          }
          final Path output = target.resolve(entry.getName()).normalize();
          if (!output.startsWith(target)) {
            throw new IOException(
                String.format(
                    CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_ENTRY_IS_UNSAFE_ARG_3E548152,
                    entry.getName()));
          }
          if (entry.isDirectory()) {
            Files.createDirectories(output);
          } else {
            final Path parent = output.getParent();
            if (parent != null) {
              Files.createDirectories(parent);
            }
            try (final OutputStream stream =
                Files.newOutputStream(
                    output, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
              int length;
              while ((length = zip.read(buffer)) >= 0) {
                if (length == 0) {
                  continue;
                }
                totalBytes += length;
                if (totalBytes > MAX_ARCHIVE_BYTES) {
                  throw new IOException(
                      CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432);
                }
                stream.write(buffer, 0, length);
              }
            }
          }
          zip.closeEntry();
        }
      }
    }

    private Path getPath() {
      return path;
    }

    @Override
    public void close() throws IOException {
      if (temporaryDirectory != null) {
        deleteRecursively(temporaryDirectory);
      }
    }

    private static void deleteRecursively(final Path directory) throws IOException {
      if (!Files.exists(directory)) {
        return;
      }
      try (final java.util.stream.Stream<Path> paths = Files.walk(directory)) {
        for (final Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }
}
