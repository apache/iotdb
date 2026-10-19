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
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupCatalog;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupFormat;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecord;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupSettings;
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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/** Command line inspector, verifier, archiver and importer for Pipe logical backups. */
public final class PipeLogicalBackupTool {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final String COMMAND_DUMP = "dump";
  private static final String COMMAND_PACK = "pack";
  private static final String COMMAND_RESTORE = "restore";
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

  public static int run(final String[] args) throws Exception {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      printUsage();
      return 0;
    }
    final String command = args[0].toLowerCase(java.util.Locale.ROOT);
    if (!List.of(
            COMMAND_DUMP,
            COMMAND_PACK,
            COMMAND_RESTORE,
            COMMAND_INSPECT,
            COMMAND_VERIFY,
            COMMAND_EXPORT,
            COMMAND_IMPORT)
        .contains(command)) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_UNKNOWN_LOGICAL_BACKUP_COMMAND_ARG_79275619, command));
    }
    if (java.util.Arrays.asList(args).contains("--help")
        || java.util.Arrays.asList(args).contains("-h")) {
      printUsage();
      return 0;
    }
    final CommandLine line = parse(command, java.util.Arrays.copyOfRange(args, 1, args.length));
    switch (command) {
      case COMMAND_INSPECT:
        return inspect(line);
      case COMMAND_VERIFY:
        return verify(line);
      case COMMAND_DUMP:
        final Path dumped = dump(line, false);
        printDumpResult(line, dumped);
        return 0;
      case COMMAND_PACK:
        return pack(line);
      case COMMAND_RESTORE:
        return importBackup(line);
      case COMMAND_EXPORT:
        System.err.println(
            CliMessages
                .LOG_EXPORT_IS_DEPRECATED_USE_DUMP_TO_GENERATE_A_BACKUP_AND_PACK_TO_CREATE_A_ZIP_47709D2D);
        if (!line.hasOption(OPTION_INPUT) && !line.hasOption(OPTION_SOURCE)) {
          final Path source = dump(line, true);
          final List<BackupStream> streams =
              new LogicalBackupArchiveReader()
                  .read(source, line.hasOption(OPTION_ALLOW_INCOMPLETE));
          return exportArchive(
              source,
              Path.of(optionValue(line, OPTION_OUTPUT, OPTION_TARGET)),
              streams,
              line.hasOption(OPTION_JSON));
        }
        return pack(line);
      case COMMAND_IMPORT:
        System.err.println(CliMessages.LOG_IMPORT_IS_DEPRECATED_USE_RESTORE_2F1AE2CE);
        return importBackup(line);
      default:
        throw new AssertionError(command);
    }
  }

  static CommandLine parse(final String command, final String[] args) throws ParseException {
    final Options options = new Options();
    final Set<String> names = new LinkedHashSet<>(List.of(OPTION_JSON, OPTION_ALLOW_INCOMPLETE));
    final boolean legacyExport = COMMAND_EXPORT.equals(command);
    final boolean legacyImport = COMMAND_IMPORT.equals(command);
    final boolean dump = COMMAND_DUMP.equals(command) || legacyExport;
    final boolean restore = COMMAND_RESTORE.equals(command) || legacyImport;
    if (!COMMAND_DUMP.equals(command)) {
      names.add(OPTION_INPUT);
    }
    if (COMMAND_PACK.equals(command) || legacyExport) {
      names.add(OPTION_OUTPUT);
    }
    if (dump || restore) {
      names.addAll(
          List.of(
              OPTION_HOST,
              OPTION_PORT,
              OPTION_USER,
              OPTION_PASSWORD_STDIN,
              OPTION_PASSWORD_ENV,
              OPTION_RESUME));
    }
    if (restore) {
      names.addAll(List.of(OPTION_CHECKPOINT, OPTION_DRY_RUN, "conflict", "include-auth"));
    }
    if (dump) {
      names.addAll(
          List.of(
              OPTION_BACKUP_DIR,
              OPTION_BACKUP_ID,
              OPTION_PIPE_NAME,
              OPTION_TIMEOUT_SECONDS,
              OPTION_POLL_INTERVAL_SECONDS,
              "scope",
              "model",
              "include",
              "exclude",
              "start-time",
              "end-time",
              "include-auth",
              "mode",
              "until",
              "stop-sequence",
              "segment-size-bytes",
              "max-record-bytes",
              "fsync-policy",
              "fsync-batch-operations",
              "fsync-period-ms",
              "on-unsupported",
              "include-heartbeat"));
    }
    if (legacyExport || legacyImport) {
      names.add(OPTION_SOURCE);
      if (legacyExport) {
        names.addAll(List.of(OPTION_TARGET, OPTION_FORMAT));
      }
    }
    if (COMMAND_VERIFY.equals(command)) {
      names.add(OPTION_DEEP); // Accepted for compatibility; verification is always deep.
    }
    final Set<String> flags =
        Set.of(
            OPTION_JSON,
            OPTION_ALLOW_INCOMPLETE,
            OPTION_DRY_RUN,
            OPTION_PASSWORD_STDIN,
            OPTION_DEEP);
    for (final String name : names) {
      if (OPTION_RESUME.equals(name) && restore) {
        options.addOption(
            Option.builder().longOpt(name).hasArg(legacyImport).optionalArg(legacyImport).build());
      } else {
        addOption(options, name, !flags.contains(name), false);
      }
    }
    final CommandLine line = new DefaultParser().parse(options, args);
    if (!line.getArgList().isEmpty()
        || (line.hasOption(OPTION_INPUT) && line.hasOption(OPTION_SOURCE))
        || (line.hasOption(OPTION_OUTPUT) && line.hasOption(OPTION_TARGET))) {
      throw new ParseException(
          CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
    }
    if (!COMMAND_DUMP.equals(command)
        && !(legacyExport && line.hasOption(OPTION_BACKUP_DIR))
        && optionValue(line, OPTION_INPUT, OPTION_SOURCE) == null) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA, OPTION_INPUT));
    }
    if ((COMMAND_PACK.equals(command) || legacyExport)
        && optionValue(line, OPTION_OUTPUT, OPTION_TARGET) == null) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_OPTION_ARG_IS_REQUIRED_1E7449AA, OPTION_OUTPUT));
    }
    if (legacyExport && !"binary".equals(line.getOptionValue(OPTION_FORMAT, "binary"))) {
      throw new ParseException(
          String.format(
              CliMessages.EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EXPORT_FORMAT_ARG_A6D7DEB1,
              line.getOptionValue(OPTION_FORMAT)));
    }
    if (line.hasOption(OPTION_PORT)) {
      final long port = longOption(line, OPTION_PORT, 6667);
      if (port <= 0 || port > 65535) {
        throw new ParseException(
            CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
      }
    }
    if (restore && !"fail".equals(line.getOptionValue("conflict", "fail"))) {
      throw new ParseException(
          CliMessages
              .EXCEPTION_ONLY_CONFLICT_FAIL_IS_SUPPORTED_RESTORE_NEVER_IMPLICITLY_OVERWRITES_OR_DELETES_TARGET_OBJECTS_70C9523E);
    }
    // Offline export is exactly pack, including its refusal of source connection options.
    if (legacyExport && optionValue(line, OPTION_INPUT, OPTION_SOURCE) != null) {
      final Set<String> packOptions =
          Set.of(
              OPTION_INPUT,
              OPTION_SOURCE,
              OPTION_OUTPUT,
              OPTION_TARGET,
              OPTION_FORMAT,
              OPTION_JSON,
              OPTION_ALLOW_INCOMPLETE);
      for (final Option option : line.getOptions()) {
        if (!packOptions.contains(option.getLongOpt())) {
          throw new ParseException(
              CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
        }
      }
    }
    return line;
  }

  private static void addOption(
      final Options options, final String name, final boolean hasArg, final boolean required) {
    options.addOption(Option.builder().longOpt(name).hasArg(hasArg).required(required).build());
  }

  private static Map<String, Object> summary(final List<BackupStream> streams) {
    final Map<String, Object> result = new LinkedHashMap<>();
    if (!streams.isEmpty() && streams.get(0).getCatalog() != null) {
      result.put("manifest", streams.get(0).getCatalog());
    }
    final List<Map<String, Object>> details = new ArrayList<>();
    for (final BackupStream stream : streams) {
      final Map<String, Object> item = new LinkedHashMap<>();
      item.put("manifest", stream.getManifest());
      item.put("recordCount", stream.getRecords().size());
      item.put("eventGroupCount", stream.getEventGroups().size());
      details.add(item);
    }
    result.put("streams", details);
    return result;
  }

  private static int inspect(final CommandLine line) throws IOException {
    final List<BackupStream> streams = read(line);
    printSummary(line, summary(streams));
    return 0;
  }

  private static int verify(final CommandLine line) throws IOException {
    final List<BackupStream> streams = read(line);
    final Map<String, Object> result = summary(streams);
    result.put("structural", "passed");
    result.put("checksum", "passed");
    result.put("semantic", "passed");
    printSummary(line, result);
    return 0;
  }

  private static void printSummary(final CommandLine line, final Map<String, Object> result) {
    if (line.hasOption(OPTION_JSON)) {
      System.out.println(GSON.toJson(result));
    } else {
      System.out.println(
          String.format(CliMessages.LOG_LOGICAL_BACKUP_ARG_1E5C23CE, GSON.toJson(result)));
    }
  }

  private static int pack(final CommandLine line) throws IOException {
    final Path source = Path.of(input(line)).toAbsolutePath().normalize();
    final Path target =
        Path.of(optionValue(line, OPTION_OUTPUT, OPTION_TARGET)).toAbsolutePath().normalize();
    final List<BackupStream> streams = read(line);
    return exportArchive(source, target, streams, line.hasOption(OPTION_JSON));
  }

  private static Path dump(final CommandLine line, final boolean legacy) throws Exception {
    final LogicalBackupDumpOptions options = new LogicalBackupDumpOptions(line, legacy);
    final String host = required(line, OPTION_HOST);
    final int port = Integer.parseInt(required(line, OPTION_PORT));
    final String user = line.getOptionValue(OPTION_USER, "root");
    final Path backupRoot = Path.of(required(line, OPTION_BACKUP_DIR)).toAbsolutePath().normalize();
    String backupId =
        legacy
            ? line.getOptionValue(
                OPTION_BACKUP_ID, "backup_" + UUID.randomUUID().toString().replace('-', '_'))
            : required(line, OPTION_BACKUP_ID);
    LogicalBackupSettings.validateBackupId(backupId);
    if ("new".equals(options.resume)
        && Files.exists(backupRoot.resolve(backupId), LinkOption.NOFOLLOW_LINKS)) {
      backupId += "-new-" + UUID.randomUUID().toString().substring(0, 8);
      LogicalBackupSettings.validateBackupId(backupId);
    }
    final Path directory = backupRoot.resolve(backupId);
    LogicalBackupCatalog.rejectSymbolicLinks(directory);
    final boolean append = "append".equals(options.resume);
    final boolean existed = Files.exists(directory, LinkOption.NOFOLLOW_LINKS);
    if (existed && !append) {
      throw new java.nio.file.FileAlreadyExistsException(directory.toString());
    }
    final String password = password(line);
    final long timeoutSeconds = longOption(line, OPTION_TIMEOUT_SECONDS, 3600L);
    final long pollIntervalSeconds = longOption(line, OPTION_POLL_INTERVAL_SECONDS, 2L);
    if (timeoutSeconds <= 0
        || timeoutSeconds > 31_536_000L
        || pollIntervalSeconds <= 0
        || pollIntervalSeconds > 3600L) {
      throw new IOException(
          CliMessages
              .EXCEPTION_LOGICAL_BACKUP_EXPORT_TIMEOUT_AND_POLL_INTERVAL_MUST_BE_POSITIVE_38622769);
    }
    LogicalBackupCatalog catalog = options.catalog;
    catalog.backupId = backupId;
    catalog.pipeName =
        line.getOptionValue(
            OPTION_PIPE_NAME, "logical_backup_" + UUID.randomUUID().toString().replace('-', '_'));
    validateIdentifier(catalog.pipeName, OPTION_PIPE_NAME);
    if (existed) {
      final LogicalBackupCatalog previous =
          LogicalBackupCatalog.load(directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME));
      if (previous == null
          || !catalog.backupId.equals(previous.backupId)
          || !catalog.captureOptions.equals(previous.captureOptions)
          || (line.hasOption(OPTION_PIPE_NAME) && !catalog.pipeName.equals(previous.pipeName))) {
        throw new IOException(
            CliMessages
                .EXCEPTION_THE_EXISTING_BACKUP_TASK_DOES_NOT_MATCH_THIS_BACKUP_IDENTITY_OR_CAPTURE_OPTIONS_50E6388F);
      }
      catalog = previous;
      if ("SEALED".equals(catalog.status) || "INCOMPLETE".equals(catalog.status)) {
        new LogicalBackupArchiveReader().read(directory, line.hasOption(OPTION_ALLOW_INCOMPLETE));
        return directory;
      }
    } else {
      Files.createDirectories(backupRoot);
      Files.createDirectory(directory);
    }
    // Keep the lock inode: deleting it after unlock can let two later callers lock different files.
    final Path lock = directory.resolve(".backup.lock");
    try (final java.nio.channels.FileChannel channel =
            java.nio.channels.FileChannel.open(
                lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        final java.nio.channels.FileLock ignored = channel.tryLock()) {
      if (ignored == null) {
        throw new IOException(
            CliMessages.EXCEPTION_ANOTHER_DUMP_PROCESS_IS_MANAGING_THIS_BACKUP_FE8EE828);
      }
      catalog.write(directory);
      final String pipeName = catalog.pipeName;
      try (final Session session =
          new Session.Builder().host(host).port(port).username(user).password(password).build()) {
        session.open();
        final PipeStatus existing = readPipeStatus(session, pipeName);
        if (existing != null && !append) {
          throw new IOException(
              CliMessages
                  .EXCEPTION_THE_EXISTING_BACKUP_TASK_DOES_NOT_MATCH_THIS_BACKUP_IDENTITY_OR_CAPTURE_OPTIONS_50E6388F);
        }
        if (existing == null && (!existed || streamManifests(directory).isEmpty())) {
          session.executeNonQueryStatement(
              options.createSql(pipeName, backupRoot.toString(), backupId));
          session.executeNonQueryStatement("START PIPE " + pipeName);
        }
        waitForPipe(
            session,
            pipeName,
            timeoutSeconds,
            pollIntervalSeconds,
            options.until,
            line.hasOption(OPTION_JSON));
        final List<BackupStream> streams =
            awaitSealedStreams(directory, Math.min(timeoutSeconds, 120L), pollIntervalSeconds);
        catalog.seal(directory, streams);
      } catch (final Exception e) {
        throw new IOException(
            String.format(
                CliMessages
                    .EXCEPTION_BACKUP_ARG_TASK_ARG_DIRECTORY_ARG_ARG_THE_TASK_IS_RETAINED_QUERY_SHOW_PIPE_OR_REPEAT_DUMP_WITH_THE_SAME_OPTIONS_AND_RESUME_APPEND_6F8C1E87,
                backupId,
                pipeName,
                directory,
                e.getMessage()),
            e);
      }
    }
    new LogicalBackupArchiveReader().read(directory, line.hasOption(OPTION_ALLOW_INCOMPLETE));
    return directory;
  }

  private static List<Path> streamManifests(final Path root) throws IOException {
    try (final java.util.stream.Stream<Path> paths = Files.walk(root)) {
      return paths
          .filter(path -> !path.equals(root.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME)))
          .filter(
              path -> LogicalBackupFormat.MANIFEST_FILE_NAME.equals(path.getFileName().toString()))
          .sorted()
          .toList();
    }
  }

  private static List<BackupStream> readGeneratedStreams(final Path root) throws Exception {
    final List<BackupStream> streams = new ArrayList<>();
    for (final Path manifest : streamManifests(root)) {
      streams.addAll(new LogicalBackupArchiveReader().read(manifest, true));
    }
    if (streams.isEmpty()
        || streams.stream()
            .anyMatch(
                stream ->
                    stream.getManifest().segments.stream()
                        .anyMatch(segment -> !"SEALED".equals(segment.status)))) {
      throw new IOException(
          CliMessages
              .EXCEPTION_THE_BACKUP_TASK_HAS_NOT_PRODUCED_SEALED_STREAMS_YET_RESUME_THE_SAME_TASK_TO_FINISH_SEALING_87C9B599);
    }
    return streams;
  }

  private static List<BackupStream> awaitSealedStreams(
      final Path root, final long timeoutSeconds, final long pollSeconds) throws Exception {
    final long deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L;
    Exception lastFailure;
    do {
      try {
        return readGeneratedStreams(root);
      } catch (final IOException e) {
        lastFailure = e;
      }
      Thread.sleep(Math.min(pollSeconds * 1000L, 1000L));
    } while (System.nanoTime() < deadline);
    throw lastFailure;
  }

  private static void printDumpResult(final CommandLine line, final Path directory)
      throws IOException {
    final LogicalBackupCatalog catalog =
        LogicalBackupCatalog.load(directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME));
    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("backupId", catalog.backupId);
    result.put("manifest", directory.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME).toString());
    result.put("status", catalog.status);
    result.put("sourceHighWatermark", catalog.sourceHighWatermark);
    printSummary(line, result);
  }

  private static int exportArchive(
      final Path source, final Path target, final List<BackupStream> streams, final boolean json)
      throws IOException {
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
    LogicalBackupCatalog.rejectSymbolicLinks(root);
    LogicalBackupCatalog.rejectSymbolicLinks(normalizedTarget);
    if (normalizedTarget.startsWith(root.toRealPath())) {
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
    final boolean incomplete =
        streams.stream()
            .anyMatch(
                stream ->
                    stream.getManifest().skippedEventCount > 0
                        || "INCOMPLETE".equals(stream.getManifest().status)
                        || (stream.getCatalog() != null
                            && "INCOMPLETE".equals(stream.getCatalog().status)));
    boolean published = false;
    try {
      final boolean legacy =
          LogicalBackupCatalog.load(root.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME)) == null;
      final boolean relocateLegacyStream =
          legacy
              && streams.stream()
                  .anyMatch(stream -> stream.getManifestPath().getParent().equals(root));
      try (final ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(temporary))) {
        if (legacy) {
          final LogicalBackupManifest first = streams.get(0).getManifest();
          final LogicalBackupCatalog catalog = new LogicalBackupCatalog();
          catalog.backupId = first.backupId;
          catalog.pipeName = first.pipeName;
          catalog.scope =
              streams.stream().map(stream -> stream.getManifest().streamType).distinct().toList();
          catalog.model = List.of("tree", "table");
          catalog.includeAuth = containsConfigStream(streams);
          catalog.captureOptions.putAll(first.captureOptions);
          catalog.collect(root, streams);
          if (relocateLegacyStream) {
            for (final LogicalBackupCatalog.StreamEntry entry : catalog.streams) {
              entry.manifest.file = "legacy-stream/" + entry.manifest.file;
              for (final LogicalBackupCatalog.FileEntry segment : entry.segments) {
                segment.file = "legacy-stream/" + segment.file;
              }
            }
          }
          zip.putNextEntry(new ZipEntry(LogicalBackupFormat.MANIFEST_FILE_NAME));
          zip.write(GSON.toJson(catalog).getBytes(StandardCharsets.UTF_8));
          zip.closeEntry();
        }
        for (final Path file : exportFiles(root, streams)) {
          final String entryName =
              (relocateLegacyStream ? "legacy-stream/" : "")
                  + root.relativize(file).toString().replace('\\', '/');
          zip.putNextEntry(new ZipEntry(entryName));
          Files.copy(file, zip);
          zip.closeEntry();
        }
      }
      try (final BackupInput packed = BackupInput.open(temporary)) {
        new LogicalBackupArchiveReader().read(packed.getPath(), incomplete);
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
    if (json) {
      System.out.println(
          GSON.toJson(
              Map.of(
                  "input",
                  normalizedSource.toString(),
                  "output",
                  normalizedTarget.toString(),
                  "status",
                  incomplete ? "INCOMPLETE" : "SEALED")));
    } else {
      System.out.println(
          String.format(
              CliMessages.LOG_LOGICAL_BACKUP_PACKED_FROM_ARG_TO_ARG_CB9D5304,
              normalizedSource,
              normalizedTarget));
    }
    return 0;
  }

  private static void waitForPipe(
      final Session session,
      final String pipeName,
      final long timeoutSeconds,
      final long pollSeconds,
      final long until,
      final boolean json)
      throws Exception {
    final long deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L;
    while (System.nanoTime() < deadline) {
      final PipeStatus status = readPipeStatus(session, pipeName);
      if (status == null) {
        return;
      } else {
        (json ? System.err : System.out)
            .println(
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
    final Path rootManifest = root.resolve(LogicalBackupFormat.MANIFEST_FILE_NAME);
    if (LogicalBackupCatalog.load(rootManifest) != null) {
      files.add(rootManifest);
    }
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
    final Map<String, Object> report = summary(streams);
    final boolean authorizationObjects =
        streams.get(0).getCatalog() == null
            ? containsConfigStream(streams)
            : streams.get(0).getCatalog().includeAuth;
    final String includeAuth = line.getOptionValue("include-auth", "false");
    if (!List.of("true", "false").contains(includeAuth)) {
      throw new IOException(
          String.format(
              CliMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_DUMP_OPTION_ARG_894E11E0,
              "include-auth"));
    }
    report.put("authorizationObjects", authorizationObjects);
    report.put("authorizationRestoreSelected", Boolean.parseBoolean(includeAuth));
    if (authorizationObjects) {
      report.put(
          "authorizationSensitiveFields",
          List.of("password-hashes", "role-memberships", "privileges"));
    }
    report.put("conflict", "fail");
    report.put(
        "requestCount",
        streams.stream()
            .flatMap(stream -> stream.getEventGroups().stream())
            .mapToLong(group -> group.getRequests().size())
            .sum());
    report.put("dryRun", line.hasOption(OPTION_DRY_RUN));
    final boolean targetProvided =
        line.hasOption(OPTION_HOST)
            || line.hasOption(OPTION_PORT)
            || line.hasOption(OPTION_USER)
            || line.hasOption(OPTION_PASSWORD_STDIN)
            || line.hasOption(OPTION_PASSWORD_ENV);
    if (!targetProvided && line.hasOption(OPTION_DRY_RUN)) {
      report.put("targetPreflight", "not-requested");
      printSummary(line, report);
      return 0;
    }
    if (authorizationObjects
        && !Boolean.parseBoolean(includeAuth)
        && !line.hasOption(OPTION_DRY_RUN)) {
      throw new IOException(
          CliMessages
              .EXCEPTION_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_INSPECT_THE_BACKUP_OR_USE_DRY_RUN_FIRST_83DDCD79);
    }
    final String host = required(line, OPTION_HOST);
    final int port = Integer.parseInt(required(line, OPTION_PORT));
    final String user = line.getOptionValue(OPTION_USER, "root");
    final String password = password(line);
    report.put("target", Map.of("host", host, "port", port, "user", user));
    final Path checkpoint = checkpointPath(line);
    final boolean resume = line.hasOption(OPTION_RESUME);
    if (!line.hasOption(OPTION_DRY_RUN)
        && Files.exists(checkpoint, LinkOption.NOFOLLOW_LINKS) != resume) {
      throw new IOException(
          CliMessages
              .EXCEPTION_AN_EXISTING_CHECKPOINT_REQUIRES_RESUME_RESUME_REQUIRES_AN_EXISTING_CHECKPOINT_FB6D0B7D);
    }
    final ImportCheckpoint state =
        line.hasOption(OPTION_DRY_RUN)
            ? null
            : readCheckpoint(checkpoint, streams, host, port, user);
    if (state != null) {
      final String sourceDigest = sourceDigest(Path.of(input(line)), streams);
      if (state.sourceSha256 != null && !state.sourceSha256.equals(sourceDigest)) {
        throw invalidCheckpoint(checkpoint);
      }
      state.sourceSha256 = sourceDigest;
    }
    try (final Session session =
        new Session.Builder().host(host).port(port).username(user).password(password).build()) {
      session.open();
      final String precision = session.getTimestampPrecision();
      if (!streams.get(0).getManifest().timestampPrecision.equals(precision)) {
        throw new IOException(
            CliMessages
                .EXCEPTION_THE_TARGET_TIMESTAMP_PRECISION_DOES_NOT_MATCH_THE_BACKUP_70B225CB);
      }
      String version = "unknown";
      try (final SessionDataSet dataSet = session.executeQueryStatement("SHOW VERSION")) {
        final SessionDataSet.DataIterator iterator = dataSet.iterator();
        if (iterator.next()) {
          version = iterator.getString(dataSet.getColumnNames().get(0));
        }
      }
      final LogicalBackupCatalog catalog = streams.get(0).getCatalog();
      final String sourceVersion = streams.get(0).getManifest().sourceVersion;
      if ((sourceVersion != null && !version.split("\\.")[0].equals(sourceVersion.split("\\.")[0]))
          || (catalog != null && catalog.model.contains("table") && version.startsWith("1."))) {
        throw new IOException(
            CliMessages
                .EXCEPTION_THE_TARGET_VERSION_OR_DATA_MODEL_IS_INCOMPATIBLE_WITH_THE_BACKUP_220EB40C);
      }
      report.put("targetVersion", version);
      report.put("timestampPrecision", precision);
    }
    try (final IoTDBSyncClient client =
        new IoTDBSyncClient(
            new ThriftClientProperty.Builder().setConnectionTimeoutMs(20_000).build(),
            host,
            port,
            false,
            "",
            "")) {
      preflight(client, streams, user, password);
      report.put("targetPreflight", "passed");
      report.put("conflictCheck", "on-replay");
      if (line.hasOption(OPTION_DRY_RUN)) {
        printSummary(line, report);
        return 0;
      }
      (line.hasOption(OPTION_JSON) ? System.err : System.out).println(GSON.toJson(report));
      final java.util.concurrent.atomic.AtomicBoolean uncertainRequest =
          new java.util.concurrent.atomic.AtomicBoolean(
              state.inProgress != null
                  && state.inProgress.uncertain
                  && streams.stream()
                      .flatMap(stream -> stream.getEventGroups().stream())
                      .anyMatch(
                          group ->
                              group
                                      .getEventGroupId()
                                      .toString()
                                      .equals(state.inProgress.eventGroupId)
                                  && state.inProgress.nextRequestIndex
                                      < group.getRequests().size()));
      try {
        state.status = "RUNNING";
        state.lastError = null;
        long importedGroups = 0;
        for (final BackupStream stream : streams) {
          importedGroups +=
              importStream(
                  checkpoint,
                  state,
                  stream,
                  request -> transfer(client, request, uncertainRequest.getAndSet(false)),
                  checkpointState -> writeCheckpoint(checkpoint, checkpointState));
        }
        state.status = "COMPLETED";
        writeCheckpoint(checkpoint, state);
        report.put("status", state.status);
        report.put("importedEventGroups", importedGroups);
        report.put("checkpoint", checkpoint.toString());
        printSummary(line, report);
      } catch (final Exception e) {
        state.status = "FAILED";
        state.lastError = e.getClass().getSimpleName();
        try {
          writeCheckpoint(checkpoint, state);
        } catch (final IOException checkpointFailure) {
          e.addSuppressed(checkpointFailure);
        }
        throw e;
      }
    }
    return 0;
  }

  static void preflight(
      final IoTDBSyncClient client,
      final List<BackupStream> streams,
      final String user,
      final String password)
      throws IOException, TException {
    handshake(client, streams.get(0).getManifest().timestampPrecision, user, password);
    if (containsConfigStream(streams)) {
      configNodeHandshake(client, streams.get(0).getManifest().timestampPrecision, user, password);
    }
  }

  private static String sourceDigest(final Path input, final List<BackupStream> streams)
      throws IOException {
    if (Files.isRegularFile(input)
        && !LogicalBackupFormat.MANIFEST_FILE_NAME.equals(input.getFileName().toString())) {
      return LogicalBackupCatalog.sha256(input);
    }
    final MessageDigest digest = LogicalBackupFormat.newSha256();
    updateDigest(digest, GSON.toJson(sourceStreamIdentities(streams)));
    if (!streams.isEmpty() && streams.get(0).getCatalog() != null) {
      updateDigest(digest, GSON.toJson(streams.get(0).getCatalog()));
    }
    return LogicalBackupFormat.toHex(digest.digest());
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
        checkpointState.inProgress.uncertain = true;
        checkpointPersister.persist(checkpointState);
        try {
          requestTransfer.transfer(requests.get(index).toTPipeTransferReq());
        } catch (final RestoreRequestRejectedException e) {
          checkpointState.inProgress.uncertain = false;
          throw e;
        }
        checkpointState.inProgress.nextRequestIndex = index + 1;
        checkpointState.inProgress.uncertain = false;
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

  static void transfer(
      final IoTDBSyncClient client,
      final TPipeTransferReq request,
      final boolean allowIdempotentReplay)
      throws TException, IOException {
    final TPipeTransferResp response = client.pipeTransfer(request);
    if (response == null
        || response.getStatus() == null
        || (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && response.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
            && (!allowIdempotentReplay
                || response.getStatus().getCode()
                    != TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()))) {
      throw new RestoreRequestRejectedException(
          String.format(
              CliMessages.EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_FAILED_ARG_75EE2D11,
              request.getType(),
              response == null ? "null" : response.getStatus()));
    }
  }

  private static Path checkpointPath(final CommandLine line) throws IOException {
    final String configuredCheckpoint =
        line.hasOption(OPTION_CHECKPOINT)
            ? line.getOptionValue(OPTION_CHECKPOINT)
            : line.getOptionValue(OPTION_RESUME);
    final Path source = Path.of(input(line)).toAbsolutePath().normalize();
    final Path checkpoint =
        (configuredCheckpoint == null
                ? source.resolveSibling(source.getFileName() + ".restore.checkpoint.json")
                : Path.of(configuredCheckpoint))
            .toAbsolutePath()
            .normalize();
    LogicalBackupCatalog.rejectSymbolicLinks(checkpoint);
    final Path sourceRoot =
        Files.isDirectory(source)
            ? source
            : LogicalBackupFormat.MANIFEST_FILE_NAME.equals(source.getFileName().toString())
                ? source.getParent()
                : source;
    if (checkpoint.startsWith(sourceRoot)) {
      throw new IOException(
          CliMessages.EXCEPTION_THE_CHECKPOINT_MUST_BE_OUTSIDE_THE_INPUT_BACKUP_15B70F59);
    }
    return checkpoint;
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
      state.backupId = streams.get(0).getManifest().backupId;
      state.backupFormatVersion = streams.get(0).getManifest().formatVersion;
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
    if (!streams.get(0).getManifest().backupId.equals(state.backupId)
        || !streams.get(0).getManifest().formatVersion.equals(state.backupFormatVersion)
        || !sourceStreams.equals(state.sourceStreams)
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
      updateDigest(digest, manifest.formatName);
      updateDigest(digest, manifest.formatVersion);
      updateDigest(digest, manifest.timestampPrecision);
      updateDigest(digest, manifest.sourceVersion);
      updateDigest(digest, GSON.toJson(manifest.captureOptions));
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
    LogicalBackupCatalog.rejectSymbolicLinks(checkpoint);
    checkpointState.updatedAt = java.time.Instant.now().toString();
    final Path temporary = Files.createTempFile(parent, ".checkpoint-", ".tmp");
    final ByteBuffer checkpointBytes = StandardCharsets.UTF_8.encode(GSON.toJson(checkpointState));
    try (final java.nio.channels.FileChannel checkpointChannel =
        java.nio.channels.FileChannel.open(
            temporary, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
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
    System.out.println(
        CliMessages
            .LOG_LOGICAL_BACKUP_DUMP_VERIFY_INSPECT_PACK_RESTORE_OPTIONS_DUMP_CONNECT_TO_A_SOURCE_AND_SEAL_A_BACKUP_DIRECTORY_DOES_NOT_CREATE_A_ZIP_INSPECT_SHOW_BACKUP_IDENTITY_SCOPE_STREAMS_SEGMENTS_COUNTS_AND_STATUS_VERIFY_CHECK_STRUCTURE_CHECKSUMS_AND_SEMANTICS_WITHOUT_CHANGING_THE_INPUT_PACK_VERIFY_AND_PACKAGE_AN_EXISTING_DIRECTORY_AS_ZIP_ACCEPTS_NO_CONNECTION_OPTIONS_RESTORE_PREFLIGHT_AND_REPLAY_A_DIRECTORY_ROOT_MANIFEST_JSON_OR_ZIP_INSPECT_VERIFY_INPUT_PATH_JSON_ALLOW_INCOMPLETE_PACK_INPUT_DIR_OUTPUT_ZIP_JSON_ALLOW_INCOMPLETE_DUMP_HOST_HOST_PORT_PORT_USER_ROOT_PASSWORD_STDIN_PASSWORD_ENV_NAME_BACKUP_DIR_DIR_BACKUP_ID_ID_SCOPE_CONFIG_SCHEMA_DATA_MODEL_TREE_TABLE_INCLUDE_PATTERN_EXCLUDE_PATTERN_START_TIME_TIME_END_TIME_TIME_INCLUDE_AUTH_TRUE_FALSE_MODE_SNAPSHOT_CONTINUOUS_UNTIL_ISO_8601_RESUME_FAIL_IF_EXISTS_APPEND_NEW_PIPE_NAME_NAME_SEGMENT_SIZE_BYTES_268435456_MAX_RECORD_BYTES_67108864_FSYNC_POLICY_ALWAYS_BATCH_PERIODIC_NONE_FSYNC_BATCH_OPERATIONS_N_FSYNC_PERIOD_MS_N_ON_UNSUPPORTED_FAIL_SKIP_INCLUDE_HEARTBEAT_TRUE_FALSE_TIMEOUT_SECONDS_3600_POLL_INTERVAL_SECONDS_2_JSON_ALLOW_INCOMPLETE_CONTINUOUS_REQUIRES_UNTIL_STOP_SEQUENCE_IS_RESERVED_AND_CURRENTLY_REJECTED_OBJECT_FILTERS_REQUIRE_ONE_MODEL_TREE_USES_PATH_LISTS_TABLE_USES_DATABASE_REGEX_TABLE_REGEX_TREE_CONFIG_SCHEMA_CAPTURE_ACCEPTS_A_SINGLE_PREFIX_FULL_PATH_WITHOUT_EXCLUSIONS_TABLE_EXCLUSIONS_MUST_SELECT_WHOLE_DATABASES_OR_WHOLE_TABLE_NAME_PATTERNS_RESTORE_INPUT_PATH_HOST_HOST_PORT_PORT_USER_USER_PASSWORD_STDIN_PASSWORD_ENV_NAME_DRY_RUN_CHECKPOINT_FILE_RESUME_CONFLICT_FAIL_INCLUDE_AUTH_TRUE_FALSE_JSON_ALLOW_INCOMPLETE_RESTORING_AUTHORIZATION_OBJECTS_REQUIRES_INCLUDE_AUTH_TRUE_LEGACY_CONFIG_IS_CONSERVATIVE_A_TARGET_CONNECTION_IS_REQUIRED_FOR_REPLAY_LOCAL_DRY_RUN_NEEDS_NO_CONNECTION_TARGET_PREFLIGHT_CHECKS_VERSION_PRECISION_AUTHENTICATION_AND_HANDSHAKE_SUPPORT_OBJECT_CONFLICTS_AND_PER_OBJECT_PRIVILEGES_ARE_CHECKED_DURING_REPLAY_DEPRECATED_ALIASES_EXPORT_INPUT_DIR_OUTPUT_ZIP_PACK_EXPORT_WITH_SOURCE_CONNECTION_OPTIONS_DUMP_THEN_PACK_IMPORT_RESTORE_EXIT_CODE_0_SUCCESS_1_FAILURE_ERRORS_AND_DEPRECATION_NOTICES_GO_TO_STDERR_B1983BE8);
  }

  static class ImportCheckpoint {
    String formatName;
    String formatVersion;
    String sourceSha256;
    String backupId;
    String backupFormatVersion;
    String status = "RUNNING";
    String updatedAt;
    String lastError;
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
    boolean uncertain;

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

  private static class RestoreRequestRejectedException extends IOException {
    private RestoreRequestRejectedException(final String message) {
      super(message);
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
        if (e instanceof IOException) {
          throw (IOException) e;
        }
        throw new IOException(
            CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81, e);
      }
    }

    private static void extract(final Path archive, final Path target) throws IOException {
      int entryCount = 0;
      long totalBytes = 0;
      final byte[] buffer = new byte[64 * 1024];
      try (final ZipFile zip = new ZipFile(archive.toFile())) {
        final Map<String, ZipEntry> entries = new LinkedHashMap<>();
        final Set<String> allowed = new LinkedHashSet<>();
        final java.util.Enumeration<? extends ZipEntry> enumeration = zip.entries();
        while (enumeration.hasMoreElements()) {
          final ZipEntry entry = enumeration.nextElement();
          if (++entryCount > MAX_ARCHIVE_ENTRIES) {
            throw new IOException(
                CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432);
          }
          LogicalBackupCatalog.resolve(
              target,
              entry.isDirectory()
                  ? entry.getName().substring(0, entry.getName().length() - 1)
                  : entry.getName());
          if (entries.putIfAbsent(entry.getName(), entry) != null
              || entry.getSize() < 0
              || entry.getSize() > MAX_ARCHIVE_BYTES
              || (entry.getSize() > 1024 * 1024
                  && entry.getSize() / Math.max(1, entry.getCompressedSize()) > 1000)) {
            throw new IOException(
                CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432);
          }
        }
        final ZipEntry rootManifest = entries.get(LogicalBackupFormat.MANIFEST_FILE_NAME);
        if (rootManifest != null) {
          final com.google.gson.JsonObject rootJson = manifestJson(zip, rootManifest);
          if (rootJson.has("streams")) {
            final LogicalBackupCatalog catalog =
                GSON.fromJson(rootJson, LogicalBackupCatalog.class);
            allowed.add(LogicalBackupFormat.MANIFEST_FILE_NAME);
            if (catalog.streams == null) {
              throw new IOException(
                  CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
            }
            for (final LogicalBackupCatalog.StreamEntry stream : catalog.streams) {
              if (stream == null
                  || stream.manifest == null
                  || stream.segments == null
                  || !allowed.add(stream.manifest.file)) {
                throw new IOException(
                    CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
              }
              for (final LogicalBackupCatalog.FileEntry segment : stream.segments) {
                if (segment == null || !allowed.add(segment.file)) {
                  throw new IOException(
                      CliMessages
                          .EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
                }
              }
            }
          }
        }
        if (allowed.isEmpty()) {
          // Read archives produced by the initial stream-only format with the same allowlist.
          for (final ZipEntry entry : entries.values()) {
            if (!entry.isDirectory() && entry.getName().endsWith("manifest.json")) {
              final LogicalBackupManifest manifest =
                  GSON.fromJson(manifestJson(zip, entry), LogicalBackupManifest.class);
              if (manifest.segments == null || !allowed.add(entry.getName())) {
                throw new IOException(
                    CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
              }
              final Path parent = Path.of(entry.getName()).getParent();
              for (final LogicalBackupManifest.Segment segment : manifest.segments) {
                if (segment == null
                    || segment.file == null
                    || !allowed.add(
                        parent == null
                            ? segment.file
                            : parent.resolve(segment.file).toString().replace('\\', '/'))) {
                  throw new IOException(
                      CliMessages
                          .EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
                }
              }
            }
          }
        }
        if (allowed.isEmpty()) {
          throw new IOException(
              CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
        }
        for (final String file : allowed) {
          LogicalBackupCatalog.resolve(target, file);
          if (!entries.containsKey(file) || entries.get(file).isDirectory()) {
            throw new IOException(
                CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
          }
        }
        for (final ZipEntry entry : entries.values()) {
          if (entry.isDirectory()) {
            if (allowed.stream().noneMatch(file -> file.startsWith(entry.getName()))) {
              throw new IOException(
                  CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
            }
            continue;
          }
          if (!allowed.contains(entry.getName())) {
            throw new IOException(
                CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
          }
          final Path output = LogicalBackupCatalog.resolve(target, entry.getName());
          Files.createDirectories(output.getParent());
          try (final java.io.InputStream input = zip.getInputStream(entry);
              final OutputStream stream =
                  Files.newOutputStream(
                      output, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            int length;
            while ((length = input.read(buffer)) >= 0) {
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
      }
    }

    private static com.google.gson.JsonObject manifestJson(final ZipFile zip, final ZipEntry entry)
        throws IOException {
      if (entry.isDirectory() || entry.getSize() > 16 * 1024 * 1024) {
        throw new IOException(
            CliMessages.EXCEPTION_LOGICAL_BACKUP_ARCHIVE_EXCEEDS_SAFETY_LIMIT_FFC54432);
      }
      try (final java.io.InputStream input = zip.getInputStream(entry)) {
        final byte[] bytes = input.readNBytes(16 * 1024 * 1024 + 1);
        final com.google.gson.JsonObject json =
            GSON.fromJson(
                new String(bytes, StandardCharsets.UTF_8), com.google.gson.JsonObject.class);
        if (bytes.length > 16 * 1024 * 1024 || json == null) {
          throw new IOException(
              CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81);
        }
        return json;
      } catch (final JsonParseException | IllegalStateException e) {
        throw new IOException(
            CliMessages.EXCEPTION_INVALID_OR_CONFLICTING_LOGICAL_BACKUP_ARGUMENTS_76233F81, e);
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
