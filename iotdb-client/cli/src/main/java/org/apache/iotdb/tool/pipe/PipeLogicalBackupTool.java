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
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

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
  private static final String COMMAND_RESTORE = "restore";
  private static final String COMMAND_STATS = "stats";
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
      case COMMAND_RESTORE:
        return importBackup(line);
      case COMMAND_STATS:
        return inspect(line);
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
    final CommandLine line = new DefaultParser().parse(options, args);
    final String input = optionValue(line, OPTION_INPUT, OPTION_SOURCE);
    if (input == null || input.isBlank()) {
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

  private static int export(final CommandLine line) throws IOException {
    final Path source = Path.of(input(line)).toAbsolutePath().normalize();
    final Path target =
        Path.of(optionValue(line, OPTION_OUTPUT, OPTION_TARGET)).toAbsolutePath().normalize();
    final List<BackupStream> streams = read(line);
    final Path root;
    if (Files.isDirectory(source)) {
      root = source;
    } else if (Files.isRegularFile(source)
        && "manifest.json".equals(source.getFileName().toString())) {
      root = source.getParent();
    } else {
      throw new IOException(
          String.format(
              CliMessages
                  .EXCEPTION_LOGICAL_BACKUP_EXPORT_SOURCE_MUST_BE_A_DIRECTORY_OR_MANIFEST_ARG_0BBDE9F0,
              source));
    }
    if (target.startsWith(root)) {
      throw new IOException(
          CliMessages.EXCEPTION_LOGICAL_BACKUP_EXPORT_TARGET_MUST_NOT_BE_INSIDE_SOURCE_A499F994);
    }
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
      throw new java.nio.file.FileAlreadyExistsException(target.toString());
    }
    final Path parent = target.toAbsolutePath().normalize().getParent();
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
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (final java.nio.file.AtomicMoveNotSupportedException e) {
        Files.move(temporary, target);
      }
      published = true;
    } finally {
      if (!published) {
        Files.deleteIfExists(temporary);
      }
    }
    System.out.println(
        String.format(
            CliMessages.LOG_LOGICAL_BACKUP_EXPORTED_FROM_ARG_TO_ARG_B3E8D280, source, target));
    return 0;
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
      long importedGroups = 0;
      for (final BackupStream stream : streams) {
        final String streamId = stream.getManifest().streamId;
        final long lastApplied = checkpointState.appliedSequences.getOrDefault(streamId, -1L);
        for (final EventGroup group : stream.getEventGroups()) {
          if (group.getLastSequence() <= lastApplied) {
            continue;
          }
          for (final LogicalBackupRecord record : group.getRequests()) {
            transfer(client, record.toTPipeTransferReq());
          }
          checkpointState.appliedSequences.put(streamId, group.getLastSequence());
          writeCheckpoint(checkpoint, checkpointState);
          importedGroups++;
        }
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

  private static void transfer(final IoTDBSyncClient client, final TPipeTransferReq request)
      throws TException, IOException {
    final TPipeTransferResp response = client.pipeTransfer(request);
    if (response == null
        || response.getStatus() == null
        || (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && response.getStatus().getCode()
                != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())) {
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

  private static ImportCheckpoint readCheckpoint(
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
        || !checkpointSequencesAreValid(streams, state.appliedSequences)) {
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

  private static boolean checkpointSequencesAreValid(
      final List<BackupStream> streams, final Map<String, Long> appliedSequences) {
    if (appliedSequences.size() > streams.size()) {
      return false;
    }
    for (final BackupStream stream : streams) {
      if (!appliedSequences.containsKey(stream.getManifest().streamId)) {
        continue;
      }
      final Long applied = appliedSequences.get(stream.getManifest().streamId);
      if (applied == null) {
        return false;
      }
      if (applied == -1) {
        continue;
      }
      if (stream.getEventGroups().stream().noneMatch(group -> group.getLastSequence() == applied)) {
        return false;
      }
    }
    return appliedSequences.keySet().stream()
        .allMatch(
            streamId ->
                streams.stream()
                    .anyMatch(stream -> stream.getManifest().streamId.equals(streamId)));
  }

  private static void writeCheckpoint(final Path checkpoint, final ImportCheckpoint checkpointState)
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
        CliMessages.LOG_PIPE_LOGICAL_BACKUP_INSPECT_VERIFY_EXPORT_IMPORT_RESTORE_STATS_BFF9FDC2,
        CliMessages
            .LOG_USE_INPUT_TO_SPECIFY_THE_INPUT_EXPORT_ALSO_REQUIRES_OUTPUT_IMPORT_REQUIRES_HOST_AND_PORT_USE_PASSWORD_STDIN_OR_PASSWORD_ENV_TO_AVOID_COMMAND_LINE_PASSWORDS_4677380E,
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
    return options;
  }

  private static class ImportCheckpoint {
    private String formatName;
    private String formatVersion;
    private Map<String, String> sourceStreams = new LinkedHashMap<>();
    private String targetHost;
    private int targetPort;
    private String targetUser;
    private Map<String, Long> appliedSequences = new LinkedHashMap<>();
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
