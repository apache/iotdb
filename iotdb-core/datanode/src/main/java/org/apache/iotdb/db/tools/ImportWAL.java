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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.i18n.ImportWALMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.ObjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusLogToTabletConverter;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportWAL {

  private static final int CODE_OK = 0;
  private static final int CODE_ERROR = 1;
  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 6667;
  private static final String DEFAULT_USER = "root";
  private static final int SNAPSHOT_TABLET_ROW_LIMIT = 1024;

  private ImportWAL() {}

  public static void main(final String[] args) {
    System.exit(run(args, System.out, System.err));
  }

  static int run(final String[] args, final PrintStream out, final PrintStream err) {
    final Options options = createOptions();
    if (containsHelpOption(args)) {
      printHelp(options, out);
      return CODE_OK;
    }

    final CommandLine commandLine;
    try {
      commandLine = new DefaultParser().parse(options, args);
    } catch (final ParseException e) {
      err.printf(ImportWALMessages.MESSAGE_ARGUMENT_ERROR_ARG_A9767F62, e.getMessage());
      err.println();
      printHelp(options, err);
      return CODE_ERROR;
    }

    try {
      final Path source = Paths.get(commandLine.getOptionValue("file"));
      final List<Path> walFiles = collectWALFiles(source);
      final String database = commandLine.getOptionValue("database");
      final boolean deleteSource =
          shouldDeleteSource(commandLine.getOptionValue("on_success", "none"));
      final String password = getPassword(commandLine);
      final Session treeSession =
          createSession(
              commandLine.getOptionValue("host", DEFAULT_HOST),
              parsePort(commandLine.getOptionValue("port", String.valueOf(DEFAULT_PORT))),
              commandLine.getOptionValue("username", DEFAULT_USER),
              password,
              null);
      final Session tableSession =
          database == null
              ? null
              : createSession(
                  commandLine.getOptionValue("host", DEFAULT_HOST),
                  parsePort(commandLine.getOptionValue("port", String.valueOf(DEFAULT_PORT))),
                  commandLine.getOptionValue("username", DEFAULT_USER),
                  password,
                  database);
      try {
        treeSession.open(false);
        if (tableSession != null) {
          tableSession.open(false);
        }
        final ReplayStatistics statistics =
            replayWALFiles(
                walFiles, new WALReplayer(treeSession, tableSession, database), out, deleteSource);
        out.printf(
            ImportWALMessages
                .MESSAGE_REPLAYED_ARG_OPERATIONS_FROM_ARG_WAL_FILES_SKIPPED_ARG_ENTRIES_F0D37E3A,
            statistics.replayedOperationCount,
            walFiles.size(),
            statistics.skippedEntryCount);
        out.println();
        out.printf(
            ImportWALMessages
                .MESSAGE_IMPORT_DURATION_ARG_SECONDS_TOTAL_SIZE_ARG_BYTES_AVERAGE_RATE_ARG_MB_PER_SECOND_4B4EA58D,
            statistics.getElapsedSeconds(),
            statistics.getTotalBytes(),
            statistics.getAverageRateMbPerSecond());
        out.println();
        if (deleteSource) {
          out.printf(
              ImportWALMessages.MESSAGE_DELETED_ARG_SOURCE_WAL_FILES_C7A5AA1B, walFiles.size());
          out.println();
        }
      } finally {
        closeSession(tableSession);
        closeSession(treeSession);
      }
      return CODE_OK;
    } catch (final Exception e) {
      err.printf(ImportWALMessages.MESSAGE_WAL_IMPORT_FAILED_ARG_55C014BA, e.getMessage());
      err.println();
      return CODE_ERROR;
    }
  }

  private static Options createOptions() {
    final Options options = new Options();
    options.addOption(
        Option.builder("f")
            .longOpt("file")
            .hasArg()
            .required()
            .desc(
                ImportWALMessages
                    .MESSAGE_PATH_OF_A_WAL_FILE_OR_A_DIRECTORY_CONTAINING_WAL_FILES_473D0554)
            .build());
    options.addOption(
        Option.builder("h")
            .longOpt("host")
            .hasArg()
            .desc(ImportWALMessages.MESSAGE_TARGET_IOTDB_HOST_DEFAULT_127_0_0_1_3729156F)
            .build());
    options.addOption(
        Option.builder("p")
            .longOpt("port")
            .hasArg()
            .desc(ImportWALMessages.MESSAGE_TARGET_IOTDB_RPC_PORT_DEFAULT_6667_FC0D345D)
            .build());
    options.addOption(
        Option.builder("u")
            .longOpt("username")
            .hasArg()
            .desc(ImportWALMessages.MESSAGE_TARGET_IOTDB_USERNAME_DEFAULT_ROOT_EB91453B)
            .build());
    options.addOption(
        Option.builder("pw")
            .longOpt("password")
            .hasArg()
            .desc(
                ImportWALMessages
                    .MESSAGE_TARGET_IOTDB_PASSWORD_PROMPTED_INTERACTIVELY_IF_OMITTED_29681961)
            .build());
    options.addOption(
        Option.builder("db")
            .longOpt("database")
            .hasArg()
            .desc(ImportWALMessages.MESSAGE_TARGET_DATABASE_FOR_TABLE_MODEL_WAL_ENTRIES_27BACD1C)
            .build());
    options.addOption(
        Option.builder("os")
            .longOpt("on_success")
            .argName("on_success")
            .hasArg()
            .desc(
                ImportWALMessages
                    .MESSAGE_WHEN_ALL_WAL_FILES_ARE_REPLAYED_SUCCESSFULLY_DO_OPERATION_ON_SOURCE_WAL_FILES_OPTIONAL_PARAMETERS_ARE_NONE_DEFAULT_AND_DELETE_41963A66)
            .build());
    options.addOption(
        Option.builder()
            .longOpt("help")
            .desc(ImportWALMessages.MESSAGE_PRINT_THIS_HELP_MESSAGE_E800AF7A)
            .build());
    return options;
  }

  private static String getPassword(final CommandLine commandLine) {
    return getPassword(commandLine, System.console());
  }

  static String getPassword(final CommandLine commandLine, final Console console) {
    if (commandLine.hasOption("password")) {
      return commandLine.getOptionValue("password");
    }
    if (console == null) {
      throw new IllegalArgumentException(
          ImportWALMessages
              .EXCEPTION_PASSWORD_WAS_NOT_PROVIDED_AND_INTERACTIVE_INPUT_IS_UNAVAILABLE_40F42BCD);
    }
    final char[] password =
        console.readPassword(ImportWALMessages.MESSAGE_PASSWORD_PROMPT_F2D0E794);
    if (password == null) {
      throw new IllegalArgumentException(
          ImportWALMessages
              .EXCEPTION_PASSWORD_WAS_NOT_PROVIDED_AND_INTERACTIVE_INPUT_IS_UNAVAILABLE_40F42BCD);
    }
    return new String(password);
  }

  static boolean shouldDeleteSource(final String onSuccess) {
    final String normalizedOnSuccess = onSuccess.trim();
    if ("none".equalsIgnoreCase(normalizedOnSuccess)) {
      return false;
    }
    if ("delete".equalsIgnoreCase(normalizedOnSuccess)) {
      return true;
    }
    throw new IllegalArgumentException(
        String.format(
            ImportWALMessages
                .EXCEPTION_UNSUPPORTED_ON_SUCCESS_VALUE_ARG_EXPECTED_NONE_OR_DELETE_F1C8EACE,
            onSuccess));
  }

  private static boolean containsHelpOption(final String[] args) {
    if (args == null) {
      return false;
    }
    for (final String arg : args) {
      if ("--help".equals(arg) || "-help".equals(arg)) {
        return true;
      }
    }
    return false;
  }

  private static void printHelp(final Options options, final PrintStream stream) {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(120);
    formatter.printHelp(
        new PrintWriter(stream, true),
        120,
        ImportWALMessages.MESSAGE_IMPORT_WAL_5E42804E,
        null,
        options,
        2,
        2,
        null,
        true);
  }

  private static int parsePort(final String port) {
    try {
      final int value = Integer.parseInt(port);
      if (value <= 0 || value > 65535) {
        throw new NumberFormatException(port);
      }
      return value;
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(ImportWALMessages.EXCEPTION_INVALID_PORT_ARG_A7CDD5AC, port), e);
    }
  }

  private static Session createSession(
      final String host,
      final int port,
      final String username,
      final String password,
      final String database) {
    final Session.Builder builder =
        new Session.Builder().host(host).port(port).username(username).password(password);
    if (database != null) {
      builder.sqlDialect("table").database(database);
    }
    return builder.build();
  }

  private static void closeSession(final Session session) {
    if (session == null) {
      return;
    }
    try {
      session.close();
    } catch (final IoTDBConnectionException ignored) {
      // The import result has already been determined; closing failure must not hide it.
    }
  }

  static List<Path> collectWALFiles(final Path source) throws IOException {
    if (!Files.exists(source)) {
      throw new IOException(
          String.format(
              ImportWALMessages.EXCEPTION_SOURCE_PATH_DOES_NOT_EXIST_ARG_7C806CA2, source));
    }
    if (Files.isRegularFile(source)) {
      if (!isWALFile(source)) {
        throw new IOException(
            String.format(
                ImportWALMessages.EXCEPTION_SOURCE_FILE_IS_NOT_A_WAL_FILE_ARG_14A43F76, source));
      }
      return List.of(source.toAbsolutePath().normalize());
    }
    if (!Files.isDirectory(source)) {
      throw new IOException(
          String.format(
              ImportWALMessages.EXCEPTION_SOURCE_PATH_DOES_NOT_EXIST_ARG_7C806CA2, source));
    }

    final List<Path> walFiles;
    try (Stream<Path> stream = Files.walk(source)) {
      walFiles =
          stream
              .filter(Files::isRegularFile)
              .filter(ImportWAL::isWALFile)
              .map(path -> path.toAbsolutePath().normalize())
              .sorted(WAL_FILE_COMPARATOR)
              .collect(Collectors.toList());
    }
    if (walFiles.isEmpty()) {
      throw new IOException(
          String.format(ImportWALMessages.EXCEPTION_NO_WAL_FILES_FOUND_UNDER_ARG_45F7FA22, source));
    }
    return walFiles;
  }

  private static boolean isWALFile(final Path path) {
    return path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".wal");
  }

  private static final Comparator<Path> WAL_FILE_COMPARATOR =
      Comparator.comparing((Path path) -> Objects.toString(path.getParent(), ""))
          .thenComparingLong(ImportWAL::getWALVersion)
          .thenComparing(path -> path.getFileName().toString());

  private static long getWALVersion(final Path path) {
    final String filename = path.getFileName().toString();
    return WALFileUtils.WAL_FILE_NAME_PATTERN.matcher(filename).find()
        ? WALFileUtils.parseVersionId(filename)
        : Long.MAX_VALUE;
  }

  static ReplayStatistics replayWALFiles(final List<Path> walFiles, final WALReplayer replayer)
      throws IOException {
    return replayWALFiles(walFiles, replayer, null);
  }

  static ReplayStatistics replayWALFiles(
      final List<Path> walFiles, final WALReplayer replayer, final PrintStream progressStream)
      throws IOException {
    return replayWALFiles(walFiles, replayer, progressStream, false);
  }

  static ReplayStatistics replayWALFiles(
      final List<Path> walFiles,
      final WALReplayer replayer,
      final PrintStream progressStream,
      final boolean deleteSource)
      throws IOException {
    final ReplayStatistics statistics = new ReplayStatistics();
    final long startNanos = System.nanoTime();
    for (final Path walFile : walFiles) {
      statistics.totalBytes += Files.size(walFile);
    }
    for (final Path walFile : walFiles) {
      try (WALReader reader = new WALReader(walFile.toFile())) {
        long offset = reader.getWALCurrentReadOffset();
        while (reader.hasNext()) {
          final WALEntry entry = reader.next();
          try {
            if (replayer.replay(entry)) {
              statistics.replayedOperationCount++;
            } else {
              statistics.skippedEntryCount++;
            }
          } catch (final IoTDBConnectionException | StatementExecutionException e) {
            throw new WALReplayException(
                String.format(
                    ImportWALMessages
                        .EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9,
                    walFile,
                    offset,
                    e.getMessage()),
                e);
          }
          offset = reader.getWALCurrentReadOffset();
        }
        if (reader.isFileCorrupted()) {
          throw new WALReplayException(
              String.format(
                  ImportWALMessages
                      .EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9,
                  walFile,
                  reader.getWALCurrentReadOffset(),
                  ImportWALMessages.EXCEPTION_THE_WAL_FILE_IS_TRUNCATED_OR_CORRUPTED_6B0734C5),
              null);
        }
        statistics.completedFileCount++;
        statistics.processedBytes += Files.size(walFile);
        statistics.elapsedNanos = System.nanoTime() - startNanos;
        if (progressStream != null) {
          progressStream.printf(
              ImportWALMessages
                  .MESSAGE_PROGRESS_ARG_COMPLETED_FILES_ARG_TOTAL_FILES_ARG_PROCESSED_BYTES_ARG_TOTAL_BYTES_ARG_PERCENT_ARG_ELAPSED_SECONDS_ARG_RATE_ARG_MB_PER_SECOND_F1C1356F,
              statistics.completedFileCount,
              walFiles.size(),
              statistics.processedBytes,
              statistics.totalBytes,
              statistics.getProgressPercent(),
              statistics.getElapsedSeconds(),
              statistics.getAverageRateMbPerSecond());
          progressStream.println();
        }
      } catch (final IOException e) {
        if (e instanceof WALReplayException walReplayException) {
          throw walReplayException;
        }
        throw new WALReplayException(
            String.format(
                ImportWALMessages
                    .EXCEPTION_FAILED_TO_REPLAY_WAL_FILE_ARG_AT_OFFSET_ARG_ARG_FCFAF7F9,
                walFile,
                0,
                e.getMessage()),
            e);
      }
    }
    if (deleteSource) {
      deleteSourceWALFiles(walFiles);
    }
    return statistics;
  }

  private static void deleteSourceWALFiles(final List<Path> walFiles) throws IOException {
    for (final Path walFile : walFiles) {
      try {
        Files.delete(walFile);
      } catch (final IOException e) {
        throw new IOException(
            String.format(
                ImportWALMessages.EXCEPTION_FAILED_TO_DELETE_SOURCE_WAL_FILE_ARG_ARG_236AF580,
                walFile,
                e.getMessage()),
            e);
      }
    }
  }

  static class WALReplayer {

    private final Session treeSession;
    private final Session tableSession;
    private final ConsensusLogToTabletConverter converter;
    private final UnsupportedEntryPrompt unsupportedEntryPrompt;
    private final Map<String, List<IMeasurementSchema>> tableTagSchemas = new HashMap<>();

    WALReplayer(
        final Session treeSession, final Session tableSession, final String tableDatabaseName) {
      this(
          treeSession,
          tableSession,
          tableDatabaseName,
          createUnsupportedEntryPrompt(System.console()));
    }

    WALReplayer(
        final Session treeSession,
        final Session tableSession,
        final String tableDatabaseName,
        final UnsupportedEntryPrompt unsupportedEntryPrompt) {
      this.treeSession = treeSession;
      this.tableSession = tableSession;
      this.unsupportedEntryPrompt = unsupportedEntryPrompt;
      converter =
          new ConsensusLogToTabletConverter(
              null, null, ColumnFilterMatcher.matchAll(), tableDatabaseName);
    }

    boolean replay(final WALEntry entry)
        throws IoTDBConnectionException, StatementExecutionException {
      if (entry.getType() == WALEntryType.MEMORY_TABLE_SNAPSHOT
          || entry.getType() == WALEntryType.OLD_MEMORY_TABLE_SNAPSHOT) {
        return replayMemTableSnapshot((IMemTable) entry.getValue());
      }
      if (entry.getValue() instanceof InsertNode insertNode) {
        replayInsert(insertNode);
        return true;
      }
      if (entry.getValue() instanceof DeleteDataNode deleteDataNode) {
        replayTreeDelete(deleteDataNode);
        return true;
      }
      if (entry.getValue() instanceof RelationalDeleteDataNode
          || entry.getValue() instanceof ObjectNode) {
        // A null prompt means no interactive console is available, so preserve fail-fast behavior.
        if (unsupportedEntryPrompt != null && unsupportedEntryPrompt.shouldSkip(entry)) {
          return false;
        }
        throw unsupportedOperation(entry);
      }
      return false;
    }

    private static UnsupportedEntryPrompt createUnsupportedEntryPrompt(final Console console) {
      if (console == null) {
        return null;
      }
      return entry -> {
        final String answer =
            console.readLine(
                ImportWALMessages
                    .MESSAGE_UNSUPPORTED_WAL_OPERATION_ARG_SKIP_THIS_ENTRY_Y_N_DAFBE650,
                entry.getType());
        return isSkipConfirmation(answer);
      };
    }

    static boolean isSkipConfirmation(final String answer) {
      return answer != null
          && ("y".equalsIgnoreCase(answer.trim()) || "yes".equalsIgnoreCase(answer.trim()));
    }

    @FunctionalInterface
    interface UnsupportedEntryPrompt {

      boolean shouldSkip(WALEntry entry);
    }

    private static StatementExecutionException unsupportedOperation(final WALEntry entry) {
      return new StatementExecutionException(
          String.format(
              ImportWALMessages.EXCEPTION_UNSUPPORTED_WAL_OPERATION_ARG_ABD227A0, entry.getType()));
    }

    private void replayInsert(final InsertNode node)
        throws IoTDBConnectionException, StatementExecutionException {
      if (node instanceof InsertRowsNode insertRowsNode
          && !(node instanceof RelationalInsertRowsNode)) {
        for (final InsertNode rowNode : insertRowsNode.getInsertRowNodeList()) {
          replayInsert(rowNode);
        }
        return;
      }
      final List<Tablet> tablets = converter.convert(node);
      if (tablets.isEmpty()) {
        throw new StatementExecutionException(
            String.format(
                ImportWALMessages.EXCEPTION_INSERT_NODE_ARG_CONTAINS_NO_REPLAYABLE_DATA_5DA13453,
                node.getType()));
      }
      final boolean tableModel = isTableModelInsert(node);
      if (tableModel && tableSession == null) {
        throw new StatementExecutionException(
            ImportWALMessages.EXCEPTION_TABLE_MODEL_WAL_ENTRIES_REQUIRE_DB_DATABASE_F7597726);
      }
      for (final Tablet tablet : tablets) {
        if (tableModel) {
          tableSession.insertRelationalTablet(tablet);
        } else if (node.isAligned()) {
          treeSession.insertAlignedTablet(tablet);
        } else {
          treeSession.insertTablet(tablet);
        }
      }
    }

    private static boolean isTableModelInsert(final InsertNode node) {
      return node instanceof RelationalInsertRowNode
          || node instanceof RelationalInsertRowsNode
          || node instanceof RelationalInsertTabletNode;
    }

    private void replayTreeDelete(final DeleteDataNode node)
        throws IoTDBConnectionException, StatementExecutionException {
      final List<String> paths = new ArrayList<>(node.getPathList().size());
      for (final MeasurementPath path : node.getPathList()) {
        paths.add(path.getFullPath());
      }
      treeSession.deleteData(paths, node.getDeleteStartTime(), node.getDeleteEndTime());
    }

    private boolean replayMemTableSnapshot(final IMemTable memTable)
        throws IoTDBConnectionException, StatementExecutionException {
      if (memTable == null || memTable.isSignalMemTable()) {
        return false;
      }
      boolean replayed = false;
      for (Map.Entry<IDeviceID, IWritableMemChunkGroup> deviceEntry :
          memTable.getMemTableMap().entrySet()) {
        final IDeviceID deviceId = deviceEntry.getKey();
        final IWritableMemChunkGroup group = deviceEntry.getValue();
        for (IWritableMemChunk chunk : group.getMemChunkMap().values()) {
          if (chunk == null || chunk.isEmpty()) {
            continue;
          }
          if (chunk instanceof AlignedWritableMemChunk) {
            replayed |= replayAlignedMemChunk(deviceId, (AlignedWritableMemChunk) chunk);
          } else {
            replayed |= replayNonAlignedMemChunk(deviceId, chunk);
          }
        }
      }
      return replayed;
    }

    private boolean replayNonAlignedMemChunk(
        final IDeviceID deviceId, final IWritableMemChunk chunk)
        throws IoTDBConnectionException, StatementExecutionException {
      final List<IMeasurementSchema> schemas = Collections.singletonList(chunk.getSchema());
      final boolean tableModel = deviceId.isTableModel();
      requireTableSessionIfNeeded(tableModel);
      final TableTabletSchema tabletSchema = createTableTabletSchema(deviceId, schemas);
      final List<TVList> lists = new ArrayList<>();
      lists.addAll(chunk.getSortedList());
      lists.add(chunk.getWorkingTVList());
      boolean replayed = false;
      for (TVList list : lists) {
        if (list == null || list.rowCount() == 0) {
          continue;
        }
        if (!list.isSorted()) {
          list.sort();
        }
        for (int start = 0; start < list.rowCount(); start += SNAPSHOT_TABLET_ROW_LIMIT) {
          final int end = Math.min(start + SNAPSHOT_TABLET_ROW_LIMIT, list.rowCount());
          final Tablet tablet =
              buildNonAlignedTablet(deviceId, tabletSchema, schemas, list, start, end);
          sendTablet(tablet, tableModel, false);
          replayed = true;
        }
      }
      return replayed;
    }

    private boolean replayAlignedMemChunk(
        final IDeviceID deviceId, final AlignedWritableMemChunk chunk)
        throws IoTDBConnectionException, StatementExecutionException {
      final boolean tableModel = deviceId.isTableModel();
      requireTableSessionIfNeeded(tableModel);
      final List<IMeasurementSchema> schemas = chunk.getSchemaList();
      final TableTabletSchema tabletSchema = createTableTabletSchema(deviceId, schemas);
      final List<AlignedTVList> lists = new ArrayList<>();
      lists.addAll(chunk.getSortedList());
      lists.add(chunk.getWorkingTVList());
      boolean replayed = false;
      for (AlignedTVList list : lists) {
        if (list == null || list.rowCount() == 0) {
          continue;
        }
        if (!list.isSorted()) {
          list.sort();
        }
        final List<Integer> replayableRows = getReplayableAlignedRows(list);
        for (int start = 0; start < replayableRows.size(); start += SNAPSHOT_TABLET_ROW_LIMIT) {
          final int end = Math.min(start + SNAPSHOT_TABLET_ROW_LIMIT, replayableRows.size());
          final Tablet tablet =
              buildAlignedTablet(deviceId, tabletSchema, schemas, list, replayableRows, start, end);
          sendTablet(tablet, tableModel, true);
          replayed = true;
        }
      }
      return replayed;
    }

    private static List<Integer> getReplayableAlignedRows(final AlignedTVList list) {
      final List<Integer> replayableRows = new ArrayList<>(list.rowCount());
      for (int row = 0; row < list.rowCount(); row++) {
        if (!list.isTimeDeleted(row)) {
          replayableRows.add(row);
        }
      }
      return replayableRows;
    }

    private void requireTableSessionIfNeeded(final boolean tableModel)
        throws StatementExecutionException {
      if (tableModel && tableSession == null) {
        throw new StatementExecutionException(
            ImportWALMessages.EXCEPTION_TABLE_MODEL_WAL_ENTRIES_REQUIRE_DB_DATABASE_F7597726);
      }
    }

    private TableTabletSchema createTableTabletSchema(
        final IDeviceID deviceId, final List<IMeasurementSchema> fieldSchemas)
        throws IoTDBConnectionException, StatementExecutionException {
      if (!deviceId.isTableModel()) {
        return new TableTabletSchema(fieldSchemas, null, 0);
      }
      final String tableName = deviceId.getTableName();
      List<IMeasurementSchema> tagSchemas = tableTagSchemas.get(tableName);
      if (tagSchemas == null) {
        tagSchemas = new ArrayList<>();
        try (SessionDataSet dataSet =
            tableSession.executeQueryStatement("DESCRIBE " + quoteIdentifier(tableName))) {
          final SessionDataSet.DataIterator iterator = dataSet.iterator();
          while (iterator.next()) {
            final String category = iterator.getString(3);
            if ("TAG".equalsIgnoreCase(category)) {
              tagSchemas.add(
                  new MeasurementSchema(
                      iterator.getString(1), TSDataType.valueOf(iterator.getString(2))));
            }
          }
        }
        tableTagSchemas.put(tableName, tagSchemas);
      }
      final List<IMeasurementSchema> schemas =
          new ArrayList<>(tagSchemas.size() + fieldSchemas.size());
      schemas.addAll(tagSchemas);
      schemas.addAll(fieldSchemas);
      final List<ColumnCategory> categories = new ArrayList<>(schemas.size());
      categories.addAll(Collections.nCopies(tagSchemas.size(), ColumnCategory.TAG));
      categories.addAll(Collections.nCopies(fieldSchemas.size(), ColumnCategory.FIELD));
      return new TableTabletSchema(schemas, categories, tagSchemas.size());
    }

    static String quoteIdentifier(final String identifier) {
      return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private void sendTablet(final Tablet tablet, final boolean tableModel, final boolean aligned)
        throws IoTDBConnectionException, StatementExecutionException {
      if (tableModel) {
        tableSession.insertRelationalTablet(tablet);
      } else if (aligned) {
        treeSession.insertAlignedTablet(tablet);
      } else {
        treeSession.insertTablet(tablet);
      }
    }

    private static Tablet buildNonAlignedTablet(
        final IDeviceID deviceId,
        final TableTabletSchema tabletSchema,
        final List<IMeasurementSchema> sourceSchemas,
        final TVList list,
        final int start,
        final int end) {
      final int rowCount = end - start;
      final long[] times = new long[rowCount];
      final Object[] values = createValueArrays(tabletSchema.schemas, rowCount);
      final BitMap[] bitMaps = new BitMap[tabletSchema.schemas.size()];
      final int fieldColumnIndex = tabletSchema.tagCount;
      final TSDataType type = sourceSchemas.get(0).getType();
      for (int i = 0; i < rowCount; i++) {
        final int scanIndex = start + i;
        final int valueIndex = list.getValueIndex(scanIndex);
        times[i] = list.getTime(scanIndex);
        if (list.isNullValue(valueIndex)) {
          if (bitMaps[fieldColumnIndex] == null) {
            bitMaps[fieldColumnIndex] = new BitMap(rowCount);
          }
          bitMaps[fieldColumnIndex].mark(i);
        } else {
          putValue(values[fieldColumnIndex], i, type, list, scanIndex);
        }
      }
      populateTableTags(deviceId, tabletSchema.categories, values, bitMaps, rowCount);
      return tabletSchema.categories == null
          ? new Tablet(deviceId.toString(), tabletSchema.schemas, times, values, bitMaps, rowCount)
          : new Tablet(
              deviceId.getTableName(),
              tabletSchema.schemas,
              tabletSchema.categories,
              times,
              values,
              bitMaps,
              rowCount);
    }

    private static Tablet buildAlignedTablet(
        final IDeviceID deviceId,
        final TableTabletSchema tabletSchema,
        final List<IMeasurementSchema> sourceSchemas,
        final AlignedTVList list,
        final List<Integer> replayableRows,
        final int start,
        final int end) {
      final int rowCount = end - start;
      final long[] times = new long[rowCount];
      final Object[] values = createValueArrays(tabletSchema.schemas, rowCount);
      final BitMap[] bitMaps = new BitMap[tabletSchema.schemas.size()];
      final List<TSDataType> types = list.getTsDataTypes();
      for (int i = 0; i < rowCount; i++) {
        final int scanIndex = replayableRows.get(start + i);
        final int valueIndex = list.getValueIndex(scanIndex);
        times[i] = list.getTime(scanIndex);
        for (int c = 0; c < sourceSchemas.size(); c++) {
          final int targetColumnIndex = tabletSchema.tagCount + c;
          if (c >= types.size() || list.isNullValue(valueIndex, c)) {
            if (bitMaps[targetColumnIndex] == null) {
              bitMaps[targetColumnIndex] = new BitMap(rowCount);
            }
            bitMaps[targetColumnIndex].mark(i);
          } else {
            putValue(values[targetColumnIndex], i, types.get(c), list, valueIndex, c);
          }
        }
      }
      populateTableTags(deviceId, tabletSchema.categories, values, bitMaps, rowCount);
      return tabletSchema.categories == null
          ? new Tablet(deviceId.toString(), tabletSchema.schemas, times, values, bitMaps, rowCount)
          : new Tablet(
              deviceId.getTableName(),
              tabletSchema.schemas,
              tabletSchema.categories,
              times,
              values,
              bitMaps,
              rowCount);
    }

    private static void populateTableTags(
        final IDeviceID deviceId,
        final List<ColumnCategory> columnCategories,
        final Object[] values,
        final BitMap[] bitMaps,
        final int rowCount) {
      if (columnCategories == null) {
        return;
      }
      int tagSegmentIndex = 1;
      for (int columnIndex = 0; columnIndex < columnCategories.size(); columnIndex++) {
        if (columnCategories.get(columnIndex) != ColumnCategory.TAG) {
          continue;
        }
        final Object segment =
            tagSegmentIndex < deviceId.segmentNum() ? deviceId.segment(tagSegmentIndex) : null;
        tagSegmentIndex++;
        final Binary tagValue =
            segment == null ? null : new Binary(segment.toString(), TSFileConfig.STRING_CHARSET);
        for (int row = 0; row < rowCount; row++) {
          if (tagValue == null) {
            if (bitMaps[columnIndex] == null) {
              bitMaps[columnIndex] = new BitMap(rowCount);
            }
            bitMaps[columnIndex].mark(row);
          } else {
            ((Binary[]) values[columnIndex])[row] = tagValue;
            if (bitMaps[columnIndex] != null) {
              bitMaps[columnIndex].unmark(row);
            }
          }
        }
      }
    }

    private static Object createValueArray(final TSDataType type, final int rowCount) {
      return switch (type) {
        case BOOLEAN -> new boolean[rowCount];
        case INT32 -> new int[rowCount];
        case DATE -> new LocalDate[rowCount];
        case INT64, TIMESTAMP -> new long[rowCount];
        case FLOAT -> new float[rowCount];
        case DOUBLE -> new double[rowCount];
        case TEXT, STRING, BLOB, OBJECT -> new Binary[rowCount];
        case VECTOR, UNKNOWN -> throw unsupportedSnapshotDataType(type);
      };
    }

    private static Object[] createValueArrays(
        final List<IMeasurementSchema> schemas, final int rowCount) {
      final Object[] values = new Object[schemas.size()];
      for (int column = 0; column < schemas.size(); column++) {
        values[column] = createValueArray(schemas.get(column).getType(), rowCount);
      }
      return values;
    }

    private static void putValue(
        final Object target,
        final int targetIndex,
        final TSDataType type,
        final TVList list,
        final int sourceIndex) {
      switch (type) {
        case BOOLEAN -> ((boolean[]) target)[targetIndex] = list.getBoolean(sourceIndex);
        case INT32 -> ((int[]) target)[targetIndex] = list.getInt(sourceIndex);
        case DATE ->
            ((LocalDate[]) target)[targetIndex] =
                DateUtils.parseIntToLocalDate(list.getInt(sourceIndex));
        case INT64, TIMESTAMP -> ((long[]) target)[targetIndex] = list.getLong(sourceIndex);
        case FLOAT -> ((float[]) target)[targetIndex] = list.getFloat(sourceIndex);
        case DOUBLE -> ((double[]) target)[targetIndex] = list.getDouble(sourceIndex);
        case TEXT, STRING, BLOB, OBJECT ->
            ((Binary[]) target)[targetIndex] = list.getBinary(sourceIndex);
        case VECTOR, UNKNOWN -> throw unsupportedSnapshotDataType(type);
      }
    }

    private static void putValue(
        final Object target,
        final int targetIndex,
        final TSDataType type,
        final AlignedTVList list,
        final int sourceIndex,
        final int columnIndex) {
      switch (type) {
        case BOOLEAN ->
            ((boolean[]) target)[targetIndex] =
                list.getBooleanByValueIndex(sourceIndex, columnIndex);
        case INT32 ->
            ((int[]) target)[targetIndex] = list.getIntByValueIndex(sourceIndex, columnIndex);
        case DATE ->
            ((LocalDate[]) target)[targetIndex] =
                DateUtils.parseIntToLocalDate(list.getIntByValueIndex(sourceIndex, columnIndex));
        case INT64, TIMESTAMP ->
            ((long[]) target)[targetIndex] = list.getLongByValueIndex(sourceIndex, columnIndex);
        case FLOAT ->
            ((float[]) target)[targetIndex] = list.getFloatByValueIndex(sourceIndex, columnIndex);
        case DOUBLE ->
            ((double[]) target)[targetIndex] = list.getDoubleByValueIndex(sourceIndex, columnIndex);
        case TEXT, STRING, BLOB, OBJECT ->
            ((Binary[]) target)[targetIndex] = list.getBinaryByValueIndex(sourceIndex, columnIndex);
        case VECTOR, UNKNOWN -> throw unsupportedSnapshotDataType(type);
      }
    }

    private static IllegalArgumentException unsupportedSnapshotDataType(final TSDataType type) {
      return new IllegalArgumentException(
          String.format(
              ImportWALMessages.EXCEPTION_UNSUPPORTED_SNAPSHOT_DATA_TYPE_ARG_7A32D312, type));
    }

    private static class TableTabletSchema {
      private final List<IMeasurementSchema> schemas;
      private final List<ColumnCategory> categories;
      private final int tagCount;

      private TableTabletSchema(
          final List<IMeasurementSchema> schemas,
          final List<ColumnCategory> categories,
          final int tagCount) {
        this.schemas = schemas;
        this.categories = categories;
        this.tagCount = tagCount;
      }
    }
  }

  static class ReplayStatistics {
    private long replayedOperationCount;
    private long skippedEntryCount;
    private long totalBytes;
    private long processedBytes;
    private long completedFileCount;
    private long elapsedNanos;

    long getReplayedOperationCount() {
      return replayedOperationCount;
    }

    long getSkippedEntryCount() {
      return skippedEntryCount;
    }

    long getTotalBytes() {
      return totalBytes;
    }

    long getCompletedFileCount() {
      return completedFileCount;
    }

    double getElapsedSeconds() {
      return elapsedNanos / 1_000_000_000.0;
    }

    double getProgressPercent() {
      return totalBytes == 0 ? 100.0 : processedBytes * 100.0 / totalBytes;
    }

    double getAverageRateMbPerSecond() {
      final double elapsedSeconds = getElapsedSeconds();
      return elapsedSeconds <= 0 ? 0.0 : processedBytes / elapsedSeconds / (1024.0 * 1024.0);
    }
  }

  private static class WALReplayException extends IOException {
    private WALReplayException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
