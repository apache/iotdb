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

package org.apache.iotdb.cli.fs;

import org.apache.iotdb.cli.fs.command.FilesystemCommand;
import org.apache.iotdb.cli.fs.command.FilesystemCommandHelp;
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.command.FsShellWords;
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsColumn;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.UnsupportedFilesystemMutationProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.fs.write.TsFileWriteExecutor;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.builtins.Less;
import org.jline.builtins.Source;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class FilesystemShell {

  public static final int SUCCESS = 0;
  public static final int USAGE_ERROR = 1;
  public static final int INPUT_ERROR = 2;
  public static final int RUNTIME_ERROR = 3;

  private static final List<String> COMMANDS =
      Arrays.asList(
          "pwd", "ls", "ll", "cd", "stat", "meta", "schema", "stats", "count", "wc", "cat", "head",
          "tail", "grep", "find", "less", "more", "file", "mkdir", "rmdir", "rm", "mv", "cp", "cut",
          "paste", "join", "tree", "help", "exit", "quit", "tee", "write", "export", "sketch",
          "sql");

  private final CliContext ctx;
  private final FilesystemSchemaProvider provider;
  private final FilesystemMutationProvider mutationProvider;
  private final boolean writeEnabled;
  private int lastStatus = SUCCESS;
  private FsPath currentPath = FsPath.absolute("/");
  private FsPath previousPath;
  private byte[] standardInput;
  private BufferedReader confirmationInput;

  public FilesystemShell(CliContext ctx, FilesystemSchemaProvider provider) {
    this(ctx, provider, new UnsupportedFilesystemMutationProvider(), false);
  }

  public FilesystemShell(
      CliContext ctx,
      FilesystemSchemaProvider provider,
      FilesystemMutationProvider mutationProvider,
      boolean writeEnabled) {
    this.ctx = ctx;
    this.provider = provider;
    this.mutationProvider = mutationProvider;
    this.writeEnabled = writeEnabled;
  }

  public boolean execute(String input) throws SQLException {
    return executeInput(input, false);
  }

  private boolean execute(FilesystemCommand command, boolean nonInteractive) throws SQLException {
    lastStatus = SUCCESS;
    standardInput = null;
    confirmationInput = null;
    switch (command.getType()) {
      case PWD:
        ctx.getPrinter().println(currentPath.toString());
        return true;
      case LS:
      case LL:
        printListing(command);
        return true;
      case CD:
        changeDirectory(command.getPath());
        return true;
      case STAT:
        FsNode node = provider.describe(resolve(command.getPath()));
        if (checkExists("stat", node)) {
          printNode(node);
        }
        return true;
      case META:
      case SCHEMA:
      case STATS:
      case COUNT:
        printMetadata(command);
        return true;
      case WC:
        printByteCounts(command);
        return true;
      case CAT:
        printSequentialReads(command.getPaths(), command.getReadOptions());
        return true;
      case HEAD:
        printHead(command);
        return true;
      case TAIL:
        printUnixTail(command, nonInteractive);
        return true;
      case GREP:
      case CUT:
      case PASTE:
      case JOIN:
        try {
          List<List<String>> inputs = new ArrayList<>();
          for (String path : command.getPaths()) inputs.add(textLines(path));
          lastStatus = UnixTextCommands.execute(command, inputs, ctx.getOut());
        } catch (SQLException | IllegalArgumentException e) {
          if (command.getType() != FilesystemCommand.Type.GREP) throw e;
          reportError(2, e.getMessage());
        }
        return true;
      case FIND:
        printFind(resolve(command.getPath()), command, 0);
        return true;
      case LESS:
      case MORE:
        page(command, nonInteractive);
        return true;
      case FILE:
        printFile(command.getPath());
        return true;
      case MKDIR:
        makeDirectories(command);
        return true;
      case RMDIR:
        for (String path : command.getPaths()) rmdir(path);
        return true;
      case RM:
        removePaths(command);
        return true;
      case MV:
      case CP:
        transfer(command);
        return true;
      case WRITE:
        if (!writeEnabled) {
          reportError(
              RUNTIME_ERROR,
              String.format(
                  CliMessages.MESSAGE_ARG_ARG_READ_ONLY_FILE_SYSTEM_A86EB99C,
                  "write",
                  command.getWriteOptions().getOutput()));
        } else {
          TsFileWriteExecutor writer = new TsFileWriteExecutor();
          lastStatus =
              !nonInteractive && ctx.getLineReader() != null
                  ? writer.run(
                      command.getWriteOptions(),
                      ctx.getLineReader().getTerminal().reader(),
                      ctx.getErr())
                  : writer.run(command.getWriteOptions(), ctx.getIn(), ctx.getErr());
        }
        return true;
      case TEE:
        tee(command, nonInteractive);
        return true;
      case EXPORT:
        new FsLocalCommands(ctx.getOut(), provider).export(command, currentPath);
        return true;
      case SKETCH:
        new FsLocalCommands(ctx.getOut(), provider).sketch(command);
        return true;
      case HELP:
        FilesystemCommandHelp.print(ctx.getOut(), command.getPath());
        return true;
      case EXIT:
        lastStatus = command.getLimit();
        return false;
      case TREE:
        printTree(resolve(command.getPath()), command.getDepth());
        return true;
      case INVALID:
        reportError(command.getErrorStatus(), command.getErrorMessage());
        return true;
      case SQL:
        executeSql(command.getStatement());
        return true;
      default:
        reportError(
            USAGE_ERROR,
            String.format(
                CliMessages.MESSAGE_UNSUPPORTED_FILESYSTEM_COMMAND_ARG_428768D0,
                command.getType()));
        return true;
    }
  }

  public boolean executeNonInteractive(String input) throws SQLException {
    return executeInput(input, true);
  }

  /** Execute a command with separate result and diagnostic streams and a script exit status. */
  public int runNonInteractive(String input) {
    try {
      executeNonInteractive(input);
    } catch (IllegalArgumentException e) {
      reportError(USAGE_ERROR, e.getMessage());
    } catch (SQLException e) {
      reportError(
          RUNTIME_ERROR,
          String.format(
              CliMessages.MESSAGE_CANNOT_EXECUTE_FILESYSTEM_COMMAND_ARG_C61FAE4B, e.getMessage()));
    }
    if (ctx.getOut().checkError()) {
      reportError(RUNTIME_ERROR, CliMessages.MESSAGE_FAILED_TO_WRITE_STANDARD_OUTPUT_C1A5CCF7);
    }
    return lastStatus;
  }

  /** Returns null when a command needs a connection; handles local commands before login. */
  public static Integer runOffline(CliContext ctx, String input) {
    return runOffline(ctx, input, false);
  }

  public static Integer runOffline(CliContext ctx, String input, boolean writeEnabled) {
    FsCommandLine line;
    try {
      line = FsCommandLine.parse(input);
    } catch (IllegalArgumentException e) {
      return new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input);
    }
    if (line.compound()) {
      for (String part : line.commands) {
        FilesystemCommand command = FilesystemCommandParser.parse(part);
        if (command.getType() == FilesystemCommand.Type.TAIL && command.hasOption("-f")) {
          return new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input);
        }
      }
      return null;
    }
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    switch (command.getType()) {
      case HELP:
      case INVALID:
      case SKETCH:
      case EXIT:
      case PWD:
      case WRITE:
        return new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input);
      case WC:
      case GREP:
      case CUT:
      case PASTE:
      case JOIN:
      case CAT:
      case HEAD:
      case LESS:
      case MORE:
        if (command.getPaths().stream().allMatch("-"::equals)) {
          return new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input);
        }
        return null;
      case TAIL:
        if (command.getPaths().stream().allMatch("-"::equals)
            && !command.hasOption("--format")
            && !command.hasOption("-m")
            && !command.hasOption("--start")
            && !command.hasOption("--end")
            && !command.hasOption("--offset")
            && !command.hasOption("--tag-filter")) {
          return new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input);
        }
        return null;
      case TEE:
        return command.getPaths().isEmpty()
            ? new FilesystemShell(ctx, null, null, writeEnabled).runNonInteractive(input)
            : null;
      default:
        return null;
    }
  }

  private void reportError(int status, String message) {
    lastStatus = status;
    ctx.getErr().println(message);
  }

  private boolean checkExists(String command, FsNode node) {
    if (node.getType() != FsNodeType.UNKNOWN) {
      return true;
    }
    reportError(
        INPUT_ERROR,
        String.format(
            CliMessages.MESSAGE_ARG_ARG_NO_SUCH_FILE_OR_DIRECTORY_ABDC5A9C,
            command,
            node.getPath()));
    return false;
  }

  public Completer createCompleter() {
    return new FilesystemCompleter();
  }

  private void printTree(FsPath path, int depth) throws SQLException {
    FsNode node = provider.describe(path);
    if (!checkExists("tree", node)) {
      return;
    }
    ctx.getPrinter().println(path.toString());
    long[] counts = new long[2];
    if (isDirectory(node.getType())) {
      printTreeChildren(path, "", 0, depth, counts);
    } else {
      counts[1] = 1;
    }
    ctx.getPrinter().println();
    ctx.getPrinter().println(String.format(CliMessages.FS_TREE_SUMMARY, counts[0], counts[1]));
  }

  private void printTreeChildren(
      FsPath path, String prefix, int currentDepth, int maxDepth, long[] counts)
      throws SQLException {
    if (currentDepth >= maxDepth) {
      return;
    }
    List<FsNode> children = provider.list(path);
    for (int i = 0; i < children.size(); i++) {
      FsNode node = children.get(i);
      boolean last = i == children.size() - 1;
      ctx.getPrinter().println(prefix + (last ? "`-- " : "|-- ") + node.getName());
      if (isDirectory(node.getType())) {
        counts[0]++;
        printTreeChildren(
            node.getPath(), prefix + (last ? "    " : "|   "), currentDepth + 1, maxDepth, counts);
      } else {
        counts[1]++;
      }
    }
  }

  private void changeDirectory(String path) throws SQLException {
    if ("-".equals(path) && previousPath == null) {
      throw new IllegalArgumentException(
          String.format(CliMessages.FS_SCOPE_PATH, "-", currentPath));
    }
    FsPath target = "-".equals(path) ? previousPath : resolve(path);
    FsNode node = provider.describe(target);
    if (!checkExists("cd", node)) {
      return;
    }
    if (isDirectory(node.getType())) {
      previousPath = currentPath;
      currentPath = target;
      if ("-".equals(path)) ctx.getOut().println(currentPath);
    } else {
      reportError(
          INPUT_ERROR,
          String.format(CliMessages.MESSAGE_ARG_ARG_NOT_A_DIRECTORY_CF18DCA5, "cd", target));
    }
  }

  private FsPath resolve(String path) {
    return currentPath.resolve(path);
  }

  private void printList(String path, boolean all, boolean longListing) throws SQLException {
    FsPath resolvedPath = resolve(path);
    FsNode node = provider.describe(resolvedPath);
    if (!checkExists("ls", node)) {
      return;
    }
    if (!isDirectory(node.getType())) {
      if (longListing) {
        printLongNode(node);
      } else {
        ctx.getPrinter().println(node.getName());
      }
      return;
    }
    if (longListing) {
      printLongNodes(provider.list(resolvedPath), all);
    } else {
      printNodes(provider.list(resolvedPath), all);
    }
  }

  private void printNodes(List<FsNode> nodes, boolean all) {
    if (all) {
      ctx.getPrinter().println(".");
      ctx.getPrinter().println("..");
    }
    for (FsNode node : nodes) {
      ctx.getPrinter().println(node.getName());
    }
  }

  private void printLongNodes(List<FsNode> nodes, boolean all) {
    if (all) {
      ctx.getPrinter().println(longMode(FsNodeType.VIRTUAL_ROOT) + " - - - - - .");
      ctx.getPrinter().println(longMode(FsNodeType.VIRTUAL_ROOT) + " - - - - - ..");
    }
    for (FsNode node : nodes) {
      printLongNode(node);
    }
  }

  private void printLongNode(FsNode node) {
    String size = node.getMetadata().getOrDefault("size_bytes", "-");
    ctx.getPrinter().println(longMode(node.getType()) + " - - - " + size + " - " + node.getName());
  }

  private void printNode(FsNode node) throws SQLException {
    ctx.getPrinter().println(String.format(CliMessages.FS_STAT_FILE, node.getPath()));
    ctx.getPrinter().println(String.format(CliMessages.FS_STAT_TYPE, unixType(node.getType())));
    if (!isDirectory(node.getType())) {
      ctx.getPrinter()
          .println(
              String.format(CliMessages.FS_STAT_SIZE, textBytes(node.getPath().toString()).length));
    }
    for (Map.Entry<String, String> entry : node.getMetadata().entrySet()) {
      ctx.getPrinter().println(entry.getKey() + ": " + entry.getValue());
    }
  }

  private void printSequentialReads(List<String> paths, ReadOptions options) throws SQLException {
    for (String path : paths) {
      printReadable(path, options);
    }
  }

  private void printHead(FilesystemCommand command) throws SQLException {
    printReadable(command.getPath(), command.getReadOptions());
  }

  private void printReadable(String path, ReadOptions options) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if ("-".equals(path) || resolvedPath.getFileName().endsWith(".meta")) {
      FsRowReader.validateTextOptions(options);
      byte[] bytes = textBytes(path);
      int start = skipLines(bytes, 0, options.getOffset());
      int end = options.getLimit() < 0 ? bytes.length : skipLines(bytes, start, options.getLimit());
      ctx.getOut().write(bytes, start, end - start);
      return;
    }
    FsRowReader.Result result = new FsRowReader(provider).read(resolvedPath, options, false);
    FsRowRenderer.print(ctx.getOut(), result.getColumns(), result.getRows(), options.getFormat());
  }

  private static int skipLines(byte[] bytes, int start, long count) {
    long lines = 0;
    while (start < bytes.length && lines < count) {
      if (bytes[start++] == '\n') lines++;
    }
    return start;
  }

  private void printFind(FsPath path, FilesystemCommand command, int depth) throws SQLException {
    FsNode node = provider.describe(path);
    if (!checkExists("find", node)) {
      return;
    }
    String type = command.optionValue("-type", "");
    boolean directory = isDirectory(node.getType());
    if ((type.isEmpty() || ("d".equals(type) == directory))
        && matchesFind(node, command.getPattern())) {
      ctx.getPrinter().println(path.toString());
    }
    if (!directory
        || depth
            >= Integer.parseInt(
                command.optionValue("-maxdepth", Integer.toString(Integer.MAX_VALUE)))) {
      return;
    }
    for (FsNode child : provider.list(path)) {
      printFind(child.getPath(), command, depth + 1);
    }
  }

  private static boolean matchesFind(FsNode node, String pattern) {
    return pattern == null
        || pattern.isEmpty()
        || UnixTextCommands.matchesName(node.getName(), pattern);
  }

  private void printFile(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    FsNode node = provider.describe(resolvedPath);
    if (checkExists("file", node)) {
      ctx.getPrinter().println(resolvedPath + ": " + unixType(node.getType()));
    }
  }

  private void mkdir(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("mkdir", resolvedPath)) {
      return;
    }
    mutationProvider.mkdir(resolvedPath);
  }

  private void rmdir(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("rmdir", resolvedPath)) {
      return;
    }
    mutationProvider.rmdir(resolvedPath);
  }

  private void remove(String path, String option) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("rm", resolvedPath)) {
      return;
    }
    if ("-r".equals(option)) {
      mutationProvider.removeRecursive(resolvedPath);
      return;
    }
    mutationProvider.remove(resolvedPath);
  }

  private boolean ensureWritable(String command, FsPath path) {
    if (writeEnabled) {
      return true;
    }
    reportError(
        RUNTIME_ERROR,
        String.format(CliMessages.MESSAGE_ARG_ARG_READ_ONLY_FILE_SYSTEM_A86EB99C, command, path));
    return false;
  }

  private static boolean isDirectory(FsNodeType type) {
    return type == FsNodeType.VIRTUAL_ROOT
        || type == FsNodeType.VIRTUAL_DIRECTORY
        || type == FsNodeType.TREE_ROOT
        || type == FsNodeType.TREE_DATABASE
        || type == FsNodeType.TREE_INTERNAL_PATH
        || type == FsNodeType.TREE_DEVICE
        || type == FsNodeType.TABLE_DATABASE;
  }

  private static String longMode(FsNodeType type) {
    if (isDirectory(type)) {
      return "d---------";
    }
    return "----------";
  }

  private static String unixType(FsNodeType type) {
    if (isDirectory(type)) {
      return CliMessages.FS_FILE_DIRECTORY;
    }
    if (type == FsNodeType.UNKNOWN) {
      return CliMessages.FS_FILE_UNKNOWN;
    }
    return type == FsNodeType.TABLE_META_FILE
        ? CliMessages.FS_FILE_METADATA
        : CliMessages.FS_FILE_CSV;
  }

  private boolean executeInput(String input, boolean nonInteractive) throws SQLException {
    lastStatus = SUCCESS;
    FsCommandLine line = FsCommandLine.parse(input);
    if (!line.compound())
      return execute(expandPaths(FilesystemCommandParser.parse(input)), nonInteractive);
    for (String part : line.commands) {
      FilesystemCommand command = FilesystemCommandParser.parse(part);
      if (command.getType() == FilesystemCommand.Type.TAIL && command.hasOption("-f")) {
        throw new IllegalArgumentException(CliMessages.FS_FOLLOW_COMPOUND);
      }
    }
    if (line.output != null && !ensureWritable(">", resolve(line.output))) return true;
    try {
      java.io.InputStream pipeInput =
          line.input == null
              ? ctx.getIn()
              : new ByteArrayInputStream(Files.readAllBytes(Paths.get(line.input)));
      byte[] result = new byte[0];
      boolean continuing = true;
      for (int i = 0; i < line.commands.size(); i++) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CliContext context =
            new CliContext(
                pipeInput,
                new PrintStream(output, false, "UTF-8"),
                ctx.getErr(),
                org.apache.iotdb.cli.type.ExitType.EXCEPTION);
        FilesystemShell stage =
            new FilesystemShell(context, provider, mutationProvider, writeEnabled);
        stage.currentPath = currentPath;
        stage.previousPath = previousPath;
        FilesystemCommand command = FilesystemCommandParser.parse(line.commands.get(i));
        if ((i > 0 || line.input != null)
            && (command.getType() == FilesystemCommand.Type.CAT
                || command.getType() == FilesystemCommand.Type.HEAD
                || command.getType() == FilesystemCommand.Type.TAIL)
            && ".".equals(command.getPath()))
          command = command.withPaths(Collections.singletonList("-"));
        continuing = stage.execute(stage.expandPaths(command), true);
        lastStatus = stage.lastStatus;
        currentPath = stage.currentPath;
        previousPath = stage.previousPath;
        result = output.toByteArray();
        pipeInput = new ByteArrayInputStream(result);
      }
      if (line.output != null) {
        Path target = Paths.get(line.output);
        Files.write(
            target,
            result,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            line.append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
      } else ctx.getOut().write(result, 0, result.length);
      return continuing;
    } catch (IOException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  private FilesystemCommand expandPaths(FilesystemCommand command) throws SQLException {
    if (provider == null
        || command.getType() == FilesystemCommand.Type.WRITE
        || command.getType() == FilesystemCommand.Type.SKETCH
        || command.getType() == FilesystemCommand.Type.SQL) return command;
    List<String> expanded = new ArrayList<>();
    for (int i = 0; i < command.getPaths().size(); i++) {
      String path = command.getPaths().get(i);
      String pattern = command.getPathPattern(i);
      if (pattern != null) {
        List<FsPath> matches = new ArrayList<>();
        FsPath globPath =
            FsPath.absolute(FsShellWords.literalGlob(currentPath.toString())).resolve(pattern);
        expandGlob(FsPath.absolute("/"), globPath.getSegments(), 0, matches);
        matches.sort(java.util.Comparator.comparing(FsPath::toString));
        if (matches.isEmpty()) expanded.add(path);
        else for (FsPath match : matches) expanded.add(match.toString());
      } else expanded.add(path);
    }
    if (expanded.size() > 1 && !acceptsMultiplePaths(command.getType())) {
      throw new IllegalArgumentException(
          String.format(
              CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
              command.getType().name().toLowerCase(Locale.ROOT),
              expanded.get(1)));
    }
    if (command.getType() == FilesystemCommand.Type.JOIN && expanded.size() != 2) {
      throw new IllegalArgumentException(
          String.format(
              CliMessages.EXCEPTION_ARG_UNEXPECTED_ARGUMENT_ARG_3EF9EC3F,
              "join",
              expanded.toString()));
    }
    return command.withPaths(expanded);
  }

  private static boolean acceptsMultiplePaths(FilesystemCommand.Type type) {
    switch (type) {
      case CAT:
      case WC:
      case GREP:
      case CUT:
      case PASTE:
      case JOIN:
      case CP:
      case MV:
      case RM:
      case RMDIR:
      case MKDIR:
      case TEE:
        return true;
      default:
        return false;
    }
  }

  private void expandGlob(FsPath base, List<String> segments, int index, List<FsPath> result)
      throws SQLException {
    if (index == segments.size()) {
      result.add(base);
      return;
    }
    String pattern = segments.get(index);
    for (FsNode node : provider.list(base)) {
      if ((!node.getName().startsWith(".") || pattern.startsWith("."))
          && UnixTextCommands.matchesName(node.getName(), pattern)) {
        if (index + 1 == segments.size() || isDirectory(node.getType()))
          expandGlob(node.getPath(), segments, index + 1, result);
      }
    }
  }

  private void printMetadata(FilesystemCommand command) throws SQLException {
    FsPath path =
        new FsRowReader(provider).scope(resolve(command.getPath()), command.getReadOptions());
    List<SqlRow> rows;
    switch (command.getType()) {
      case SCHEMA:
        rows = provider.schema(path);
        break;
      case META:
        rows = provider.meta(path);
        break;
      case STATS:
        rows = provider.stats(path, command.getReadOptions());
        break;
      default:
        rows = provider.countRows(path);
    }
    String selectedName = command.getType() == FilesystemCommand.Type.STATS ? "field" : "column";
    if (!command.getColumns().isEmpty()) {
      for (String column : command.getColumns()) {
        boolean found = false;
        for (FsColumn field : provider.columns(path)) {
          if (column.equalsIgnoreCase(field.getName())
              && (command.getType() == FilesystemCommand.Type.SCHEMA
                  || "FIELD".equalsIgnoreCase(field.getCategory())
                  || (command.getType() == FilesystemCommand.Type.COUNT
                      && "TAG".equalsIgnoreCase(field.getCategory())))) found = true;
        }
        if (!found)
          throw new IllegalArgumentException(String.format(CliMessages.FS_UNKNOWN_FIELD, column));
      }
      List<SqlRow> selected = new ArrayList<>();
      for (SqlRow row : rows) {
        if (command.getColumns().stream().anyMatch(c -> c.equalsIgnoreCase(row.get(selectedName))))
          selected.add(row);
      }
      rows = selected;
    }
    if (command.getType() == FilesystemCommand.Type.STATS && command.hasOption("--aggregates")) {
      rows = selectStatsAggregates(rows, command.optionValue("--aggregates", ""));
    }
    List<FsColumn> columns = rows.isEmpty() ? metadataColumns(command, path) : resultColumns(rows);
    FsRowRenderer.print(ctx.getOut(), columns, rows, command.getFormat());
  }

  private List<FsColumn> metadataColumns(FilesystemCommand command, FsPath path)
      throws SQLException {
    List<String> names = new ArrayList<>();
    switch (command.getType()) {
      case SCHEMA:
        names.addAll(
            Arrays.asList(
                "model", "object", "column", "category", "data_type", "encoding", "compression"));
        break;
      case COUNT:
        names.addAll(
            Arrays.asList(
                "model",
                "object",
                "column",
                "category",
                "row_count",
                "entity_count",
                "non_null_count",
                "null_count",
                "min_time",
                "max_time",
                "time_source"));
        break;
      case STATS:
        names.add("model");
        names.add("object");
        for (FsColumn column : provider.columns(path)) {
          if ("TAG".equalsIgnoreCase(column.getCategory())) names.add("tag." + column.getName());
        }
        names.add("field");
        names.add("data_type");
        if (command.hasOption("--aggregates")) {
          names.addAll(Arrays.asList(command.optionValue("--aggregates", "").split(",")));
        } else {
          names.addAll(
              Arrays.asList(
                  "non_null_count",
                  "null_count",
                  "min_time",
                  "max_time",
                  "min",
                  "max",
                  "first",
                  "last",
                  "sum",
                  "avg",
                  "median"));
        }
        names.add("stats_source");
        break;
      default:
        break;
    }
    List<FsColumn> columns = new ArrayList<>();
    for (String name : names) columns.add(new FsColumn(name, "FIELD", "STRING"));
    return columns;
  }

  private static List<SqlRow> selectStatsAggregates(List<SqlRow> rows, String aggregates) {
    List<SqlRow> selected = new ArrayList<>();
    for (SqlRow row : rows) {
      Map<String, String> cells = new LinkedHashMap<>();
      Map<String, String> types = new LinkedHashMap<>();
      for (String name : row.asMap().keySet()) {
        if ("model".equals(name)
            || "object".equals(name)
            || name.startsWith("tag.")
            || "field".equals(name)
            || "data_type".equals(name)) {
          cells.put(name, row.get(name));
          types.put(name, row.getDataType(name));
        }
      }
      for (String aggregate : aggregates.split(",")) {
        String source = "count".equals(aggregate) ? "non_null_count" : aggregate;
        cells.put(aggregate, row.get(source));
        types.put(aggregate, row.getDataType(source));
      }
      cells.put("stats_source", row.get("stats_source"));
      types.put("stats_source", row.getDataType("stats_source"));
      selected.add(new SqlRow(cells, types));
    }
    return selected;
  }

  private static List<FsColumn> resultColumns(List<SqlRow> rows) {
    List<FsColumn> columns = new ArrayList<>();
    if (rows.isEmpty()) return columns;
    SqlRow row = rows.get(0);
    for (String name : row.asMap().keySet()) {
      String type = row.getDataType(name);
      columns.add(new FsColumn(name, "FIELD", type == null ? "STRING" : type));
    }
    return columns;
  }

  private byte[] inputBytes() throws SQLException {
    if (standardInput == null) {
      try {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int length;
        while ((length = ctx.getIn().read(buffer)) >= 0) bytes.write(buffer, 0, length);
        standardInput = bytes.toByteArray();
      } catch (IOException e) {
        throw new SQLException(CliMessages.MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E, e);
      }
    }
    return standardInput;
  }

  private byte[] textBytes(String path) throws SQLException {
    if ("-".equals(path)) return inputBytes();
    FsPath resolved = resolve(path);
    if (resolved.getFileName().endsWith(".meta")) {
      List<String> lines = provider.readLines(resolved, -1);
      return (lines.isEmpty() ? "" : String.join("\n", lines) + "\n")
          .getBytes(StandardCharsets.UTF_8);
    }
    ReadOptions options =
        new ReadOptions(
            "csv",
            "",
            "",
            Collections.emptyList(),
            -1,
            0,
            null,
            null,
            Collections.emptyList(),
            "all");
    FsRowReader.Result result = new FsRowReader(provider).read(resolved, options, false);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    FsRowRenderer.print(new PrintStream(bytes), result.getColumns(), result.getRows(), "csv");
    return bytes.toByteArray();
  }

  private List<String> textLines(String path) throws SQLException {
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                new ByteArrayInputStream(textBytes(path)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) lines.add(line);
    } catch (IOException e) {
      throw new SQLException(CliMessages.MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E, e);
    }
    return lines;
  }

  private void printByteCounts(FilesystemCommand command) throws SQLException {
    long total = 0;
    for (String path : command.getPaths()) {
      long bytes = textBytes(path).length;
      total += bytes;
      ctx.getOut().println(bytes + ("-".equals(path) ? "" : " " + resolve(path)));
    }
    if (command.getPaths().size() > 1) ctx.getOut().println(total + " total");
  }

  private void page(FilesystemCommand command, boolean nonInteractive) throws SQLException {
    byte[] bytes = textBytes(command.getPath());
    if (nonInteractive
        || ctx.getLineReader() == null
        || "dumb".equals(ctx.getLineReader().getTerminal().getType())) {
      ctx.getOut().write(bytes, 0, bytes.length);
      return;
    }
    Less pager = new Less(ctx.getLineReader().getTerminal(), Paths.get("."));
    pager.quitAtFirstEof = command.getType() == FilesystemCommand.Type.MORE;
    try {
      pager.run(
          new Source.InputStreamSource(new ByteArrayInputStream(bytes), true, command.getPath()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException(CliMessages.FS_INTERRUPTED, e);
    } catch (IOException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  private void printUnixTail(FilesystemCommand command, boolean nonInteractive)
      throws SQLException {
    boolean text =
        "-".equals(command.getPath()) || resolve(command.getPath()).getFileName().endsWith(".meta");
    if (text) FsRowReader.validateTextOptions(command.getReadOptions());
    if (text && command.hasOption("--offset")) {
      throw new IllegalArgumentException(
          String.format(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D, "tail", "--offset"));
    }
    if ((command.hasOption("-f") || command.hasOption("-c") || command.hasOption("--from-start"))
        && (command.hasOption("--format")
            || command.hasOption("-m")
            || command.hasOption("--start")
            || command.hasOption("--end")
            || command.hasOption("--offset")
            || command.hasOption("--tag-filter")
            || command.hasOption("-d")
            || command.hasOption("-t"))) {
      throw new IllegalArgumentException(
          String.format(
              CliMessages.EXCEPTION_ARG_UNSUPPORTED_OPTION_ARG_33DE669D,
              "tail",
              "-f/-c/+N with query options"));
    }
    if (!text
        && (command.hasOption("--format")
            || command.hasOption("-d")
            || command.hasOption("-t")
            || command.hasOption("-m")
            || command.hasOption("--start")
            || command.hasOption("--end")
            || command.hasOption("--offset")
            || command.hasOption("--tag-filter"))) {
      FsRowReader.Result result =
          new FsRowReader(provider)
              .read(resolve(command.getPath()), command.getReadOptions(), true);
      FsRowRenderer.print(ctx.getOut(), result.getColumns(), result.getRows(), command.getFormat());
      return;
    }
    byte[] snapshot = textBytes(command.getPath());
    outputTail(snapshot, command);
    if (!command.hasOption("-f") || "-".equals(command.getPath())) return;
    org.jline.terminal.Terminal terminal =
        ctx.getLineReader() == null ? null : ctx.getLineReader().getTerminal();
    java.util.concurrent.atomic.AtomicBoolean interrupted =
        new java.util.concurrent.atomic.AtomicBoolean();
    org.jline.terminal.Terminal.SignalHandler previous =
        terminal == null
            ? null
            : terminal.handle(
                org.jline.terminal.Terminal.Signal.INT, signal -> interrupted.set(true));
    try {
      while (!interrupted.get()
          && !Thread.currentThread().isInterrupted()
          && !ctx.getOut().checkError()) {
        Thread.sleep(1000);
        byte[] next = textBytes(command.getPath());
        int offset =
            next.length >= snapshot.length && startsWith(next, snapshot) ? snapshot.length : 0;
        ctx.getOut().write(next, offset, next.length - offset);
        ctx.getOut().flush();
        snapshot = next;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (terminal != null) terminal.handle(org.jline.terminal.Terminal.Signal.INT, previous);
    }
  }

  private static boolean startsWith(byte[] bytes, byte[] prefix) {
    for (int i = 0; i < prefix.length; i++) if (bytes[i] != prefix[i]) return false;
    return true;
  }

  private void outputTail(byte[] bytes, FilesystemCommand command) {
    int count = command.getLimit();
    int start;
    if (command.hasOption("-c")) {
      start =
          command.hasOption("--from-start")
              ? Math.max(0, count - 1)
              : Math.max(0, bytes.length - count);
    } else if (command.hasOption("--from-start")) {
      start = 0;
      for (int lines = 1; start < bytes.length && lines < count; start++)
        if (bytes[start] == '\n') lines++;
    } else {
      start = bytes.length;
      int lines = 0;
      if (count > 0) {
        for (int i = bytes.length - 1; i >= 0; i--) {
          if (bytes[i] == '\n' && i != bytes.length - 1 && ++lines == count) break;
          start = i;
        }
      }
    }
    start = Math.min(start, bytes.length);
    ctx.getOut().write(bytes, start, bytes.length - start);
  }

  private void makeDirectories(FilesystemCommand command) throws SQLException {
    if (command.hasOption("-m")) throw new IllegalArgumentException(CliMessages.FS_VIRTUAL_MODE);
    for (String path : command.getPaths()) {
      FsPath target = resolve(path);
      if (command.hasOption("-p") && isDirectory(provider.describe(target).getType())) continue;
      mkdir(path);
    }
  }

  private void removePaths(FilesystemCommand command) throws SQLException {
    for (String path : command.getPaths()) {
      FsPath target = resolve(path);
      if (command.hasOption("-f") && provider.describe(target).getType() == FsNodeType.UNKNOWN)
        continue;
      if (command.hasOption("-i") && !confirm(String.format(CliMessages.FS_CONFIRM_REMOVE, target)))
        continue;
      remove(path, command.hasOption("-r") ? "-r" : "");
    }
  }

  private void transfer(FilesystemCommand command) throws SQLException {
    List<String> paths = command.getPaths();
    FsPath destination = resolve(paths.get(paths.size() - 1));
    String operation = command.getType().name().toLowerCase(Locale.ROOT);
    if (!ensureWritable(operation, destination)) return;
    boolean directory = isDirectory(provider.describe(destination).getType());
    if (paths.size() > 2 && !directory)
      throw new SQLException(
          String.format(
              CliMessages.MESSAGE_ARG_ARG_NOT_A_DIRECTORY_CF18DCA5, operation, destination));
    for (int i = 0; i + 1 < paths.size(); i++) {
      FsPath source = resolve(paths.get(i));
      FsPath target = directory ? destination.resolve(source.getFileName()) : destination;
      if (source.equals(target))
        throw new SQLException(String.format(CliMessages.FS_SAME_FILE, target));
      boolean exists = provider.describe(target).getType() != FsNodeType.UNKNOWN;
      if (exists && command.hasOption("-n")) continue;
      if (exists
          && command.hasOption("-i")
          && !confirm(String.format(CliMessages.FS_CONFIRM_REPLACE, operation, target))) continue;
      if (command.getType() == FilesystemCommand.Type.CP)
        mutationProvider.copy(source, target, exists);
      else mutationProvider.move(source, target, exists);
    }
  }

  private boolean confirm(String prompt) throws SQLException {
    ctx.getErr().print(prompt);
    ctx.getErr().flush();
    try {
      String answer;
      if (ctx.getLineReader() != null) {
        answer = ctx.getLineReader().readLine();
      } else {
        if (confirmationInput == null) {
          confirmationInput =
              new BufferedReader(new InputStreamReader(ctx.getIn(), StandardCharsets.UTF_8));
        }
        answer = confirmationInput.readLine();
      }
      return "y".equalsIgnoreCase(answer) || "yes".equalsIgnoreCase(answer);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  private void tee(FilesystemCommand command, boolean nonInteractive) throws SQLException {
    for (String path : command.getPaths()) if (!ensureWritable("tee", resolve(path))) return;
    if (!nonInteractive && ctx.getLineReader() != null) {
      java.io.Reader reader = ctx.getLineReader().getTerminal().reader();
      StringBuilder input = new StringBuilder();
      char[] buffer = new char[8192];
      try {
        int length;
        while ((length = reader.read(buffer)) >= 0) input.append(buffer, 0, length);
        standardInput = input.toString().getBytes(StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new SQLException(CliMessages.MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E, e);
      }
    }
    byte[] bytes = inputBytes();
    List<String> lines = textLines("-");
    for (String path : command.getPaths())
      mutationProvider.write(resolve(path), lines, command.hasOption("-a"));
    ctx.getOut().write(bytes, 0, bytes.length);
  }

  private void executeSql(String sql) throws SQLException {
    if (!writeEnabled && !isReadOnlySql(sql))
      throw new IllegalArgumentException(CliMessages.FS_SQL_READONLY);
    List<SqlRow> rows = provider.executeSql(sql);
    FsRowRenderer.print(ctx.getOut(), resultColumns(rows), rows, "table");
  }

  private static boolean isReadOnlySql(String sql) {
    // A conservative gate also excludes SELECT INTO and additional statements.
    String text = sql.trim();
    if (text.endsWith(";")) text = text.substring(0, text.length() - 1);
    return text.matches("(?is)^(SELECT|SHOW|DESC|DESCRIBE)\\s+.*")
        && !text.contains(";")
        && !text.contains("/*")
        && !text.contains("--")
        && !Pattern.compile("(?i)\\bINTO\\b").matcher(text).find();
  }

  private void printListing(FilesystemCommand command) throws SQLException {
    boolean longListing = command.getType() == FilesystemCommand.Type.LL || command.hasOption("-l");
    boolean all = command.hasOption("-a");
    if (command.hasOption("-f")) {
      FsPath path = resolve(command.getPath());
      FsNode node = provider.describe(path);
      if (!checkExists("ls", node)) return;
      List<SqlRow> rows = new ArrayList<>();
      collectListing(node, rows, command.hasOption("-R"));
      FsRowRenderer.print(
          ctx.getOut(),
          Arrays.asList(
              new FsColumn("model", "FIELD", "STRING"), new FsColumn("object", "FIELD", "STRING")),
          rows,
          command.getFormat());
      return;
    }
    if (command.hasOption("-R")) {
      recursiveList(resolve(command.getPath()), all, longListing);
    } else printList(command.getPath(), all, longListing);
  }

  private void collectListing(FsNode parent, List<SqlRow> rows, boolean recursive)
      throws SQLException {
    List<FsNode> nodes =
        isDirectory(parent.getType())
            ? provider.list(parent.getPath())
            : Collections.singletonList(parent);
    for (FsNode node : nodes) {
      String model = node.getType().name().startsWith("TREE_") ? "tree" : "table";
      String object = node.getMetadata().get("table");
      if (object == null) object = node.getPath().toString();
      // A metadata sidecar describes the same table object as its CSV sibling.
      if (node.getType() != FsNodeType.TABLE_META_FILE)
        rows.add(SqlRow.of("model", model, "object", object));
      if (recursive && isDirectory(node.getType())) collectListing(node, rows, true);
    }
  }

  private void recursiveList(FsPath path, boolean all, boolean longListing) throws SQLException {
    ctx.getOut().println(path + ":");
    printList(path.toString(), all, longListing);
    for (FsNode child : provider.list(path)) {
      if (isDirectory(child.getType())) {
        ctx.getOut().println();
        recursiveList(child.getPath(), all, longListing);
      }
    }
  }

  private class FilesystemCompleter implements Completer {

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
      if (line.wordIndex() == 0) {
        completeCommand(line.word(), candidates);
        return;
      }
      completePath(line.word(), candidates);
    }

    private void completeCommand(String prefix, List<Candidate> candidates) {
      for (String command : COMMANDS) {
        if (command.startsWith(prefix)) {
          candidates.add(new Candidate(command));
        }
      }
    }

    private void completePath(String word, List<Candidate> candidates) {
      try {
        FsPath basePath = completionBasePath(word);
        String prefix = completionPrefix(word);
        for (FsNode node : provider.list(basePath)) {
          if (!node.getName().startsWith(prefix)) {
            continue;
          }
          String value = completionValue(word, node);
          candidates.add(new Candidate(value));
        }
      } catch (SQLException e) {
        // Ignore completion errors to keep TAB non-disruptive.
      }
    }

    private FsPath completionBasePath(String word) {
      int slash = word.lastIndexOf('/');
      if (slash < 0) {
        return currentPath;
      }
      String parent = slash == 0 ? "/" : word.substring(0, slash);
      return resolve(parent);
    }

    private String completionPrefix(String word) {
      int slash = word.lastIndexOf('/');
      if (slash < 0) {
        return word;
      }
      return word.substring(slash + 1);
    }

    private String completionValue(String word, FsNode node) {
      int slash = word.lastIndexOf('/');
      String parent = slash < 0 ? "" : word.substring(0, slash + 1);
      String suffix = isDirectory(node.getType()) ? "/" : "";
      return parent + node.getName() + suffix;
    }
  }
}
