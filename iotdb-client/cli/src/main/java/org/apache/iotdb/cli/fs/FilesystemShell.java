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
import org.apache.iotdb.cli.fs.command.ReadOptions;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.UnsupportedFilesystemMutationProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FilesystemShell {

  public static final int SUCCESS = 0;
  public static final int USAGE_ERROR = 1;
  public static final int INPUT_ERROR = 2;
  public static final int RUNTIME_ERROR = 3;
  private static final int DEFAULT_READ_LIMIT = 20;

  private static final List<String> COMMANDS =
      Arrays.asList(
          "pwd", "ls", "ll", "cd", "stat", "meta", "schema", "stats", "count", "cat", "head",
          "tail", "grep", "find", "less", "more", "file", "mkdir", "rmdir", "rm", "mv", "cp", "cut",
          "paste", "join", "tree", "help", "exit", "quit", "tee");

  private final CliContext ctx;
  private final FilesystemSchemaProvider provider;
  private final FilesystemMutationProvider mutationProvider;
  private final boolean writeEnabled;
  private int lastStatus = SUCCESS;
  private FsPath currentPath = FsPath.absolute("/");

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
    return execute(FilesystemCommandParser.parse(input), false);
  }

  private boolean execute(FilesystemCommand command, boolean nonInteractive) throws SQLException {
    lastStatus = SUCCESS;
    switch (command.getType()) {
      case PWD:
        ctx.getPrinter().println(currentPath.toString());
        return true;
      case LS:
        printList(command.getPath(), isAllOption(command), false);
        return true;
      case LL:
        printList(command.getPath(), isAllOption(command), true);
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
        FsPath metadataPath = resolve(command.getPath());
        printRows(provider.meta(metadataPath));
        return true;
      case SCHEMA:
        printRows(provider.schema(resolve(command.getPath())));
        return true;
      case STATS:
        printRows(provider.stats(resolve(command.getPath())));
        return true;
      case COUNT:
        printRows(provider.countRows(resolve(command.getPath())));
        return true;
      case CAT:
        printSequentialReads(command.getPaths(), command.getReadOptions());
        return true;
      case HEAD:
        printHead(command);
        return true;
      case TAIL:
        printTail(command);
        return true;
      case GREP:
        printMatchingRows(command.getPath(), command.getPattern());
        return true;
      case FIND:
        printFind(resolve(command.getPath()), command.getPattern());
        return true;
      case LESS:
      case MORE:
        printReadable(command.getPath(), DEFAULT_READ_LIMIT);
        return true;
      case FILE:
        printFile(command.getPath());
        return true;
      case MKDIR:
        mkdir(command.getPath());
        return true;
      case RMDIR:
        rmdir(command.getPath());
        return true;
      case RM:
        remove(command.getPath(), command.getOption());
        return true;
      case MV:
        move(command.getPaths());
        return true;
      case CP:
        copy(command.getPaths());
        return true;
      case CUT:
        printCut(command.getPath(), command.getOption(), command.getPattern());
        return true;
      case PASTE:
        printPaste(command.getPaths());
        return true;
      case JOIN:
        printJoin(command.getPaths(), command.getOption(), command.getPattern());
        return true;
      case TEE:
        append(command.getPath(), nonInteractive);
        return true;
      case HELP:
        FilesystemCommandHelp.print(ctx.getOut(), command.getPath());
        return true;
      case EXIT:
        return false;
      case TREE:
        printTree(resolve(command.getPath()), command.getDepth());
        return true;
      case INVALID:
        reportError(USAGE_ERROR, command.getErrorMessage());
        return true;
      case SQL:
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
    return execute(FilesystemCommandParser.parse(input), true);
  }

  /** Execute a command with separate result and diagnostic streams and a script exit status. */
  public int runNonInteractive(String input) {
    try {
      executeNonInteractive(input);
    } catch (SQLException | IllegalArgumentException e) {
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

  /** Returns null when a command needs a connection; handles help and usage before login. */
  public static Integer runOffline(CliContext ctx, String input) {
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    switch (command.getType()) {
      case HELP:
      case INVALID:
      case SQL:
      case EXIT:
      case PWD:
        return new FilesystemShell(ctx, null).runNonInteractive(input);
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
    if (!isDirectory(node.getType())) {
      ctx.getPrinter().println(node.getName());
      return;
    }
    printTreeChildren(path, 0, depth);
  }

  private void printTreeChildren(FsPath path, int currentDepth, int maxDepth) throws SQLException {
    if (currentDepth >= maxDepth) {
      return;
    }
    for (FsNode node : provider.list(path)) {
      ctx.getPrinter().println(indent(currentDepth) + node.getName());
      if (isDirectory(node.getType())) {
        printTreeChildren(node.getPath(), currentDepth + 1, maxDepth);
      }
    }
  }

  private static String indent(int depth) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      builder.append("  ");
    }
    return builder.toString();
  }

  private void changeDirectory(String path) throws SQLException {
    FsPath target = resolve(path);
    FsNode node = provider.describe(target);
    if (!checkExists("cd", node)) {
      return;
    }
    if (isDirectory(node.getType())) {
      currentPath = target;
    } else {
      reportError(
          INPUT_ERROR,
          String.format(CliMessages.MESSAGE_ARG_ARG_NOT_A_DIRECTORY_CF18DCA5, "cd", target));
    }
  }

  private FsPath resolve(String path) {
    return currentPath.resolve(path);
  }

  private List<FsPath> resolve(List<String> paths) {
    List<FsPath> resolvedPaths = new ArrayList<>();
    for (String path : paths) {
      resolvedPaths.add(resolve(path));
    }
    return resolvedPaths;
  }

  private void printList(String path, boolean all, boolean longListing) throws SQLException {
    FsPath resolvedPath = resolve(path);
    FsNode node = provider.describe(resolvedPath);
    if (!checkExists("ls", node)) {
      return;
    }
    if (!isDirectory(node.getType())) {
      if (longListing) {
        ctx.getPrinter().println(longMode(node.getType()) + "  1 iotdb iotdb 0 " + node.getName());
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
      ctx.getPrinter().println(longMode(FsNodeType.VIRTUAL_ROOT) + "  1 iotdb iotdb 0 .");
      ctx.getPrinter().println(longMode(FsNodeType.VIRTUAL_ROOT) + "  1 iotdb iotdb 0 ..");
    }
    for (FsNode node : nodes) {
      ctx.getPrinter().println(longMode(node.getType()) + "  1 iotdb iotdb 0 " + node.getName());
    }
  }

  private void printNode(FsNode node) {
    ctx.getPrinter().println("File: " + node.getPath());
    ctx.getPrinter().println("Type: " + unixType(node.getType()));
    for (Map.Entry<String, String> entry : node.getMetadata().entrySet()) {
      ctx.getPrinter().println(entry.getKey() + ": " + entry.getValue());
    }
  }

  private void printRows(List<SqlRow> rows) {
    for (SqlRow row : rows) {
      ctx.getPrinter().println(joinValues(row));
    }
  }

  private void printRows(List<SqlRow> rows, String format) {
    if ("csv".equalsIgnoreCase(format)) {
      if (!rows.isEmpty()) {
        List<String> headers = new ArrayList<>();
        for (String header : rows.get(0).asMap().keySet()) {
          headers.add(csvValue(header));
        }
        ctx.getPrinter().println(String.join(",", headers));
      }
      for (SqlRow row : rows) {
        List<String> values = new ArrayList<>();
        for (String value : row.asMap().values()) {
          values.add(csvValue(value));
        }
        ctx.getPrinter().println(String.join(",", values));
      }
    } else if ("ndjson".equalsIgnoreCase(format)) {
      for (SqlRow row : rows) {
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, String> e : row.asMap().entrySet()) {
          values.add(jsonString(e.getKey()) + ":" + jsonValue(e.getKey(), e.getValue()));
        }
        ctx.getPrinter().println("{" + String.join(",", values) + "}");
      }
    } else {
      printTableRows(rows);
    }
  }

  private void printTableRows(List<SqlRow> rows) {
    if (rows.isEmpty()) return;
    List<String> headers = new ArrayList<>(rows.get(0).asMap().keySet());
    List<Integer> widths = new ArrayList<>();
    for (String header : headers) widths.add(header.length());
    for (SqlRow row : rows) {
      for (int i = 0; i < headers.size(); i++) {
        String value = row.asMap().get(headers.get(i));
        widths.set(i, Math.max(widths.get(i), value == null ? 0 : value.length()));
      }
    }
    ctx.getPrinter().println(formatTableLine(headers, widths));
    for (SqlRow row : rows) {
      List<String> values = new ArrayList<>();
      for (String header : headers) values.add(row.asMap().getOrDefault(header, ""));
      ctx.getPrinter().println(formatTableLine(values, widths));
    }
  }

  private static String formatTableLine(List<String> values, List<Integer> widths) {
    StringBuilder line = new StringBuilder();
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) line.append("  ");
      String value = values.get(i) == null ? "" : values.get(i);
      line.append(value);
      for (int padding = value.length(); padding < widths.get(i); padding++) line.append(' ');
    }
    return line.toString();
  }

  private void printLines(List<String> lines) {
    for (String line : lines) {
      ctx.getPrinter().println(line);
    }
  }

  private void printSequentialReads(List<String> paths, ReadOptions options) throws SQLException {
    for (String path : paths) {
      printReadable(path, options);
    }
  }

  private void printReadable(String path, int limit) throws SQLException {
    printReadable(path, limit, "table");
  }

  private void printReadable(String path, int limit, String format) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      printLines(provider.readLines(resolvedPath, limit));
      return;
    }
    printRows(provider.read(resolvedPath, limit), format);
  }

  private void printHead(FilesystemCommand command) throws SQLException {
    printReadable(command.getPath(), command.getReadOptions());
  }

  private void printReadable(String path, ReadOptions options) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      printLines(
          provider.readLines(
              resolvedPath,
              options.getLimit() < 0 ? DEFAULT_READ_LIMIT : (int) options.getLimit()));
      return;
    }
    List<SqlRow> rows = provider.read(resolvedPath, readLimit(options));
    List<SqlRow> filtered = new ArrayList<>();
    for (SqlRow row : rows) {
      String time = valueIgnoreCase(row, "time");
      if (options.getStart() != null && !withinLowerBound(time, options.getStart())) continue;
      if (options.getEnd() != null && !withinUpperBound(time, options.getEnd())) continue;
      if (!matchesTagFilters(row, options)) continue;
      filtered.add(row);
    }
    filtered = projectColumns(filtered, options.getColumns());
    long offset = Math.min(options.getOffset(), filtered.size());
    List<SqlRow> result = filtered.subList((int) offset, filtered.size());
    if (options.getLimit() >= 0 && result.size() > options.getLimit())
      result = result.subList(0, (int) options.getLimit());
    printRows(result, options.getFormat());
  }

  private static boolean withinLowerBound(String value, long bound) {
    try {
      return value != null && Long.parseLong(value) >= bound;
    } catch (NumberFormatException e) {
      return true;
    }
  }

  private static boolean withinUpperBound(String value, long bound) {
    try {
      return value != null && Long.parseLong(value) <= bound;
    } catch (NumberFormatException e) {
      return true;
    }
  }

  private void printTail(FilesystemCommand command) throws SQLException {
    ReadOptions options = command.getReadOptions();
    String path = command.getPath();
    int limit = readLimit(options);
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      printLines(provider.tailLines(resolvedPath, limit));
      return;
    }
    List<SqlRow> rows = provider.tail(resolvedPath, limit);
    List<SqlRow> filtered = new ArrayList<>();
    for (SqlRow row : rows) {
      String time = valueIgnoreCase(row, "time");
      if (options.getStart() != null && !withinLowerBound(time, options.getStart())) continue;
      if (options.getEnd() != null && !withinUpperBound(time, options.getEnd())) continue;
      if (!matchesTagFilters(row, options)) continue;
      filtered.add(row);
    }
    filtered = projectColumns(filtered, options.getColumns());
    long offset = Math.min(options.getOffset(), filtered.size());
    List<SqlRow> result = filtered.subList((int) offset, filtered.size());
    if (options.getLimit() >= 0 && result.size() > options.getLimit()) {
      result = result.subList(0, (int) options.getLimit());
    }
    printRows(result, options.getFormat());
  }

  private static String csvValue(String value) {
    if (value == null) {
      return "\\N";
    }
    if (value.isEmpty()
        || value.indexOf(',') >= 0
        || value.indexOf('"') >= 0
        || value.indexOf('\n') >= 0
        || value.indexOf('\r') >= 0) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  private static String jsonString(String value) {
    if (value == null) {
      return "\"\"";
    }
    StringBuilder escaped = new StringBuilder(value.length() + 2);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '\\':
          escaped.append("\\\\");
          break;
        case '"':
          escaped.append("\\\"");
          break;
        case '\b':
          escaped.append("\\b");
          break;
        case '\f':
          escaped.append("\\f");
          break;
        case '\n':
          escaped.append("\\n");
          break;
        case '\r':
          escaped.append("\\r");
          break;
        case '\t':
          escaped.append("\\t");
          break;
        default:
          if (c < 0x20) {
            escaped.append(String.format("\\u%04x", (int) c));
          } else {
            escaped.append(c);
          }
      }
    }
    return "\"" + escaped + "\"";
  }

  private static String jsonValue(String column, String value) {
    if (value == null) return "null";
    // IoTDB exposes timestamps as decimal values; keep them strings for parity
    // with TsFile-Cli's INT64/TIMESTAMP JSON representation.
    if ("time".equalsIgnoreCase(column)) return jsonString(value);
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))
      return value.toLowerCase();
    if (value.matches("-?(?:0|[1-9]\\d*)")
        || value.matches("-?(?:0|[1-9]\\d*)\\.[0-9]+(?:[eE][+-]?[0-9]+)?")
        || value.matches("-?(?:0|[1-9]\\d*)(?:[eE][+-]?[0-9]+)")) return value;
    return jsonString(value);
  }

  private static List<SqlRow> projectColumns(List<SqlRow> rows, List<String> columns) {
    if (columns == null || columns.isEmpty()) return rows;
    List<SqlRow> projected = new ArrayList<>();
    for (SqlRow row : rows) {
      Map<String, String> values = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : row.asMap().entrySet()) {
        if ("time".equalsIgnoreCase(entry.getKey())
            || columns.stream().anyMatch(c -> c.equalsIgnoreCase(entry.getKey()))) {
          values.put(entry.getKey(), entry.getValue());
        }
      }
      projected.add(new SqlRow(values));
    }
    return projected;
  }

  private static boolean matchesTagFilters(SqlRow row, ReadOptions options) {
    if (options.getTagFilters().isEmpty()) return true;
    boolean any = "any".equalsIgnoreCase(options.getTagMatch());
    boolean matched = !any;
    for (String spec : options.getTagFilters()) {
      String[] parts = spec.split("\\s+", 3);
      if (parts.length < 2) continue;
      String actual = valueIgnoreCase(row, parts[0]);
      String op = parts[1].toLowerCase();
      String expected = parts.length == 3 ? parts[2] : null;
      boolean current;
      switch (op) {
        case "eq":
          current = actual != null && actual.equals(expected);
          break;
        case "neq":
          current = actual == null || !actual.equals(expected);
          break;
        case "is-null":
          current = actual == null;
          break;
        case "not-null":
          current = actual != null;
          break;
        case "regexp":
          try {
            current = actual != null && Pattern.matches(expected == null ? "" : expected, actual);
          } catch (RuntimeException e) {
            current = false;
          }
          break;
        default:
          current = false;
      }
      if (any) matched |= current;
      else matched &= current;
    }
    return matched;
  }

  private static int readLimit(ReadOptions options) {
    if (options.getLimit() < 0) return -1;
    if (options.getStart() != null
        || options.getEnd() != null
        || !options.getTagFilters().isEmpty()) return -1;
    long requested =
        options.getOffset() > Integer.MAX_VALUE - options.getLimit()
            ? Integer.MAX_VALUE
            : options.getLimit() + options.getOffset();
    return requested > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requested;
  }

  private static String valueIgnoreCase(SqlRow row, String column) {
    String value = row.get(column);
    if (value != null) return value;
    for (Map.Entry<String, String> entry : row.asMap().entrySet()) {
      if (column.equalsIgnoreCase(entry.getKey())) return entry.getValue();
    }
    return null;
  }

  private void printMatchingRows(String path, String pattern) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      for (String line : provider.readLines(resolvedPath, DEFAULT_READ_LIMIT)) {
        if (line.contains(pattern)) {
          ctx.getPrinter().println(line);
        }
      }
      return;
    }
    for (SqlRow row : provider.read(resolvedPath, DEFAULT_READ_LIMIT)) {
      String line = joinValues(row);
      if (line.contains(pattern)) {
        ctx.getPrinter().println(line);
      }
    }
  }

  private void printCut(String path, String delimiter, String fields) throws SQLException {
    FsPath resolvedPath = resolve(path);
    for (String line : readableLines(resolvedPath, DEFAULT_READ_LIMIT)) {
      ctx.getPrinter().println(cutLine(line, delimiter, fields));
    }
  }

  private void printPaste(List<String> paths) throws SQLException {
    List<List<String>> files = new ArrayList<>();
    int maxLines = 0;
    for (String path : paths) {
      List<String> lines = readableLines(resolve(path), DEFAULT_READ_LIMIT);
      files.add(lines);
      maxLines = Math.max(maxLines, lines.size());
    }
    for (int i = 0; i < maxLines; i++) {
      ctx.getPrinter().println(pasteLine(files, i));
    }
  }

  private void printJoin(List<String> paths, String delimiter, String fields) throws SQLException {
    int[] joinFields = joinFields(fields);
    List<String> leftLines = readableLines(resolve(paths.get(0)), DEFAULT_READ_LIMIT);
    List<String> rightLines = readableLines(resolve(paths.get(1)), DEFAULT_READ_LIMIT);
    Map<String, List<String[]>> rightRows = joinRowsByKey(rightLines, delimiter, joinFields[1]);

    for (String leftLine : leftLines) {
      String[] left = splitJoinFields(leftLine, delimiter);
      if (!hasField(left, joinFields[0])) {
        continue;
      }
      List<String[]> matches = rightRows.get(left[joinFields[0] - 1]);
      if (matches == null) {
        continue;
      }
      for (String[] right : matches) {
        ctx.getPrinter().println(joinLine(left, right, joinFields[0], joinFields[1], delimiter));
      }
    }
  }

  private List<String> readableLines(FsPath path, int limit) throws SQLException {
    if (isTextFile(path)) {
      return provider.readLines(path, limit);
    }
    List<String> lines = new ArrayList<>();
    for (SqlRow row : provider.read(path, limit)) {
      lines.add(joinValues(row));
    }
    return lines;
  }

  private void printFind(FsPath path, String pattern) throws SQLException {
    FsNode node = provider.describe(path);
    if (!checkExists("find", node)) {
      return;
    }
    if (matchesFind(node, pattern)) {
      ctx.getPrinter().println(path.toString());
    }
    if (!isDirectory(node.getType())) {
      return;
    }
    for (FsNode child : provider.list(path)) {
      printFind(child.getPath(), pattern);
    }
  }

  private static boolean matchesFind(FsNode node, String pattern) {
    return pattern == null || pattern.isEmpty() || node.getName().equals(pattern);
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

  private void move(List<String> paths) throws SQLException {
    FsPath source = resolve(paths.get(0));
    FsPath target = resolve(paths.get(1));
    if (!ensureWritable("mv", source)) {
      return;
    }
    mutationProvider.move(source, target);
  }

  private void copy(List<String> paths) throws SQLException {
    FsPath source = resolve(paths.get(0));
    FsPath target = resolve(paths.get(1));
    if (!ensureWritable("cp", source)) {
      return;
    }
    mutationProvider.copy(source, target);
  }

  private void append(String path, boolean nonInteractive) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("tee", resolvedPath)) {
      return;
    }
    if (nonInteractive || ctx.getLineReader() == null) {
      mutationProvider.append(resolvedPath, readStandardInputLines());
      return;
    }
    appendInteractive(resolvedPath);
  }

  private List<String> readStandardInputLines() throws SQLException {
    List<String> lines = new ArrayList<>();
    try {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(ctx.getIn(), StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
      return lines;
    } catch (IOException e) {
      throw new SQLException(CliMessages.MESSAGE_FAILED_TO_READ_STANDARD_INPUT_3CB0AD1E, e);
    }
  }

  private void appendInteractive(FsPath path) throws SQLException {
    List<String> lines = new ArrayList<>();
    while (true) {
      String line;
      try {
        line = ctx.getLineReader().readLine("tee> ", null);
      } catch (EndOfFileException e) {
        if (!lines.isEmpty()) {
          ctx.getErr()
              .println(
                  CliMessages.MESSAGE_TEE_USE_WQ_TO_WRITE_OR_Q_TO_QUIT_WITHOUT_WRITING_C46EFD2C);
        }
        return;
      }
      if (":wq".equals(line)) {
        try {
          mutationProvider.append(path, lines);
          return;
        } catch (SQLException e) {
          ctx.getErr().println("tee: " + e.getMessage());
          continue;
        }
      }
      if (":q!".equals(line)) {
        return;
      }
      if (":q".equals(line)) {
        if (lines.isEmpty()) {
          return;
        }
        ctx.getErr()
            .println(CliMessages.MESSAGE_TEE_USE_WQ_TO_WRITE_OR_Q_TO_QUIT_WITHOUT_WRITING_C46EFD2C);
        continue;
      }
      lines.add(line);
    }
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

  private static String joinValues(SqlRow row) {
    StringBuilder builder = new StringBuilder();
    for (String value : row.asMap().values()) {
      if (builder.length() > 0) {
        builder.append('\t');
      }
      if (value != null) {
        builder.append(value);
      }
    }
    return builder.toString();
  }

  private static boolean isDirectory(FsNodeType type) {
    return type == FsNodeType.VIRTUAL_ROOT
        || type == FsNodeType.TREE_ROOT
        || type == FsNodeType.TREE_DATABASE
        || type == FsNodeType.TREE_INTERNAL_PATH
        || type == FsNodeType.TREE_DEVICE
        || type == FsNodeType.TABLE_DATABASE;
  }

  private static String longMode(FsNodeType type) {
    if (isDirectory(type)) {
      return "dr-xr-xr-x";
    }
    return "-r--r--r--";
  }

  private static String unixType(FsNodeType type) {
    if (isDirectory(type)) {
      return "directory";
    }
    if (type == FsNodeType.UNKNOWN) {
      return "unknown";
    }
    return "regular file";
  }

  private static boolean isAllOption(FilesystemCommand command) {
    return "-a".equals(command.getOption());
  }

  private static String cutLine(String line, String delimiter, String fields) {
    if (!line.contains(delimiter)) {
      return line;
    }
    String[] values = line.split(Pattern.quote(delimiter), -1);
    boolean[] selected = selectedFields(fields, values.length);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (!selected[i]) {
        continue;
      }
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(values[i]);
    }
    return builder.toString();
  }

  private static String pasteLine(List<List<String>> files, int lineIndex) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < files.size(); i++) {
      if (i > 0) {
        builder.append('\t');
      }
      List<String> lines = files.get(i);
      if (lineIndex < lines.size()) {
        builder.append(lines.get(lineIndex));
      }
    }
    return builder.toString();
  }

  private static int[] joinFields(String fields) {
    String[] values = fields.split(",", -1);
    return new int[] {parsePositiveInt(values[0]), parsePositiveInt(values[1])};
  }

  private static Map<String, List<String[]>> joinRowsByKey(
      List<String> lines, String delimiter, int keyField) {
    Map<String, List<String[]>> rowsByKey = new LinkedHashMap<>();
    for (String line : lines) {
      String[] fields = splitJoinFields(line, delimiter);
      if (!hasField(fields, keyField)) {
        continue;
      }
      String key = fields[keyField - 1];
      rowsByKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(fields);
    }
    return rowsByKey;
  }

  private static String[] splitJoinFields(String line, String delimiter) {
    if (delimiter.isEmpty()) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        return new String[0];
      }
      return trimmed.split("\\s+");
    }
    return line.split(Pattern.quote(delimiter), -1);
  }

  private static boolean hasField(String[] fields, int fieldNumber) {
    return fieldNumber > 0 && fieldNumber <= fields.length;
  }

  private static String joinLine(
      String[] left, String[] right, int leftKeyField, int rightKeyField, String delimiter) {
    String outputDelimiter = delimiter.isEmpty() ? " " : delimiter;
    List<String> output = new ArrayList<>();
    output.add(left[leftKeyField - 1]);
    addNonKeyFields(output, left, leftKeyField);
    addNonKeyFields(output, right, rightKeyField);
    return String.join(outputDelimiter, output);
  }

  private static void addNonKeyFields(List<String> output, String[] fields, int keyField) {
    for (int i = 0; i < fields.length; i++) {
      if (i != keyField - 1) {
        output.add(fields[i]);
      }
    }
  }

  private static boolean[] selectedFields(String fields, int fieldCount) {
    boolean[] selected = new boolean[fieldCount];
    for (String field : fields.split(",")) {
      selectField(field.trim(), selected);
    }
    return selected;
  }

  private static void selectField(String field, boolean[] selected) {
    if (field.isEmpty()) {
      return;
    }
    int dash = field.indexOf('-');
    if (dash < 0) {
      selectFieldNumber(field, selected);
      return;
    }
    int start = parsePositiveInt(field.substring(0, dash));
    int end = parsePositiveInt(field.substring(dash + 1));
    if (start <= 0 || end <= 0 || start > end) {
      return;
    }
    for (int i = start; i <= end && i <= selected.length; i++) {
      selected[i - 1] = true;
    }
  }

  private static void selectFieldNumber(String field, boolean[] selected) {
    int fieldNumber = parsePositiveInt(field);
    if (fieldNumber > 0 && fieldNumber <= selected.length) {
      selected[fieldNumber - 1] = true;
    }
  }

  private static int parsePositiveInt(String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static boolean isTextFile(FsPath path) {
    String fileName = path.getFileName();
    return fileName.endsWith(".csv") || fileName.endsWith(".meta");
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
