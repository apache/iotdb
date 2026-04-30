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
import org.apache.iotdb.cli.fs.command.FilesystemCommandParser;
import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemMutationProvider;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.provider.UnsupportedFilesystemMutationProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FilesystemShell {

  private static final int DEFAULT_READ_LIMIT = 20;
  private static final List<String> COMMANDS =
      Arrays.asList(
          "pwd", "ls", "ll", "cd", "stat", "cat", "head", "tail", "wc", "grep", "find", "less",
          "more", "file", "du", "mkdir", "rm", "mv", "cut", "paste", "tree", "help", "exit", "quit",
          "tee");

  private final CliContext ctx;
  private final FilesystemSchemaProvider provider;
  private final FilesystemMutationProvider mutationProvider;
  private final boolean writeEnabled;
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
    FilesystemCommand command = FilesystemCommandParser.parse(input);
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
        printNode(provider.describe(resolve(command.getPath())));
        return true;
      case CAT:
        printSequentialReads(command.getPaths(), DEFAULT_READ_LIMIT);
        return true;
      case HEAD:
        printHead(command.getPath(), command.getLimit());
        return true;
      case TAIL:
        printTail(command.getPath(), command.getLimit());
        return true;
      case WC:
        printLineCount(command.getPath());
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
      case DU:
        printDiskUsage(command.getPath());
        return true;
      case MKDIR:
        mkdir(command.getPath());
        return true;
      case RM:
        remove(command.getPath());
        return true;
      case MV:
        move(command.getPaths());
        return true;
      case CUT:
        printCut(command.getPath(), command.getOption(), command.getPattern());
        return true;
      case PASTE:
        printPaste(command.getPaths());
        return true;
      case TEE:
        append(command.getPath(), false);
        return true;
      case HELP:
        printHelp();
        return true;
      case EXIT:
        return false;
      case TREE:
        printTree(resolve(command.getPath()), command.getDepth());
        return true;
      case INVALID:
        ctx.getPrinter().println(command.getErrorMessage());
        return true;
      case SQL:
      default:
        ctx.getPrinter().println("Unsupported filesystem command: " + command.getType());
        return true;
    }
  }

  public boolean executeNonInteractive(String input) throws SQLException {
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    if (command.getType() == FilesystemCommand.Type.TEE) {
      append(command.getPath(), true);
      return true;
    }
    return execute(input);
  }

  public Completer createCompleter() {
    return new FilesystemCompleter();
  }

  private void printTree(FsPath path, int depth) throws SQLException {
    FsNode node = provider.describe(path);
    if (node.getType() == FsNodeType.UNKNOWN) {
      ctx.getPrinter().println("tree: " + path + ": No such file or directory");
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
    if (isDirectory(node.getType())) {
      currentPath = target;
    } else {
      ctx.getPrinter().println("cd: " + target + ": Not a directory");
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
    if (node.getType() == FsNodeType.UNKNOWN) {
      ctx.getPrinter().println("ls: " + resolvedPath + ": No such file or directory");
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

  private void printLines(List<String> lines) {
    for (String line : lines) {
      ctx.getPrinter().println(line);
    }
  }

  private void printSequentialReads(List<String> paths, int limit) throws SQLException {
    for (String path : paths) {
      printReadable(path, limit);
    }
  }

  private void printReadable(String path, int limit) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      printLines(provider.readLines(resolvedPath, limit));
      return;
    }
    printRows(provider.read(resolvedPath, limit));
  }

  private void printHead(String path, int limit) throws SQLException {
    printReadable(path, limit);
  }

  private void printTail(String path, int limit) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (isTextFile(resolvedPath)) {
      printLines(provider.tailLines(resolvedPath, limit));
      return;
    }
    printRows(provider.tail(resolvedPath, limit));
  }

  private void printLineCount(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    ctx.getPrinter().println(provider.count(resolvedPath) + " " + resolvedPath);
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
    ctx.getPrinter()
        .println(resolvedPath + ": " + unixType(provider.describe(resolvedPath).getType()));
  }

  private void printDiskUsage(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    ctx.getPrinter().println(provider.count(resolvedPath) + "\t" + resolvedPath);
  }

  private void mkdir(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("mkdir", resolvedPath)) {
      return;
    }
    mutationProvider.mkdir(resolvedPath);
  }

  private void remove(String path) throws SQLException {
    FsPath resolvedPath = resolve(path);
    if (!ensureWritable("rm", resolvedPath)) {
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
      throw new SQLException("Failed to read standard input", e);
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
          ctx.getPrinter().println("tee: use :wq to write or :q! to quit without writing");
        }
        return;
      }
      if (":wq".equals(line)) {
        try {
          mutationProvider.append(path, lines);
          return;
        } catch (SQLException e) {
          ctx.getPrinter().println("tee: " + e.getMessage());
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
        ctx.getPrinter().println("tee: use :wq to write or :q! to quit without writing");
        continue;
      }
      lines.add(line);
    }
  }

  private boolean ensureWritable(String command, FsPath path) {
    if (writeEnabled) {
      return true;
    }
    ctx.getPrinter().println(command + ": " + path + ": Read-only file system");
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

  private void printHelp() {
    ctx.getPrinter().println("pwd");
    ctx.getPrinter().println("ls [path]");
    ctx.getPrinter().println("ll [path]");
    ctx.getPrinter().println("cd <path>");
    ctx.getPrinter().println("stat [path]");
    ctx.getPrinter().println("cat <path>...");
    ctx.getPrinter().println("head [-n lines] <path>");
    ctx.getPrinter().println("tail [-n lines] <path>");
    ctx.getPrinter().println("wc -l <path>");
    ctx.getPrinter().println("grep <pattern> <path>");
    ctx.getPrinter().println("find [path] [-name pattern]");
    ctx.getPrinter().println("less <path>");
    ctx.getPrinter().println("more <path>");
    ctx.getPrinter().println("file <path>");
    ctx.getPrinter().println("du <path>");
    ctx.getPrinter().println("mkdir <path>");
    ctx.getPrinter().println("rm <path>");
    ctx.getPrinter().println("mv <source> <target>");
    ctx.getPrinter().println("cut -d<delimiter> -f<fields> <path>");
    ctx.getPrinter().println("paste <path>...");
    ctx.getPrinter().println("tee -a <path>");
    ctx.getPrinter().println("tree [-L depth] [path]");
    ctx.getPrinter().println("exit");
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
    return fileName.endsWith(".csv") || fileName.endsWith(".schema") || fileName.endsWith(".meta");
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
