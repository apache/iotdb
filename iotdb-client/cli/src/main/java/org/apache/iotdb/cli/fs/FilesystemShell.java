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
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlRow;
import org.apache.iotdb.cli.utils.CliContext;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilesystemShell {

  private static final int DEFAULT_READ_LIMIT = 20;
  private static final List<String> COMMANDS =
      Arrays.asList(
          "pwd", "ls", "ll", "cd", "stat", "cat", "paste", "tree", "help", "exit", "quit");

  private final CliContext ctx;
  private final FilesystemSchemaProvider provider;
  private FsPath currentPath = FsPath.absolute("/");

  public FilesystemShell(CliContext ctx, FilesystemSchemaProvider provider) {
    this.ctx = ctx;
    this.provider = provider;
  }

  public boolean execute(String input) throws SQLException {
    FilesystemCommand command = FilesystemCommandParser.parse(input);
    switch (command.getType()) {
      case PWD:
        ctx.getPrinter().println(currentPath.toString());
        return true;
      case LS:
        printNodes(provider.list(resolve(command.getPath())));
        return true;
      case LL:
        printLongNodes(provider.list(resolve(command.getPath())));
        return true;
      case CD:
        changeDirectory(command.getPath());
        return true;
      case STAT:
        printNode(provider.describe(resolve(command.getPath())));
        return true;
      case CAT:
        printRows(provider.read(resolve(command.getPath()), DEFAULT_READ_LIMIT));
        return true;
      case PASTE:
        printRows(provider.read(resolve(command.getPaths()), DEFAULT_READ_LIMIT));
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

  public Completer createCompleter() {
    return new FilesystemCompleter();
  }

  private void printTree(FsPath path, int depth) throws SQLException {
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

  private void printNodes(List<FsNode> nodes) {
    for (FsNode node : nodes) {
      ctx.getPrinter().println(node.getName());
    }
  }

  private void printLongNodes(List<FsNode> nodes) {
    for (FsNode node : nodes) {
      ctx.getPrinter().println(longMode(node.getType()) + "  1 iotdb iotdb 0 " + node.getName());
    }
  }

  private void printNode(FsNode node) {
    ctx.getPrinter().println(node.getName() + "\t" + node.getType());
    for (Map.Entry<String, String> entry : node.getMetadata().entrySet()) {
      ctx.getPrinter().println(entry.getKey() + "\t" + entry.getValue());
    }
  }

  private void printRows(List<SqlRow> rows) {
    for (SqlRow row : rows) {
      ctx.getPrinter().println(joinValues(row));
    }
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
    ctx.getPrinter().println("cat <path>");
    ctx.getPrinter().println("paste <path>...");
    ctx.getPrinter().println("exit");
  }

  private static boolean isDirectory(FsNodeType type) {
    return type == FsNodeType.VIRTUAL_ROOT
        || type == FsNodeType.TREE_ROOT
        || type == FsNodeType.TREE_DATABASE
        || type == FsNodeType.TREE_INTERNAL_PATH
        || type == FsNodeType.TREE_DEVICE
        || type == FsNodeType.TABLE_DATABASE
        || type == FsNodeType.TABLE_TABLE
        || type == FsNodeType.TABLE_VIEW;
  }

  private static String longMode(FsNodeType type) {
    if (isDirectory(type)) {
      return "dr-xr-xr-x";
    }
    return "-r--r--r--";
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
