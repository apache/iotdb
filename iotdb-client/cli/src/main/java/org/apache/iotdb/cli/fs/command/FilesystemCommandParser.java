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

package org.apache.iotdb.cli.fs.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class FilesystemCommandParser {

  private static final String DEFAULT_PATH = ".";
  private static final int DEFAULT_TREE_DEPTH = Integer.MAX_VALUE;

  private FilesystemCommandParser() {}

  public static FilesystemCommand parse(String input) {
    String line = input == null ? "" : input.trim();
    if (line.isEmpty()) {
      return FilesystemCommand.invalid("Empty command");
    }

    String lowerLine = line.toLowerCase(Locale.ROOT);
    if (lowerLine.equals("pwd")) {
      return FilesystemCommand.simple(FilesystemCommand.Type.PWD);
    }
    if (lowerLine.equals("help")) {
      return FilesystemCommand.simple(FilesystemCommand.Type.HELP);
    }
    if (lowerLine.equals("exit") || lowerLine.equals("quit")) {
      return FilesystemCommand.simple(FilesystemCommand.Type.EXIT);
    }
    if (lowerLine.startsWith("sql ")) {
      return parseSql(line);
    }

    String[] tokens = line.split("\\s+");
    String command = tokens[0].toLowerCase(Locale.ROOT);
    if ("ls".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.LS, pathArgument(tokens));
    }
    if ("ll".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.LL, pathArgument(tokens));
    }
    if ("cd".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.CD, pathArgument(tokens));
    }
    if ("stat".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.STAT, pathArgument(tokens));
    }
    if ("cat".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.CAT, pathArgument(tokens));
    }
    if ("paste".equals(command)) {
      return parsePaste(tokens);
    }
    if ("tree".equals(command)) {
      return parseTree(tokens);
    }
    return FilesystemCommand.invalid("Unknown command: " + tokens[0]);
  }

  private static FilesystemCommand parseSql(String line) {
    String statement = line.substring(3).trim();
    if (statement.isEmpty()) {
      return FilesystemCommand.invalid("SQL statement is empty");
    }
    return FilesystemCommand.sql(statement);
  }

  private static FilesystemCommand parsePaste(String[] tokens) {
    if (tokens.length < 2) {
      return FilesystemCommand.invalid("Missing paste path");
    }
    List<String> paths = new ArrayList<>();
    for (int i = 1; i < tokens.length; i++) {
      paths.add(tokens[i]);
    }
    return FilesystemCommand.paths(FilesystemCommand.Type.PASTE, paths);
  }

  private static FilesystemCommand parseTree(String[] tokens) {
    String path = DEFAULT_PATH;
    int depth = DEFAULT_TREE_DEPTH;

    for (int i = 1; i < tokens.length; i++) {
      if ("-L".equals(tokens[i])) {
        if (i + 1 >= tokens.length) {
          return FilesystemCommand.invalid("Missing tree depth");
        }
        try {
          depth = Integer.parseInt(tokens[++i]);
        } catch (NumberFormatException e) {
          return FilesystemCommand.invalid("Invalid tree depth: " + tokens[i]);
        }
        if (depth < 0) {
          return FilesystemCommand.invalid("Invalid tree depth: " + depth);
        }
      } else {
        path = tokens[i];
      }
    }
    return FilesystemCommand.tree(path, depth);
  }

  private static String pathArgument(String[] tokens) {
    return tokens.length > 1 ? tokens[1] : DEFAULT_PATH;
  }
}
