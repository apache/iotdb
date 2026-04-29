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
  private static final int DEFAULT_HEAD_LIMIT = 10;

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
      return parseLs(tokens);
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
      return parseCat(tokens);
    }
    if ("head".equals(command)) {
      return parseHead(tokens);
    }
    if ("tail".equals(command)) {
      return parseTail(tokens);
    }
    if ("wc".equals(command)) {
      return parseWc(tokens);
    }
    if ("grep".equals(command)) {
      return parseGrep(tokens);
    }
    if ("find".equals(command)) {
      return parseFind(tokens);
    }
    if ("less".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.LESS, pathArgument(tokens));
    }
    if ("more".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.MORE, pathArgument(tokens));
    }
    if ("file".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.FILE, pathArgument(tokens));
    }
    if ("du".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.DU, pathArgument(tokens));
    }
    if ("mkdir".equals(command)) {
      return FilesystemCommand.path(FilesystemCommand.Type.MKDIR, pathArgument(tokens));
    }
    if ("rm".equals(command)) {
      return parseRm(tokens);
    }
    if ("mv".equals(command)) {
      return parseMv(tokens);
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

  private static FilesystemCommand parseRm(String[] tokens) {
    if (tokens.length < 2) {
      return FilesystemCommand.invalid("Missing rm path");
    }
    if (tokens[1].startsWith("-")) {
      return FilesystemCommand.invalid("Unsupported rm option: " + tokens[1]);
    }
    return FilesystemCommand.path(FilesystemCommand.Type.RM, tokens[1]);
  }

  private static FilesystemCommand parseMv(String[] tokens) {
    if (tokens.length < 3) {
      return FilesystemCommand.invalid("Usage: mv <source> <target>");
    }
    List<String> paths = new ArrayList<>();
    paths.add(tokens[1]);
    paths.add(tokens[2]);
    return FilesystemCommand.paths(FilesystemCommand.Type.MV, paths);
  }

  private static FilesystemCommand parseLs(String[] tokens) {
    FilesystemCommand.Type type = FilesystemCommand.Type.LS;
    String path = DEFAULT_PATH;

    for (int i = 1; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.startsWith("-")) {
        for (int j = 1; j < token.length(); j++) {
          char option = token.charAt(j);
          if (option == 'l') {
            type = FilesystemCommand.Type.LL;
          } else if (option != 'a') {
            return FilesystemCommand.invalid("Unsupported ls option: -" + option);
          }
        }
      } else {
        path = token;
      }
    }
    return FilesystemCommand.path(type, path);
  }

  private static FilesystemCommand parseCat(String[] tokens) {
    if (tokens.length < 2) {
      return FilesystemCommand.path(FilesystemCommand.Type.CAT, DEFAULT_PATH);
    }
    List<String> paths = new ArrayList<>();
    for (int i = 1; i < tokens.length; i++) {
      paths.add(tokens[i]);
    }
    return FilesystemCommand.paths(FilesystemCommand.Type.CAT, paths);
  }

  private static FilesystemCommand parseHead(String[] tokens) {
    String path = DEFAULT_PATH;
    int limit = DEFAULT_HEAD_LIMIT;

    for (int i = 1; i < tokens.length; i++) {
      String token = tokens[i];
      if ("-n".equals(token)) {
        if (i + 1 >= tokens.length) {
          return FilesystemCommand.invalid("Missing head line count");
        }
        try {
          limit = Integer.parseInt(tokens[++i]);
        } catch (NumberFormatException e) {
          return FilesystemCommand.invalid("Invalid head line count: " + tokens[i]);
        }
      } else if (token.startsWith("-") && token.length() > 1) {
        try {
          limit = Integer.parseInt(token.substring(1));
        } catch (NumberFormatException e) {
          return FilesystemCommand.invalid("Unsupported head option: " + token);
        }
      } else {
        path = token;
      }
    }

    if (limit < 0) {
      return FilesystemCommand.invalid("Invalid head line count: " + limit);
    }
    return FilesystemCommand.head(path, limit);
  }

  private static FilesystemCommand parseTail(String[] tokens) {
    String path = DEFAULT_PATH;
    int limit = DEFAULT_HEAD_LIMIT;

    for (int i = 1; i < tokens.length; i++) {
      String token = tokens[i];
      if ("-n".equals(token)) {
        if (i + 1 >= tokens.length) {
          return FilesystemCommand.invalid("Missing tail line count");
        }
        try {
          limit = Integer.parseInt(tokens[++i]);
        } catch (NumberFormatException e) {
          return FilesystemCommand.invalid("Invalid tail line count: " + tokens[i]);
        }
      } else if (token.startsWith("-") && token.length() > 1) {
        try {
          limit = Integer.parseInt(token.substring(1));
        } catch (NumberFormatException e) {
          return FilesystemCommand.invalid("Unsupported tail option: " + token);
        }
      } else {
        path = token;
      }
    }

    if (limit < 0) {
      return FilesystemCommand.invalid("Invalid tail line count: " + limit);
    }
    return FilesystemCommand.tail(path, limit);
  }

  private static FilesystemCommand parseWc(String[] tokens) {
    String option = "-l";
    String path = DEFAULT_PATH;

    for (int i = 1; i < tokens.length; i++) {
      String token = tokens[i];
      if (token.startsWith("-")) {
        if (!"-l".equals(token)) {
          return FilesystemCommand.invalid("Unsupported wc option: " + token);
        }
        option = token;
      } else {
        path = token;
      }
    }
    return FilesystemCommand.option(FilesystemCommand.Type.WC, option, path);
  }

  private static FilesystemCommand parseGrep(String[] tokens) {
    if (tokens.length < 3) {
      return FilesystemCommand.invalid("Usage: grep <pattern> <path>");
    }
    return FilesystemCommand.pattern(FilesystemCommand.Type.GREP, tokens[1], tokens[2]);
  }

  private static FilesystemCommand parseFind(String[] tokens) {
    String path = tokens.length > 1 ? tokens[1] : DEFAULT_PATH;
    String pattern = "";

    for (int i = 2; i < tokens.length; i++) {
      if ("-name".equals(tokens[i])) {
        if (i + 1 >= tokens.length) {
          return FilesystemCommand.invalid("Missing find name pattern");
        }
        pattern = tokens[++i];
      } else {
        return FilesystemCommand.invalid("Unsupported find option: " + tokens[i]);
      }
    }
    return FilesystemCommand.pattern(FilesystemCommand.Type.FIND, pattern, path);
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
