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

import org.apache.iotdb.cli.fs.command.FsShellWords;
import org.apache.iotdb.cli.i18n.CliMessages;

import java.util.ArrayList;
import java.util.List;

/** Recognizes shell operators outside quotes and decodes local redirection targets. */
final class FsCommandLine {
  final List<String> commands = new ArrayList<>();
  String input;
  String output;
  boolean append;

  static FsCommandLine parse(String text) {
    text = text == null ? "" : text;
    FsCommandLine result = new FsCommandLine();
    if (text.trim().matches("(?is)^sql\\s+.*")) {
      result.commands.add(text);
      return result;
    }
    List<String> pieces = new ArrayList<>();
    List<String> operators = new ArrayList<>();
    char quote = 0;
    boolean escaped = false;
    int start = 0;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (escaped) {
        escaped = false;
        continue;
      }
      if (c == '\\' && quote != '\'') {
        escaped = true;
        continue;
      }
      if (quote != 0) {
        if (c == quote) quote = 0;
        continue;
      }
      if (c == '\'' || c == '"') {
        quote = c;
        continue;
      }
      if (c == '|' || c == '<' || c == '>') {
        pieces.add(text.substring(start, i).trim());
        String operator = Character.toString(c);
        if (c == '>' && i + 1 < text.length() && text.charAt(i + 1) == '>') {
          operator = ">>";
          i++;
        }
        operators.add(operator);
        start = i + 1;
      }
    }
    pieces.add(text.substring(start).trim());
    if (operators.isEmpty()) {
      result.commands.add(text);
      return result;
    }
    String command = pieces.get(0);
    for (int i = 0; i < operators.size(); i++) {
      String operator = operators.get(i);
      String next = pieces.get(i + 1);
      if (command.isEmpty() || next.isEmpty()) throw invalid();
      if ("|".equals(operator)) {
        if (result.output != null) throw invalid();
        result.commands.add(command);
        command = next;
      } else {
        List<FsShellWords.Word> words;
        try {
          words = FsShellWords.parse(next);
        } catch (IllegalArgumentException e) {
          throw invalid();
        }
        if (words.size() != 1 || words.get(0).getValue().isEmpty()) throw invalid();
        if ("<".equals(operator)) {
          if (result.input != null || !result.commands.isEmpty()) throw invalid();
          result.input = words.get(0).getValue();
        } else {
          if (result.output != null) throw invalid();
          result.output = words.get(0).getValue();
          result.append = ">>".equals(operator);
        }
      }
    }
    result.commands.add(command);
    return result;
  }

  boolean compound() {
    return commands.size() > 1 || input != null || output != null;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException(CliMessages.FS_PIPE_SYNTAX);
  }
}
