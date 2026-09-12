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

import org.apache.iotdb.cli.i18n.CliMessages;

import java.util.ArrayList;
import java.util.List;

/** Decodes shell quotes while retaining which pathname characters may expand. */
public final class FsShellWords {
  private FsShellWords() {}

  public static List<Word> parse(String input) {
    List<Word> words = new ArrayList<>();
    StringBuilder value = new StringBuilder();
    StringBuilder glob = new StringBuilder();
    char quote = 0;
    boolean started = false;
    boolean wildcard = false;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (quote == 0 && Character.isWhitespace(c)) {
        if (started) words.add(new Word(value.toString(), wildcard ? glob.toString() : null));
        value.setLength(0);
        glob.setLength(0);
        started = false;
        wildcard = false;
        continue;
      }
      started = true;
      if (c == quote) {
        quote = 0;
        continue;
      }
      if (quote == 0 && (c == '\'' || c == '"')) {
        quote = c;
        continue;
      }
      boolean literal = quote != 0;
      if (c == '\\' && quote != '\'') {
        if (i + 1 == input.length()) throw invalid();
        char next = input.charAt(i + 1);
        // In double quotes, backslash only quotes the shell's special characters.
        if (quote == 0
            || next == '$'
            || next == '`'
            || next == '"'
            || next == '\\'
            || next == '\n') {
          c = next;
          i++;
          if (c == '\n') continue;
          literal = true;
        }
      }
      value.append(c);
      if (c == '\\' || (literal && "*?[]!^-".indexOf(c) >= 0)) glob.append('\\');
      glob.append(c);
      if (!literal && (c == '*' || c == '?' || c == '[')) wildcard = true;
    }
    if (quote != 0) throw invalid();
    if (started) words.add(new Word(value.toString(), wildcard ? glob.toString() : null));
    return words;
  }

  public static String literalGlob(String value) {
    StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if ("\\*?[]!^-".indexOf(c) >= 0) escaped.append('\\');
      escaped.append(c);
    }
    return escaped.toString();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException(
        CliMessages.MESSAGE_UNCLOSED_QUOTE_OR_ESCAPE_IN_FILESYSTEM_COMMAND_42C74084);
  }

  public static final class Word {
    private final String value;
    private final String globPattern;

    private Word(String value, String globPattern) {
      this.value = value;
      this.globPattern = globPattern;
    }

    public String getValue() {
      return value;
    }

    public String getGlobPattern() {
      return globPattern;
    }
  }
}
