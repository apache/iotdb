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

package org.apache.iotdb.postgresql.session;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a simple-query string into statements at semicolons outside quotes, dollar quotes and
 * comments, like PostgreSQL. Empty statements are kept as empty strings (EmptyQueryResponse).
 */
public final class StatementSplitter {

  private StatementSplitter() {}

  public static List<String> split(String sql) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (c == '\'' || c == '"') {
        int end = closing(sql, i + 1, c);
        current.append(sql, i, end);
        i = end;
      } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        int end = sql.indexOf('\n', i);
        end = end < 0 ? n : end;
        current.append(sql, i, end);
        i = end;
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        int end = sql.indexOf("*/", i + 2);
        end = end < 0 ? n : end + 2;
        current.append(sql, i, end);
        i = end;
      } else if (c == '$' && dollarTag(sql, i) != null) {
        String tag = dollarTag(sql, i);
        int end = sql.indexOf(tag, i + tag.length());
        end = end < 0 ? n : end + tag.length();
        current.append(sql, i, end);
        i = end;
      } else if (c == ';') {
        statements.add(current.toString().trim());
        current.setLength(0);
        i++;
      } else {
        current.append(c);
        i++;
      }
    }
    String last = current.toString().trim();
    if (!last.isEmpty() || statements.isEmpty()) {
      statements.add(last);
    }
    return statements;
  }

  /** The end (exclusive) of a quoted part starting after the opening quote; '' and "" escape. */
  private static int closing(String sql, int from, char quote) {
    int i = from;
    while (i < sql.length()) {
      if (sql.charAt(i) == quote) {
        if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return sql.length();
  }

  /** {@code $tag$} starting at {@code i}, or null. */
  private static String dollarTag(String sql, int i) {
    int end = i + 1;
    while (end < sql.length()
        && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_')) {
      end++;
    }
    if (end < sql.length() && sql.charAt(end) == '$') {
      return sql.substring(i, end + 1);
    }
    return null;
  }
}
