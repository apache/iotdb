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

package org.apache.iotdb.postgresql.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Textual rewrites before parsing, for PostgreSQL syntax JSqlParser does not read and that has a
 * plain equivalent: {@code OPERATOR(pg_catalog.op)} is the operator {@code op} (psql qualifies its
 * operators), and {@code COLLATE pg_catalog.default} is the default collation, which changes
 * nothing. Only text outside literals, quoted identifiers and comments is touched.
 */
final class PreParse {

  private static final Pattern OPERATOR =
      Pattern.compile(
          "OPERATOR\\s*\\(\\s*(?:pg_catalog\\s*\\.\\s*)?([~!<>=*+\\-/%^@#&|]+)\\s*\\)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern DEFAULT_COLLATION =
      Pattern.compile(
          "\\s+COLLATE\\s+(?:pg_catalog\\s*\\.\\s*)?(?:default|\"default\")\\b",
          Pattern.CASE_INSENSITIVE);

  private PreParse() {}

  static String rewrite(String sql) {
    StringBuilder result = new StringBuilder(sql.length());
    StringBuilder code = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      int end = -1;
      if (c == '\'' || c == '"') {
        boolean escapes =
            c == '\''
                && i > 0
                && (sql.charAt(i - 1) == 'E' || sql.charAt(i - 1) == 'e')
                && (i == 1 || !Character.isLetterOrDigit(sql.charAt(i - 2)));
        end = closing(sql, i, c, escapes);
      } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        int newline = sql.indexOf('\n', i);
        end = newline < 0 ? sql.length() : newline;
      } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        int close = sql.indexOf("*/", i + 2);
        end = close < 0 ? sql.length() : close + 2;
      }
      if (end < 0) {
        code.append(c);
        i++;
        continue;
      }
      result.append(rewriteCode(code.toString()));
      code.setLength(0);
      result.append(sql, i, end);
      i = end;
    }
    result.append(rewriteCode(code.toString()));
    return result.toString();
  }

  /** The index after the closing quote (a doubled quote, or in E'' a backslash, escapes it). */
  private static int closing(String sql, int start, char quote, boolean escapes) {
    int i = start + 1;
    while (i < sql.length()) {
      if (escapes && sql.charAt(i) == '\\') {
        i += 2;
        continue;
      }
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

  private static String rewriteCode(String code) {
    Matcher m = OPERATOR.matcher(code);
    String rewritten = m.replaceAll(" $1 ");
    return DEFAULT_COLLATION.matcher(rewritten).replaceAll("");
  }
}
