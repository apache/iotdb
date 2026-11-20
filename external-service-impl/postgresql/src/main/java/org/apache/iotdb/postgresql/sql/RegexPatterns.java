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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Regular expressions that have an exact LIKE form: the patterns psql and catalog queries build,
 * such as {@code ^(sensors)$}, {@code ^pg_}, {@code ^(sens.*)$} or {@code ^(a|b)$}. Supported:
 * literal characters (backslash escapes punctuation), {@code .} (one character, as {@code _}),
 * {@code .*} (any characters, as {@code %}), the anchors {@code ^} and {@code $}, and one group of
 * alternatives. Anything else has no exact LIKE form.
 */
final class RegexPatterns {

  private static final String META = ".^$|()[]{}*+?\\";

  /** One LIKE pattern per alternative; {@link #exact} if none of them has a wildcard. */
  static final class LikeForm {
    final List<String> patterns;
    final List<String> literals;
    final boolean escaped;

    LikeForm(List<String> patterns, List<String> literals, boolean escaped) {
      this.patterns = patterns;
      this.literals = literals;
      this.escaped = escaped;
    }

    /** True if every alternative is a fixed string ({@link #literals}). */
    boolean exact() {
      return literals != null;
    }
  }

  private RegexPatterns() {}

  /**
   * The LIKE form of {@code regex}, or {@code null} if it has none.
   *
   * <p>LIKE patterns use {@code \} as the escape character when {@link LikeForm#escaped}.
   */
  static LikeForm toLike(String regex) {
    String body = regex;
    boolean start = body.startsWith("^");
    if (start) {
      body = body.substring(1);
    }
    boolean end = body.endsWith("$") && !body.endsWith("\\$");
    if (end) {
      body = body.substring(0, body.length() - 1);
    }
    List<String> alternatives;
    if (body.startsWith("(") && body.endsWith(")") && body.indexOf('(', 1) < 0) {
      String inner = body.substring(1, body.length() - 1);
      if (inner.indexOf(')') >= 0) {
        return null;
      }
      alternatives = split(inner);
      if (alternatives == null) {
        return null;
      }
    } else {
      alternatives = Collections.singletonList(body);
    }
    if (alternatives.size() > 1 && (!start || !end)) {
      // a|b without both anchors binds the anchors to the first and last alternative only
      return null;
    }
    List<String> patterns = new ArrayList<>();
    List<String> literals = new ArrayList<>();
    boolean escaped = false;
    for (String alternative : alternatives) {
      StringBuilder like = new StringBuilder();
      StringBuilder literal = new StringBuilder();
      boolean wildcard = !start || !end;
      if (!start) {
        like.append('%');
      }
      for (int i = 0; i < alternative.length(); i++) {
        char c = alternative.charAt(i);
        if (c == '\\') {
          if (i + 1 >= alternative.length()) {
            return null;
          }
          char next = alternative.charAt(++i);
          if (Character.isLetterOrDigit(next)) {
            // \d, \w, \m, ... are classes or constraints, not literals
            return null;
          }
          c = next;
        } else if (c == '.') {
          if (i + 1 < alternative.length() && alternative.charAt(i + 1) == '*') {
            like.append('%');
            i++;
          } else {
            like.append('_');
          }
          wildcard = true;
          continue;
        } else if (META.indexOf(c) >= 0) {
          return null;
        }
        if (c == '%' || c == '_' || c == '\\') {
          like.append('\\');
          escaped = true;
        }
        like.append(c);
        literal.append(c);
      }
      if (!end) {
        like.append('%');
      }
      patterns.add(like.toString());
      literals.add(wildcard ? null : literal.toString());
    }
    boolean exact = literals.stream().allMatch(l -> l != null);
    return new LikeForm(patterns, exact ? literals : null, escaped);
  }

  /** Splits alternatives at top-level {@code |}; {@code null} if a part is malformed. */
  private static List<String> split(String inner) {
    List<String> parts = new ArrayList<>();
    StringBuilder part = new StringBuilder();
    for (int i = 0; i < inner.length(); i++) {
      char c = inner.charAt(i);
      if (c == '\\' && i + 1 < inner.length()) {
        part.append(c).append(inner.charAt(++i));
      } else if (c == '|') {
        parts.add(part.toString());
        part.setLength(0);
      } else {
        part.append(c);
      }
    }
    parts.add(part.toString());
    for (String p : parts) {
      if (p.isEmpty()) {
        return null;
      }
    }
    return parts;
  }
}
