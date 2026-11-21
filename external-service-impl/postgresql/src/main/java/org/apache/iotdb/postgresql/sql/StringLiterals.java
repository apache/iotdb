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

/** PostgreSQL's escape string literals ({@code E'...'}). */
final class StringLiterals {

  private StringLiterals() {}

  /**
   * The value of the body of an {@code E'...'} literal as written between the quotes: {@code ''}
   * and C-style backslash escapes (backspace, form feed, newline, carriage return, tab, octal,
   * hexadecimal, 16- and 32-bit unicode; any other character escaped by itself).
   */
  static String unescape(String body) {
    StringBuilder out = new StringBuilder(body.length());
    for (int i = 0; i < body.length(); i++) {
      char c = body.charAt(i);
      if (c == '\'' && i + 1 < body.length() && body.charAt(i + 1) == '\'') {
        out.append('\'');
        i++;
        continue;
      }
      if (c != '\\' || i + 1 >= body.length()) {
        out.append(c);
        continue;
      }
      char e = body.charAt(++i);
      switch (e) {
        case 'b':
          out.append('\b');
          break;
        case 'f':
          out.append('\f');
          break;
        case 'n':
          out.append('\n');
          break;
        case 'r':
          out.append('\r');
          break;
        case 't':
          out.append('\t');
          break;
        case 'x':
          {
            int end = i + 1;
            while (end < body.length() && end < i + 3 && isHex(body.charAt(end))) {
              end++;
            }
            if (end == i + 1) {
              out.append('x');
            } else {
              out.append((char) Integer.parseInt(body.substring(i + 1, end), 16));
              i = end - 1;
            }
            break;
          }
        case 'u':
        case 'U':
          {
            int digits = e == 'u' ? 4 : 8;
            if (i + digits >= body.length()) {
              throw new UnsupportedConstructException("incomplete unicode escape");
            }
            String hex = body.substring(i + 1, i + 1 + digits);
            for (char h : hex.toCharArray()) {
              if (!isHex(h)) {
                throw new UnsupportedConstructException("invalid unicode escape");
              }
            }
            out.appendCodePoint(Integer.parseInt(hex, 16));
            i += digits;
            break;
          }
        default:
          if (e >= '0' && e <= '7') {
            int end = i;
            while (end < body.length()
                && end < i + 3
                && body.charAt(end) >= '0'
                && body.charAt(end) <= '7') {
              end++;
            }
            out.append((char) Integer.parseInt(body.substring(i, end), 8));
            i = end - 1;
          } else {
            out.append(e);
          }
      }
    }
    return out.toString();
  }

  private static boolean isHex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }
}
