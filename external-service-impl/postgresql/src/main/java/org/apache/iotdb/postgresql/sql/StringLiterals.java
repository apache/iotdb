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

import org.apache.iotdb.postgresql.protocol.SqlState;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** PostgreSQL's escape string literals ({@code E'...'}). */
final class StringLiterals {

  private StringLiterals() {}

  /**
   * The value of the body of an {@code E'...'} literal as written between the quotes: {@code ''}
   * and C-style backslash escapes (backspace, form feed, newline, carriage return, tab, octal,
   * hexadecimal, 16- and 32-bit unicode; any other character escaped by itself). Octal and
   * hexadecimal escapes are bytes: consecutive ones are decoded together as UTF-8, the server
   * encoding, and invalid sequences are refused as in PostgreSQL.
   */
  static String unescape(String body) {
    StringBuilder out = new StringBuilder(body.length());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (int i = 0; i < body.length(); i++) {
      char c = body.charAt(i);
      if (c == '\\' && i + 1 < body.length()) {
        char e = body.charAt(i + 1);
        int end = byteEscapeEnd(body, i + 1);
        if (end > 0) {
          int radix = e == 'x' ? 16 : 8;
          int from = e == 'x' ? i + 2 : i + 1;
          bytes.write(Integer.parseInt(body.substring(from, end), radix) & 0xFF);
          i = end - 1;
          continue;
        }
      }
      flushBytes(bytes, out);
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
          // \x without hex digits is an x, like any other escaped character
          out.append(e);
      }
    }
    flushBytes(bytes, out);
    return out.toString();
  }

  /**
   * The end (exclusive) of a byte escape whose letter or first digit is at {@code at} ({@code \xh},
   * {@code \xhh}, {@code \o} to {@code \ooo}), or -1 if there is none.
   */
  private static int byteEscapeEnd(String body, int at) {
    char e = body.charAt(at);
    if (e == 'x') {
      int end = at + 1;
      while (end < body.length() && end < at + 3 && isHex(body.charAt(end))) {
        end++;
      }
      return end == at + 1 ? -1 : end;
    }
    if (e >= '0' && e <= '7') {
      int end = at;
      while (end < body.length()
          && end < at + 3
          && body.charAt(end) >= '0'
          && body.charAt(end) <= '7') {
        end++;
      }
      return end;
    }
    return -1;
  }

  /** Decodes the collected escaped bytes as UTF-8; invalid sequences and zero bytes are errors. */
  private static void flushBytes(ByteArrayOutputStream bytes, StringBuilder out) {
    if (bytes.size() == 0) {
      return;
    }
    byte[] raw = bytes.toByteArray();
    bytes.reset();
    for (byte b : raw) {
      if (b == 0) {
        throw new PgToTableSql.Refusal(
            SqlState.CHARACTER_NOT_IN_REPERTOIRE,
            "invalid byte sequence for encoding \"UTF8\": 0x00");
      }
    }
    try {
      out.append(
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(raw)));
    } catch (CharacterCodingException e) {
      throw new PgToTableSql.Refusal(
          SqlState.CHARACTER_NOT_IN_REPERTOIRE,
          "invalid byte sequence for encoding \"UTF8\" in an escape string");
    }
  }

  private static boolean isHex(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }
}
