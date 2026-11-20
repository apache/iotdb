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

import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A SQL cursor statement: {@code DECLARE}, {@code FETCH}, {@code MOVE}, {@code CLOSE}. psycopg's
 * server-side cursors use them instead of protocol portals. Only forward movement exists: the
 * engine streams a query once.
 */
public final class CursorCommand {

  /** The kind of statement. */
  public enum Kind {
    DECLARE,
    FETCH,
    MOVE,
    CLOSE
  }

  private static final int FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
  private static final String NAME = "(\"(?:[^\"]|\"\")+\"|[A-Za-z_][\\w$]*)";

  private static final Pattern DECLARE =
      Pattern.compile(
          "DECLARE\\s+"
              + NAME
              + "\\s+((?:(?:BINARY|INSENSITIVE|ASENSITIVE|NO\\s+SCROLL|SCROLL)\\s+)*)"
              + "CURSOR\\s+(?:(WITH|WITHOUT)\\s+HOLD\\s+)?FOR\\s+(.+)",
          FLAGS);
  private static final Pattern FETCH_OR_MOVE =
      Pattern.compile(
          "(FETCH|MOVE)"
              + "(?:\\s+(FORWARD|NEXT|BACKWARD|PRIOR|FIRST|LAST|ABSOLUTE|RELATIVE))?"
              + "(?:\\s+(ALL|[+-]?\\d+))?"
              + "(?:\\s+(?:FROM|IN))?"
              + "\\s+"
              + NAME,
          FLAGS);
  private static final Pattern CLOSE = Pattern.compile("CLOSE\\s+(ALL|" + NAME + ")", FLAGS);
  private static final Pattern START = Pattern.compile("(DECLARE|FETCH|MOVE|CLOSE)\\b.*", FLAGS);

  private final Kind kind;
  private final String name;
  private final long count;
  private final boolean hold;
  private final boolean binary;
  private final String query;

  private CursorCommand(
      Kind kind, String name, long count, boolean hold, boolean binary, String query) {
    this.kind = kind;
    this.name = name;
    this.count = count;
    this.hold = hold;
    this.binary = binary;
    this.query = query;
  }

  /** The kind of statement. */
  public Kind kind() {
    return kind;
  }

  /** The cursor's name; {@code null} for {@code CLOSE ALL}. */
  public String name() {
    return name;
  }

  /** FETCH/MOVE: the number of rows, 0 for all. */
  public long count() {
    return count;
  }

  /** DECLARE: {@code WITH HOLD} (the cursor survives the end of the transaction). */
  public boolean hold() {
    return hold;
  }

  /** DECLARE: {@code BINARY} (rows in binary format by default). */
  public boolean binary() {
    return binary;
  }

  /** DECLARE: the query. */
  public String query() {
    return query;
  }

  /**
   * Parses a cursor statement.
   *
   * @return empty if {@code statement} is not a cursor statement
   * @throws PgException 0A000 for backward or absolute movement and scrollable cursors, 42601 for
   *     malformed cursor statements
   */
  public static Optional<CursorCommand> parse(String statement) throws PgException {
    String sql = statement.trim();
    if (!START.matcher(sql).matches()) {
      return Optional.empty();
    }
    Matcher m = DECLARE.matcher(sql);
    if (m.matches()) {
      String options = m.group(2).toUpperCase(Locale.ROOT);
      if (options.matches("(?s).*(^|\\s)SCROLL\\b.*") && !options.contains("NO SCROLL")) {
        throw new PgException(SqlState.FEATURE_NOT_SUPPORTED, "SCROLL cursors are not supported");
      }
      return Optional.of(
          new CursorCommand(
              Kind.DECLARE,
              fold(m.group(1)),
              0,
              "WITH".equalsIgnoreCase(m.group(3)),
              options.contains("BINARY"),
              m.group(4).trim()));
    }
    m = FETCH_OR_MOVE.matcher(sql);
    if (m.matches()) {
      Kind kind = m.group(1).equalsIgnoreCase("FETCH") ? Kind.FETCH : Kind.MOVE;
      String direction = m.group(2) == null ? "FORWARD" : m.group(2).toUpperCase(Locale.ROOT);
      if (!direction.equals("FORWARD") && !direction.equals("NEXT")) {
        throw new PgException(
            SqlState.FEATURE_NOT_SUPPORTED,
            kind + " " + direction + " is not supported: cursors only move forward");
      }
      long count = 1;
      if (m.group(3) != null) {
        if (direction.equals("NEXT")) {
          throw new PgException(
              SqlState.SYNTAX_ERROR, "syntax error at or near \"" + m.group(3) + "\"");
        }
        if (m.group(3).equalsIgnoreCase("ALL")) {
          count = 0;
        } else {
          count = Long.parseLong(m.group(3));
          if (count <= 0) {
            throw new PgException(
                SqlState.FEATURE_NOT_SUPPORTED,
                kind + " " + m.group(3) + " is not supported: cursors only move forward");
          }
        }
      }
      return Optional.of(new CursorCommand(kind, fold(m.group(4)), count, false, false, null));
    }
    m = CLOSE.matcher(sql);
    if (m.matches()) {
      String name = m.group(1).equalsIgnoreCase("ALL") ? null : fold(m.group(2));
      return Optional.of(new CursorCommand(Kind.CLOSE, name, 0, false, false, null));
    }
    throw new PgException(SqlState.SYNTAX_ERROR, "syntax error in cursor statement");
  }

  private static String fold(String identifier) {
    if (identifier.startsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
    }
    return identifier.toLowerCase(Locale.ROOT);
  }
}
