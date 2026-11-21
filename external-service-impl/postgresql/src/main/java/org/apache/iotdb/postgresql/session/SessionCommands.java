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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Statements handled by the adapter without the engine: transaction commands, SET/RESET/SHOW,
 * DISCARD, UNLISTEN, DEALLOCATE and the pool-reset function call of Npgsql. SQL cursors ({@code
 * DECLARE}, {@code FETCH}, {@code MOVE}, {@code CLOSE}) are {@link CursorCommand}s.
 */
public final class SessionCommands {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionCommands.class);

  /** What a session command produces. */
  public static final class Result {
    private final String tag;
    private final String showName;
    private final String showValue;
    private final Map<String, String> parameterStatus;

    Result(String tag, String showName, String showValue, Map<String, String> parameterStatus) {
      this.tag = tag;
      this.showName = showName;
      this.showValue = showValue;
      this.parameterStatus = parameterStatus;
    }

    /** The CommandComplete tag. */
    public String tag() {
      return tag;
    }

    /** For SHOW and the function call: the column name, else {@code null}. */
    public String showName() {
      return showName;
    }

    public String showValue() {
      return showValue;
    }

    /** Reported parameters that changed (ParameterStatus to send). */
    public Map<String, String> parameterStatus() {
      return parameterStatus;
    }
  }

  /** Parameters reported in ParameterStatus, with their names as PostgreSQL writes them. */
  private static final Map<String, String> REPORTED = new LinkedHashMap<>();

  static {
    for (String name :
        Arrays.asList(
            "application_name",
            "client_encoding",
            "DateStyle",
            "IntervalStyle",
            "TimeZone",
            "standard_conforming_strings",
            "integer_datetimes",
            "is_superuser",
            "server_encoding",
            "server_version",
            "session_authorization")) {
      REPORTED.put(name.toLowerCase(Locale.ROOT), name);
    }
  }

  private static final Set<String> READ_ONLY =
      new HashSet<>(
          Arrays.asList(
              "server_version",
              "server_version_num",
              "server_encoding",
              "integer_datetimes",
              "is_superuser",
              "session_authorization"));

  private static final int FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
  private static final Pattern BEGIN = Pattern.compile("(BEGIN|START\\s+TRANSACTION)\\b.*", FLAGS);
  private static final Pattern COMMIT =
      Pattern.compile("(COMMIT|END)(\\s+(WORK|TRANSACTION))?", FLAGS);
  private static final Pattern ROLLBACK =
      Pattern.compile("(ROLLBACK|ABORT)(\\s+(WORK|TRANSACTION))?", FLAGS);
  private static final Pattern SET_TIME_ZONE =
      Pattern.compile("SET\\s+(?:SESSION\\s+|LOCAL\\s+)?TIME\\s+ZONE\\s+(.+)", FLAGS);
  private static final Pattern SET_AUTHORIZATION_DEFAULT =
      Pattern.compile("SET\\s+SESSION\\s+AUTHORIZATION\\s+DEFAULT", FLAGS);
  private static final Pattern SET =
      Pattern.compile(
          "SET\\s++(?:SESSION\\s++|LOCAL\\s++)?+([A-Za-z_][\\w.]*+)(?:\\s*+=|\\s++TO\\s)\\s*+(.+)",
          FLAGS);
  private static final Pattern RESET = Pattern.compile("RESET\\s+([A-Za-z_][\\w.]*)", FLAGS);
  private static final Pattern SHOW = Pattern.compile("SHOW\\s+([A-Za-z_][\\w.]*)", FLAGS);
  private static final Pattern DISCARD =
      Pattern.compile("DISCARD\\s+(ALL|TEMP|TEMPORARY|SEQUENCES|PLANS)", FLAGS);
  private static final Pattern UNLISTEN = Pattern.compile("UNLISTEN\\s+\\*", FLAGS);
  private static final Pattern DEALLOCATE =
      Pattern.compile("DEALLOCATE\\s+(?:PREPARE\\s+)?([A-Za-z_\"][\\w\"]*)", FLAGS);
  private static final Pattern ADVISORY_UNLOCK_ALL =
      Pattern.compile("SELECT\\s+(?:pg_catalog\\.)?pg_advisory_unlock_all\\s*\\(\\s*\\)", FLAGS);

  private SessionCommands() {}

  private static final Pattern[] ALL_COMMANDS = {
    BEGIN,
    COMMIT,
    ROLLBACK,
    SET_TIME_ZONE,
    SET_AUTHORIZATION_DEFAULT,
    SET,
    RESET,
    SHOW,
    DISCARD,
    UNLISTEN,
    DEALLOCATE,
    ADVISORY_UNLOCK_ALL
  };

  /** True if {@code statement} is a session command (nothing is executed). */
  public static boolean isCommand(String statement) {
    String sql = statement.trim();
    for (Pattern pattern : ALL_COMMANDS) {
      if (pattern.matcher(sql).matches()) {
        return true;
      }
    }
    return false;
  }

  /** True if {@code statement} is a SHOW command (it returns one row). */
  public static boolean isShow(String statement) {
    return SHOW.matcher(statement.trim()).matches();
  }

  /** The column name of a SHOW command's result. */
  public static String showName(String statement) {
    Matcher m = SHOW.matcher(statement.trim());
    return m.matches() ? m.group(1) : "?column?";
  }

  /** The command's result if {@code statement} is a session command, else empty. */
  public static Optional<Result> execute(PgSession session, String statement) throws PgException {
    String sql = statement.trim();
    if (BEGIN.matcher(sql).matches()) {
      session.setTransactionStatus('T');
      return result("BEGIN");
    }
    if (COMMIT.matcher(sql).matches()) {
      session.setTransactionStatus('I');
      return result("COMMIT");
    }
    if (ROLLBACK.matcher(sql).matches()) {
      session.setTransactionStatus('I');
      return result("ROLLBACK");
    }
    if (SET_AUTHORIZATION_DEFAULT.matcher(sql).matches()) {
      return result("SET");
    }
    Matcher m = SET_TIME_ZONE.matcher(sql);
    if (m.matches()) {
      return Optional.of(set(session, "timezone", m.group(1)));
    }
    m = SET.matcher(sql);
    if (m.matches()) {
      return Optional.of(set(session, m.group(1), m.group(2)));
    }
    m = RESET.matcher(sql);
    if (m.matches()) {
      String name = m.group(1).toLowerCase(Locale.ROOT);
      if (name.equals("all")) {
        session.resetAll();
        return result("RESET");
      }
      if (READ_ONLY.contains(name)) {
        throw new PgException(
            SqlState.INVALID_PARAMETER_VALUE, "parameter \"" + m.group(1) + "\" cannot be changed");
      }
      String value = session.reset(name);
      String reported = REPORTED.get(name);
      if (reported != null && value != null) {
        return Optional.of(
            new Result("RESET", null, null, Collections.singletonMap(reported, value)));
      }
      return result("RESET");
    }
    m = SHOW.matcher(sql);
    if (m.matches()) {
      String name = m.group(1).toLowerCase(Locale.ROOT);
      return Optional.of(
          new Result("SHOW", m.group(1), show(session, name), Collections.emptyMap()));
    }
    m = DISCARD.matcher(sql);
    if (m.matches()) {
      return result("DISCARD " + m.group(1).toUpperCase(Locale.ROOT));
    }
    if (UNLISTEN.matcher(sql).matches()) {
      return result("UNLISTEN");
    }
    m = DEALLOCATE.matcher(sql);
    if (m.matches()) {
      return result(m.group(1).equalsIgnoreCase("ALL") ? "DEALLOCATE ALL" : "DEALLOCATE");
    }
    if (ADVISORY_UNLOCK_ALL.matcher(sql).matches()) {
      return Optional.of(
          new Result("SELECT 1", "pg_advisory_unlock_all", "", Collections.emptyMap()));
    }
    return Optional.empty();
  }

  private static Optional<Result> result(String tag) {
    return Optional.of(new Result(tag, null, null, Collections.emptyMap()));
  }

  private static Result set(PgSession session, String rawName, String rawValue) throws PgException {
    String name = rawName.toLowerCase(Locale.ROOT);
    String value = unquote(rawValue.trim());
    if (READ_ONLY.contains(name)) {
      throw new PgException(
          SqlState.INVALID_PARAMETER_VALUE, "parameter \"" + rawName + "\" cannot be changed");
    }
    switch (name) {
      case "client_encoding":
        if (!value.replace("-", "").equalsIgnoreCase("UTF8")
            && !value.equalsIgnoreCase("UNICODE")) {
          throw new PgException(
              SqlState.FEATURE_NOT_SUPPORTED, "only client_encoding UTF8 is supported");
        }
        value = "UTF8";
        break;
      case "bytea_output":
        if (!value.equalsIgnoreCase("hex")) {
          throw new PgException(
              SqlState.FEATURE_NOT_SUPPORTED, "only bytea_output hex is supported");
        }
        break;
      case "intervalstyle":
        if (!value.equalsIgnoreCase("postgres")) {
          throw new PgException(
              SqlState.FEATURE_NOT_SUPPORTED, "only IntervalStyle postgres is supported");
        }
        break;
      case "datestyle":
        if (!value.toUpperCase(Locale.ROOT).startsWith("ISO")) {
          throw new PgException(SqlState.FEATURE_NOT_SUPPORTED, "only DateStyle ISO is supported");
        }
        value = "ISO, MDY";
        break;
      case "timezone":
        try {
          ZoneId zone =
              value.equalsIgnoreCase("LOCAL") || value.equalsIgnoreCase("DEFAULT")
                  ? ZoneId.systemDefault()
                  : ZoneId.of(value);
          session.setZone(zone);
          value = zone.getId();
        } catch (DateTimeException e) {
          throw new PgException(
              SqlState.INVALID_PARAMETER_VALUE,
              "invalid value for parameter \"TimeZone\": \"" + value + "\"");
        }
        break;
      case "statement_timeout":
        try {
          StatementTimeout.parse(value);
        } catch (IllegalArgumentException e) {
          throw new PgException(SqlState.INVALID_PARAMETER_VALUE, e.getMessage());
        }
        break;
      case "search_path":
      case "application_name":
      case "extra_float_digits":
      case "lock_timeout":
      case "client_min_messages":
      case "standard_conforming_strings":
      case "ssl_renegotiation_limit":
        // kept; the query layer reads the ones it honours
        break;
      default:
        LOGGER.debug("Unknown PostgreSQL parameter {} accepted and ignored", name);
    }
    session.setParameter(name, value);
    Map<String, String> status = Collections.emptyMap();
    String reported = REPORTED.get(name);
    if (reported != null) {
      status = Collections.singletonMap(reported, value);
    }
    return new Result("SET", null, null, status);
  }

  private static String show(PgSession session, String name) throws PgException {
    switch (name) {
      case "server_version_num":
        return "140000";
      case "timezone":
        return session.zone().getId();
      case "transaction_isolation":
      case "default_transaction_isolation":
        return "read committed";
      case "transaction_read_only":
      case "default_transaction_read_only":
        return "on";
      case "standard_conforming_strings":
      case "integer_datetimes":
        return "on";
      case "client_encoding":
      case "server_encoding":
        return "UTF8";
      case "datestyle":
        return "ISO, MDY";
      case "intervalstyle":
        return "postgres";
      case "max_identifier_length":
        return "63";
      case "statement_timeout":
        String timeout = session.parameter(name);
        return timeout == null ? "0" : timeout;
      case "application_name":
        String application = session.parameter(name);
        return application == null ? "" : application;
      default:
        String value = session.parameter(name);
        if (value == null) {
          throw new PgException("42704", "unrecognized configuration parameter \"" + name + "\"");
        }
        return value;
    }
  }

  private static String unquote(String value) {
    if (value.length() >= 2
        && (value.startsWith("'") && value.endsWith("'")
            || value.startsWith("\"") && value.endsWith("\""))) {
      char q = value.charAt(0);
      return value.substring(1, value.length() - 1).replace("" + q + q, "" + q);
    }
    return value;
  }
}
