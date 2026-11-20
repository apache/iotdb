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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.postgresql.execution.ResultCursor;
import org.apache.iotdb.postgresql.execution.RunningQuery;
import org.apache.iotdb.postgresql.protocol.BackendMessages;
import org.apache.iotdb.postgresql.protocol.FrontendMessage;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;
import org.apache.iotdb.postgresql.sql.PgToTableSql;
import org.apache.iotdb.postgresql.sql.PgToTableSql.Translation;
import org.apache.iotdb.postgresql.sql.TranslationContext;
import org.apache.iotdb.postgresql.types.ParameterDecoder;
import org.apache.iotdb.postgresql.types.PgType;
import org.apache.iotdb.postgresql.types.PgValueEncoder;
import org.apache.iotdb.postgresql.types.TimestampPrecision;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The query layer: session commands, and queries translated to table-model SQL and run by the
 * engine, through the simple and the extended query protocol . Runs on the connection's ordered
 * executor.
 */
public final class SqlProtocol implements QueryProtocol {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlProtocol.class);

  /** A parsed statement (Parse). */
  private static final class Prepared {
    final String sql;
    final boolean empty;
    final boolean command;
    final Translation translation;
    final int[] parameterTypes;
    final CursorCommand cursor;

    Prepared(String sql, boolean empty, boolean command, Translation translation, int[] types) {
      this(sql, empty, command, translation, types, null);
    }

    Prepared(
        String sql,
        boolean empty,
        boolean command,
        Translation translation,
        int[] types,
        CursorCommand cursor) {
      this.sql = sql;
      this.empty = empty;
      this.command = command;
      this.translation = translation;
      this.parameterTypes = types;
      this.cursor = cursor;
    }
  }

  /** A SQL cursor (DECLARE): a running query fetched by name. */
  private static final class SqlCursor {
    final ResultCursor rows;
    final boolean hold;
    final boolean binary;
    final boolean inTransaction;

    SqlCursor(ResultCursor rows, boolean hold, boolean binary, boolean inTransaction) {
      this.rows = rows;
      this.hold = hold;
      this.binary = binary;
      this.inTransaction = inTransaction;
    }
  }

  /** A bound statement (Bind), possibly with a running query. */
  private static final class Portal {
    final Prepared statement;
    final List<Literal> parameters;
    final short[] resultFormats;
    ResultCursor cursor;
    boolean finished;

    Portal(Prepared statement, List<Literal> parameters, short[] resultFormats) {
      this.statement = statement;
      this.parameters = parameters;
      this.resultFormats = resultFormats;
    }
  }

  private final PgSession session;
  private final Duration writeTimeout;
  private final TimestampPrecision precision;
  private final Map<String, Prepared> statements = new HashMap<>();
  private final Map<String, Portal> portals = new HashMap<>();
  private final Map<String, SqlCursor> cursors = new HashMap<>();
  private boolean skipUntilSync;
  private volatile ResultCursor running;
  private volatile boolean cancelRequested;
  private volatile boolean busy;

  public SqlProtocol(PgSession session, Duration writeTimeout, TimestampPrecision precision) {
    this.session = session;
    this.writeTimeout = writeTimeout;
    this.precision = precision;
  }

  // ---------------------------------------------------------------------------- dispatch

  @Override
  public void handle(FrontendMessage message, Channel channel) {
    ByteBuf out = channel.alloc().buffer();
    if (!session.session().isLogin()) {
      // the session was ended from outside
      LOGGER.info("PostgreSQL session of {} was ended; closing the connection", session.user());
      closePortals();
      closeCursors();
      BackendMessages.errorResponse(
          out,
          PgException.fatal(
              SqlState.ADMIN_SHUTDOWN, "terminating connection due to administrator command"));
      channel.writeAndFlush(out).addListener(f -> channel.close());
      return;
    }
    busy = true;
    try {
      char type = message.type();
      if (skipUntilSync && type != 'S' && type != 'Q') {
        return;
      }
      switch (type) {
        case 'Q':
          simpleQuery(message.readCString(), channel, out);
          break;
        case 'P':
          parse(message, out);
          break;
        case 'B':
          bind(message, out);
          break;
        case 'D':
          describe(message, out);
          break;
        case 'E':
          execute(message, channel, out);
          break;
        case 'C':
          close(message, out);
          break;
        case 'S':
          sync(out);
          break;
        case 'H':
          break;
        case 'F':
          throw new PgException(SqlState.FEATURE_NOT_SUPPORTED, "the function call protocol");
        case 'd':
        case 'c':
        case 'f':
          throw new PgException(SqlState.FEATURE_NOT_SUPPORTED, "COPY is not supported");
        default:
          throw PgException.fatal(
              SqlState.PROTOCOL_VIOLATION, "invalid frontend message type '" + type + "'");
      }
    } catch (PgException e) {
      fail(e, out);
      if (e.isFatal()) {
        busy = false;
        channel.writeAndFlush(out).addListener(f -> channel.close());
        return;
      }
      if (message.type() != 'Q') {
        // extended protocol: discard messages until Sync
        skipUntilSync = true;
      }
    } catch (ResultCursor.WriteTimeoutException e) {
      LOGGER.info(
          "PostgreSQL client {} stopped reading results for {}; closing the connection",
          session.user(),
          writeTimeout);
      closePortals();
      out.clear();
      BackendMessages.errorResponse(
          out,
          PgException.fatal(
              SqlState.ADMIN_SHUTDOWN,
              "terminating connection because the client stopped reading"));
      busy = false;
      channel.writeAndFlush(out).addListener(f -> channel.close());
      return;
    } catch (RuntimeException | LinkageError e) {
      // never leave the client waiting: an unexpected failure is reported like any other error
      LOGGER.warn("PostgreSQL message '{}' of {} failed", message.type(), session.user(), e);
      closePortals();
      out.clear();
      fail(new PgException(SqlState.INTERNAL_ERROR, "internal error: " + e), out);
      if (message.type() != 'Q') {
        skipUntilSync = true;
      } else {
        BackendMessages.readyForQuery(out, session.transactionStatus());
      }
    } finally {
      busy = false;
    }
    if (out.readableBytes() > 0) {
      channel.writeAndFlush(out);
    } else {
      out.release();
    }
  }

  private void fail(PgException e, ByteBuf out) {
    BackendMessages.errorResponse(out, e);
    if (session.transactionStatus() == 'T') {
      session.setTransactionStatus('E');
    }
  }

  // ---------------------------------------------------------------------------- simple query

  private void simpleQuery(String sql, Channel channel, ByteBuf out)
      throws ResultCursor.WriteTimeoutException {
    skipUntilSync = false;
    // a simple query closes the unnamed statement and portal
    statements.remove("");
    closePortal("");
    List<String> parts = StatementSplitter.split(sql);
    if (parts.isEmpty()) {
      BackendMessages.emptyQueryResponse(out);
    }
    for (String statement : parts) {
      if (statement.isEmpty()) {
        BackendMessages.emptyQueryResponse(out);
        continue;
      }
      try {
        if (session.transactionStatus() == 'E' && !isTransactionEnd(statement)) {
          throw new PgException(
              "25P02",
              "current transaction is aborted, commands ignored until end of transaction block");
        }
        Prepared prepared = prepare(statement, new int[0]);
        Portal portal = new Portal(prepared, Collections.emptyList(), new short[0]);
        try {
          run(portal, channel, out, 0, true);
        } finally {
          if (portal.cursor != null) {
            portal.cursor.close();
          }
        }
      } catch (PgException e) {
        fail(e, out);
        break;
      }
    }
    BackendMessages.readyForQuery(out, session.transactionStatus());
  }

  private static boolean isTransactionEnd(String statement) {
    String s = statement.trim().toUpperCase(Locale.ROOT);
    return s.startsWith("COMMIT")
        || s.startsWith("ROLLBACK")
        || s.startsWith("END")
        || s.startsWith("ABORT");
  }

  // ---------------------------------------------------------------------------- extended query

  private void parse(FrontendMessage message, ByteBuf out) throws PgException {
    String name = message.readCString();
    String sql = message.readCString();
    int count = message.readInt16();
    int[] types = new int[count];
    for (int i = 0; i < count; i++) {
      types[i] = message.readInt32();
    }
    if (!name.isEmpty() && statements.containsKey(name)) {
      throw new PgException(
          SqlState.DUPLICATE_PREPARED_STATEMENT,
          "prepared statement \"" + name + "\" already exists");
    }
    if (session.transactionStatus() == 'E' && !isTransactionEnd(sql)) {
      throw new PgException(
          "25P02",
          "current transaction is aborted, commands ignored until end of transaction block");
    }
    statements.put(name, prepare(sql, types));
    BackendMessages.parseComplete(out);
  }

  private Prepared prepare(String sql, int[] types) throws PgException {
    List<String> parts = StatementSplitter.split(sql);
    if (parts.isEmpty() || (parts.size() == 1 && parts.get(0).isEmpty())) {
      return new Prepared(sql, true, false, null, types);
    }
    if (parts.size() > 1) {
      throw new PgException(
          SqlState.SYNTAX_ERROR, "cannot insert multiple commands into a prepared statement");
    }
    String statement = parts.get(0);
    Optional<CursorCommand> cursor = CursorCommand.parse(statement);
    if (cursor.isPresent()) {
      Translation query = null;
      int[] allTypes = types;
      if (cursor.get().kind() == CursorCommand.Kind.DECLARE) {
        query = PgToTableSql.translate(cursor.get().query(), translationContext());
        if (query.parameterCount() > types.length) {
          allTypes = java.util.Arrays.copyOf(types, query.parameterCount());
        }
      }
      return new Prepared(statement, false, false, query, allTypes, cursor.get());
    }
    if (SessionCommands.isCommand(statement)) {
      return new Prepared(statement, false, true, null, types);
    }
    Translation translation =
        PgToTableSql.translate(statement, translationContext()).withTypedParameters(types);
    int[] allTypes = types;
    if (translation.parameterCount() > types.length) {
      allTypes = java.util.Arrays.copyOf(types, translation.parameterCount());
    }
    return new Prepared(statement, false, false, translation, allTypes);
  }

  private void bind(FrontendMessage message, ByteBuf out) throws PgException {
    String portalName = message.readCString();
    String statementName = message.readCString();
    Prepared statement = statements.get(statementName);
    if (statement == null) {
      throw new PgException(
          SqlState.INVALID_SQL_STATEMENT_NAME,
          "prepared statement \"" + statementName + "\" does not exist");
    }
    short[] parameterFormats = readFormats(message);
    int count = message.readInt16();
    if (count != statement.parameterTypes.length) {
      throw new PgException(
          SqlState.PROTOCOL_VIOLATION,
          "bind message supplies "
              + count
              + " parameters, but prepared statement \""
              + statementName
              + "\" requires "
              + statement.parameterTypes.length);
    }
    ParameterDecoder decoder = new ParameterDecoder(session.zone(), precision);
    Literal[] values = new Literal[count];
    for (int i = 0; i < count; i++) {
      int length = message.readInt32();
      byte[] value = message.readBytes(length);
      short format =
          parameterFormats.length == 0
              ? 0
              : parameterFormats.length == 1 ? parameterFormats[0] : parameterFormats[i];
      values[i] = decoder.decode(statement.parameterTypes[i], format == 1, value);
    }
    short[] resultFormats = readFormats(message);
    List<Literal> ordered = new ArrayList<>();
    if (statement.translation != null) {
      for (int n : statement.translation.parameterOrder()) {
        ordered.add(values[n - 1]);
      }
    }
    if (!portalName.isEmpty() && portals.containsKey(portalName)) {
      throw new PgException(
          SqlState.DUPLICATE_CURSOR, "portal \"" + portalName + "\" already exists");
    }
    closePortal(portalName);
    portals.put(portalName, new Portal(statement, ordered, resultFormats));
    BackendMessages.bindComplete(out);
  }

  private static short[] readFormats(FrontendMessage message) throws PgException {
    int count = message.readInt16();
    short[] formats = new short[count];
    for (int i = 0; i < count; i++) {
      formats[i] = message.readInt16();
      if (formats[i] != 0 && formats[i] != 1) {
        throw new PgException(
            SqlState.PROTOCOL_VIOLATION, "unsupported format code: " + formats[i]);
      }
    }
    return formats;
  }

  private void describe(FrontendMessage message, ByteBuf out) throws PgException {
    char kind = (char) message.readByte();
    String name = message.readCString();
    if (kind == 'S') {
      Prepared statement = statements.get(name);
      if (statement == null) {
        throw new PgException(
            SqlState.INVALID_SQL_STATEMENT_NAME,
            "prepared statement \"" + name + "\" does not exist");
      }
      int[] reported = statement.parameterTypes.clone();
      for (int i = 0; i < reported.length; i++) {
        if (reported[i] == 0) {
          reported[i] = PgType.TEXT.oid();
        }
      }
      BackendMessages.parameterDescription(out, reported);
      describeResult(statement, null, out);
      return;
    }
    if (kind != 'P') {
      throw new PgException(SqlState.PROTOCOL_VIOLATION, "invalid DESCRIBE message subtype");
    }
    Portal portal = portals.get(name);
    if (portal == null) {
      // SQL cursors are portals too (psycopg describes its DECLAREd cursor this way)
      SqlCursor cursor = cursors.get(name);
      if (cursor == null) {
        throw new PgException(
            SqlState.INVALID_CURSOR_NAME, "portal \"" + name + "\" does not exist");
      }
      cursor.rows.setFormats(fetchFormats(cursor, null));
      BackendMessages.rowDescription(out, cursor.rows.fields());
      return;
    }
    describeResult(portal.statement, portal, out);
  }

  /**
   * RowDescription or NoData. For a portal the query starts here (Execute continues it); for a
   * statement the query is planned with NULL parameters and stopped at once.
   */
  private void describeResult(Prepared statement, Portal portal, ByteBuf out) throws PgException {
    if (statement.empty) {
      BackendMessages.noData(out);
      return;
    }
    if (statement.cursor != null) {
      if (statement.cursor.kind() == CursorCommand.Kind.FETCH) {
        SqlCursor cursor = cursor(statement.cursor.name());
        cursor.rows.setFormats(fetchFormats(cursor, portal));
        BackendMessages.rowDescription(out, cursor.rows.fields());
      } else {
        BackendMessages.noData(out);
      }
      return;
    }
    if (statement.command) {
      if (SessionCommands.isShow(statement.sql)) {
        BackendMessages.rowDescription(
            out,
            Collections.singletonList(
                new BackendMessages.Field(
                    SessionCommands.showName(statement.sql),
                    PgType.TEXT.oid(),
                    PgType.TEXT.size(),
                    (short) 0)));
      } else {
        BackendMessages.noData(out);
      }
      return;
    }
    if (portal != null) {
      start(portal);
      BackendMessages.rowDescription(out, portal.cursor.fields());
      return;
    }
    // planned with a value of each declared type: the engine refuses comparisons with untyped
    // NULLs; untyped parameters are tried as a number, then as text, then as NULL
    PgException failure = null;
    boolean untyped = false;
    for (int n : statement.translation.parameterOrder()) {
      untyped |= n - 1 >= statement.parameterTypes.length || statement.parameterTypes[n - 1] == 0;
    }
    for (int attempt = 0; attempt < (untyped ? 3 : 1); attempt++) {
      List<Literal> values = new ArrayList<>();
      for (int n : statement.translation.parameterOrder()) {
        int oid = n - 1 < statement.parameterTypes.length ? statement.parameterTypes[n - 1] : 0;
        values.add(placeholder(oid == 0 ? new int[] {20, 25, 0}[attempt] : oid));
      }
      try (RunningQuery query = startQuery(statement.translation, values)) {
        ResultCursor cursor = new ResultCursor(query, new short[0], encoder());
        BackendMessages.rowDescription(out, cursor.fields());
        return;
      } catch (PgException e) {
        failure = failure == null ? e : failure;
      }
    }
    throw failure;
  }

  /** A value of the given parameter type, for planning a statement without its parameters. */
  private static Literal placeholder(int oid) {
    switch (oid) {
      case 16:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral(
            "false");
      case 20:
      case 21:
      case 23:
      case 26:
      case 1114:
      case 1184:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral("0");
      case 700:
      case 701:
      case 1700:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral("0");
      case 25:
      case 1043:
      case 19:
      case 1042:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral("");
      case 1082:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral(
            "1970-01-01");
      case 17:
        return new org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral(
            new byte[0]);
      default:
        return new NullLiteral();
    }
  }

  private void execute(FrontendMessage message, Channel channel, ByteBuf out)
      throws PgException, ResultCursor.WriteTimeoutException {
    String name = message.readCString();
    int maxRows = message.readInt32();
    Portal portal = portals.get(name);
    if (portal == null) {
      throw new PgException(SqlState.INVALID_CURSOR_NAME, "portal \"" + name + "\" does not exist");
    }
    if (session.transactionStatus() == 'E'
        && !(portal.statement.command && isTransactionEnd(portal.statement.sql))) {
      throw new PgException(
          "25P02",
          "current transaction is aborted, commands ignored until end of transaction block");
    }
    run(portal, channel, out, Math.max(0, maxRows), false);
  }

  private void close(FrontendMessage message, ByteBuf out) throws PgException {
    char kind = (char) message.readByte();
    String name = message.readCString();
    if (kind == 'S') {
      statements.remove(name);
    } else if (kind == 'P') {
      closePortal(name);
      SqlCursor cursor = cursors.remove(name);
      if (cursor != null) {
        cursor.rows.close();
      }
    } else {
      throw new PgException(SqlState.PROTOCOL_VIOLATION, "invalid CLOSE message subtype");
    }
    BackendMessages.closeComplete(out);
  }

  private void sync(ByteBuf out) {
    skipUntilSync = false;
    if (session.transactionStatus() == 'I') {
      // end of the implicit transaction: its portals and cursors without HOLD end too
      closePortals();
      endTransactionCursors(false);
    }
    BackendMessages.readyForQuery(out, session.transactionStatus());
  }

  // ---------------------------------------------------------------------------- running

  /**
   * Runs a portal: a session command, or the next rows of its query.
   *
   * @param maxRows 0 for all rows
   * @param describe whether to send RowDescription first (simple query)
   */
  private void run(Portal portal, Channel channel, ByteBuf out, long maxRows, boolean describe)
      throws PgException, ResultCursor.WriteTimeoutException {
    Prepared statement = portal.statement;
    if (statement.empty) {
      BackendMessages.emptyQueryResponse(out);
      return;
    }
    if (statement.cursor != null) {
      runCursorCommand(portal, channel, out, describe);
      return;
    }
    if (statement.command) {
      Optional<SessionCommands.Result> result = SessionCommands.execute(session, statement.sql);
      if (!result.isPresent()) {
        throw new PgException(SqlState.INTERNAL_ERROR, "not a session command");
      }
      SessionCommands.Result r = result.get();
      r.parameterStatus().forEach((n, v) -> BackendMessages.parameterStatus(out, n, v));
      if (r.showName() != null) {
        if (describe) {
          BackendMessages.rowDescription(
              out,
              Collections.singletonList(
                  new BackendMessages.Field(
                      r.showName(), PgType.TEXT.oid(), PgType.TEXT.size(), (short) 0)));
        }
        int row = BackendMessages.beginDataRow(out, 1);
        BackendMessages.value(out, r.showValue().getBytes(StandardCharsets.UTF_8));
        BackendMessages.end(out, row);
      }
      BackendMessages.commandComplete(out, r.tag());
      if (r.tag().equals("COMMIT") || r.tag().equals("ROLLBACK")) {
        closePortalsExcept(portal);
        endTransactionCursors(r.tag().equals("ROLLBACK"));
      } else if (r.tag().equals("DISCARD ALL")) {
        closeCursors();
      }
      return;
    }
    if (portal.finished) {
      BackendMessages.commandComplete(out, "SELECT 0");
      return;
    }
    start(portal);
    if (describe) {
      BackendMessages.rowDescription(out, portal.cursor.fields());
    }
    running = portal.cursor;
    long sent;
    try {
      sent = portal.cursor.send(channel, out, maxRows, writeTimeout);
    } catch (PgException e) {
      closeCursor(portal);
      if (cancelRequested) {
        throw new PgException(SqlState.QUERY_CANCELED, "canceling statement due to user request");
      }
      throw e;
    } finally {
      running = null;
    }
    if (portal.cursor.exhausted()) {
      closeCursor(portal);
      portal.finished = true;
      BackendMessages.commandComplete(out, "SELECT " + sent);
    } else {
      BackendMessages.portalSuspended(out);
    }
  }

  private void start(Portal portal) throws PgException {
    if (portal.cursor != null) {
      return;
    }
    cancelRequested = false;
    RunningQuery query = startQuery(portal.statement.translation, portal.parameters);
    try {
      portal.cursor = new ResultCursor(query, portal.resultFormats, encoder());
    } catch (PgException e) {
      query.close();
      throw e;
    }
  }

  private RunningQuery startQuery(Translation translation, List<Literal> parameters)
      throws PgException {
    return RunningQuery.start(
        session.session(),
        translation.sqlFor(parameters),
        translation.remaining(parameters),
        StatementTimeout.parse(session.parameter("statement_timeout")));
  }

  private PgValueEncoder encoder() {
    return new PgValueEncoder(session.zone(), precision);
  }

  private static void closeCursor(Portal portal) {
    if (portal.cursor != null) {
      portal.cursor.close();
      portal.cursor = null;
    }
  }

  private void closePortal(String name) {
    Portal portal = portals.remove(name);
    if (portal != null) {
      closeCursor(portal);
    }
  }

  private void closePortals() {
    for (Portal portal : portals.values()) {
      closeCursor(portal);
    }
    portals.clear();
  }

  private void closePortalsExcept(Portal keep) {
    portals
        .entrySet()
        .removeIf(
            e -> {
              if (e.getValue() == keep) {
                return false;
              }
              closeCursor(e.getValue());
              return true;
            });
  }

  // ---------------------------------------------------------------------------- SQL cursors

  private void runCursorCommand(Portal portal, Channel channel, ByteBuf out, boolean describe)
      throws PgException, ResultCursor.WriteTimeoutException {
    CursorCommand command = portal.statement.cursor;
    switch (command.kind()) {
      case DECLARE:
        declare(portal, command, out);
        return;
      case FETCH:
        {
          SqlCursor cursor = cursor(command.name());
          cursor.rows.setFormats(fetchFormats(cursor, describe ? null : portal));
          if (describe) {
            BackendMessages.rowDescription(out, cursor.rows.fields());
          }
          long sent = moveOrFetch(command.name(), cursor, channel, out, command.count());
          BackendMessages.commandComplete(out, "FETCH " + sent);
          return;
        }
      case MOVE:
        {
          SqlCursor cursor = cursor(command.name());
          long skipped = moveOrFetch(command.name(), cursor, null, null, command.count());
          BackendMessages.commandComplete(out, "MOVE " + skipped);
          return;
        }
      case CLOSE:
      default:
        if (command.name() == null) {
          closeCursors();
        } else {
          cursor(command.name());
          cursors.remove(command.name()).rows.close();
        }
        BackendMessages.commandComplete(out, "CLOSE CURSOR");
    }
  }

  private void declare(Portal portal, CursorCommand command, ByteBuf out) throws PgException {
    if (!command.hold() && session.transactionStatus() == 'I') {
      throw new PgException("25P01", "DECLARE CURSOR can only be used in transaction blocks");
    }
    if (cursors.containsKey(command.name()) || portals.containsKey(command.name())) {
      throw new PgException(
          SqlState.DUPLICATE_CURSOR, "cursor \"" + command.name() + "\" already exists");
    }
    cancelRequested = false;
    RunningQuery query = startQuery(portal.statement.translation, portal.parameters);
    ResultCursor rows;
    try {
      rows = new ResultCursor(query, new short[0], encoder());
    } catch (PgException e) {
      query.close();
      throw e;
    }
    cursors.put(
        command.name(),
        new SqlCursor(rows, command.hold(), command.binary(), session.transactionStatus() == 'T'));
    BackendMessages.commandComplete(out, "DECLARE CURSOR");
  }

  /** Sends ({@code out != null}) or skips up to {@code count} rows (0 = all). */
  private long moveOrFetch(String name, SqlCursor cursor, Channel channel, ByteBuf out, long count)
      throws PgException, ResultCursor.WriteTimeoutException {
    running = cursor.rows;
    try {
      return out == null
          ? cursor.rows.skip(count)
          : cursor.rows.send(channel, out, count, writeTimeout);
    } catch (PgException e) {
      cursors.remove(name);
      cursor.rows.close();
      if (cancelRequested) {
        throw new PgException(SqlState.QUERY_CANCELED, "canceling statement due to user request");
      }
      throw e;
    } finally {
      running = null;
    }
  }

  private SqlCursor cursor(String name) throws PgException {
    SqlCursor cursor = cursors.get(name);
    if (cursor == null) {
      throw new PgException(SqlState.INVALID_CURSOR_NAME, "cursor \"" + name + "\" does not exist");
    }
    return cursor;
  }

  /**
   * The result formats of a FETCH: those of its Bind (extended protocol), else binary for a BINARY
   * cursor and text otherwise.
   */
  private static short[] fetchFormats(SqlCursor cursor, Portal portal) {
    if (portal != null && portal.resultFormats.length > 0) {
      return portal.resultFormats;
    }
    return cursor.binary ? new short[] {1} : new short[0];
  }

  /** At COMMIT, ROLLBACK or the end of an implicit transaction. */
  private void endTransactionCursors(boolean rollback) {
    cursors
        .entrySet()
        .removeIf(
            e -> {
              SqlCursor cursor = e.getValue();
              // ROLLBACK also ends the HOLD cursors declared in the rolled-back transaction
              if (!cursor.hold || (rollback && cursor.inTransaction)) {
                cursor.rows.close();
                return true;
              }
              return false;
            });
  }

  private void closeCursors() {
    for (SqlCursor cursor : cursors.values()) {
      cursor.rows.close();
    }
    cursors.clear();
  }

  // ---------------------------------------------------------------------------- translation

  private TranslationContext translationContext() {
    return new TranslationContext() {
      @Override
      public List<String> searchPath() {
        return SearchPath.parse(session.parameter("search_path"));
      }

      @Override
      public boolean tableExists(String database, String table) {
        try {
          return DataNodeTableCache.getInstance().getTable(database, table) != null;
        } catch (RuntimeException e) {
          return false;
        }
      }

      @Override
      public String user() {
        return session.user();
      }

      @Override
      public String version() {
        return "PostgreSQL " + session.parameter("server_version") + ", read-only adapter";
      }

      @Override
      public String currentDatabase() {
        String database = session.parameter("database");
        return database == null || database.isEmpty() ? session.user() : database;
      }

      @Override
      public int backendPid() {
        return session.processId();
      }

      @Override
      public java.time.ZoneId zone() {
        return session.zone();
      }

      @Override
      public TimestampPrecision precision() {
        return precision;
      }
    };
  }

  // ---------------------------------------------------------------------------- QueryProtocol

  @Override
  public boolean busy() {
    return busy;
  }

  @Override
  public void cancel() {
    ResultCursor cursor = running;
    if (cursor != null) {
      cancelRequested = true;
      cursor.cancel();
    }
  }

  @Override
  public void close() {
    closePortals();
    closeCursors();
    statements.clear();
  }
}
