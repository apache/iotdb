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

package org.apache.iotdb.postgresql.server;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.session.ExternalServiceClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.postgresql.i18n.PostgreSqlMessages;
import org.apache.iotdb.postgresql.protocol.BackendMessages;
import org.apache.iotdb.postgresql.protocol.FrontendMessage;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.PgFrameDecoder;
import org.apache.iotdb.postgresql.protocol.SqlState;
import org.apache.iotdb.postgresql.session.CancelRegistry;
import org.apache.iotdb.postgresql.session.PgSession;
import org.apache.iotdb.postgresql.session.PgTimeZones;
import org.apache.iotdb.postgresql.session.QueryProtocol;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.net.InetSocketAddress;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** One client connection: the startup phase, the login and then the query layer. */
final class PgConnectionHandler extends SimpleChannelInboundHandler<FrontendMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PgConnectionHandler.class);

  private enum State {
    AWAIT_STARTUP,
    PASSWORD,
    READY,
    CLOSED
  }

  private final PgServerContext context;
  private final PgFrameDecoder decoder;
  private final SerialExecutor serial;

  private volatile State state = State.AWAIT_STARTUP;
  private boolean tls;
  private String user;
  private final Map<String, String> startupParameters = new LinkedHashMap<>();
  private ExternalServiceClientSession dbSession;
  private CancelRegistry.Key key;
  // set once after login, read by the event loop and by channelInactive
  private final AtomicReference<QueryProtocol> protocol = new AtomicReference<>();
  private ScheduledFuture<?> startupTimeout;

  PgConnectionHandler(PgServerContext context, PgFrameDecoder decoder) {
    this.context = context;
    this.decoder = decoder;
    this.serial = new SerialExecutor(context.workers);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    if (context.active.incrementAndGet() > context.config.maxConnections()) {
      context.refusedConnections.incrementAndGet();
      fail(
          ctx, PgException.fatal(SqlState.TOO_MANY_CONNECTIONS, "sorry, too many clients already"));
      return;
    }
    startupTimeout =
        ctx.executor()
            .schedule(
                () -> {
                  if (state != State.READY && state != State.CLOSED) {
                    ctx.close();
                  }
                },
                context.config.startupTimeout().toMillis(),
                TimeUnit.MILLISECONDS);
    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    context.active.decrementAndGet();
    state = State.CLOSED;
    if (startupTimeout != null) {
      startupTimeout.cancel(false);
    }
    QueryProtocol running = protocol.get();
    if (running != null) {
      // stop a running query now; release() waits behind it on the serial executor
      running.cancel();
    }
    serial.execute(this::release);
    ctx.fireChannelInactive();
  }

  private void release() {
    if (key != null) {
      context.cancels.unregister(key);
    }
    QueryProtocol current = protocol.get();
    if (current != null) {
      current.close();
    }
    if (dbSession != null) {
      SessionManager.getInstance()
          .closeSession(dbSession, Coordinator.getInstance()::cleanupQueryExecution);
      // no longer listed in SHOW SESSIONS / information_schema.connections
      SessionManager.getInstance().removeExternalServiceSession(dbSession);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FrontendMessage message) {
    try {
      switch (state) {
        case AWAIT_STARTUP:
          startup(ctx, message);
          return;
        case PASSWORD:
          authentication(ctx, message);
          return;
        case READY:
          if (message.type() == 'X') {
            ctx.close();
            return;
          }
          serial.execute(() -> protocol.get().handle(message, ctx.channel()));
          return;
        default:
          // closed: ignore late messages
      }
    } catch (PgException e) {
      fail(ctx, e);
    }
  }

  // ---------------------------------------------------------------------------- startup phase

  private void startup(ChannelHandlerContext ctx, FrontendMessage message) throws PgException {
    if (!message.isStartupPacket()) {
      throw PgException.fatal(SqlState.PROTOCOL_VIOLATION, "expected a startup message");
    }
    int code = message.readInt32();
    switch (code) {
      case FrontendMessage.SSL_REQUEST:
        sslRequest(ctx);
        return;
      case FrontendMessage.GSSENC_REQUEST:
        ByteBuf no = ctx.alloc().buffer(1);
        BackendMessages.sslResponse(no, false);
        ctx.writeAndFlush(no);
        return;
      case FrontendMessage.CANCEL_REQUEST:
        int processId = message.readInt32();
        int secretKey = message.readInt32();
        context.cancels.cancel(processId, secretKey);
        ctx.close();
        return;
      default:
        startupMessage(ctx, message, code);
    }
  }

  private void sslRequest(ChannelHandlerContext ctx) throws PgException {
    if (tls) {
      throw PgException.fatal(SqlState.PROTOCOL_VIOLATION, "SSL was already negotiated");
    }
    ByteBuf answer = ctx.alloc().buffer(1);
    if (context.sslContext == null) {
      BackendMessages.sslResponse(answer, false);
      ctx.writeAndFlush(answer);
      return;
    }
    if (decoder.hasBufferedData()) {
      // sent before the handshake, so not encrypted, perhaps injected (as PostgreSQL since
      // CVE-2021-23214); the client must wait for the answer to its SSLRequest
      throw PgException.fatal(
          SqlState.PROTOCOL_VIOLATION, "received unencrypted data after SSL request");
    }
    SSLEngine engine = context.sslContext.createSSLEngine();
    engine.setUseClientMode(false);
    TlsPolicy.restrict(engine);
    // startTls: the 'S' still goes out unencrypted, everything after it is TLS
    ctx.pipeline().addFirst("tls", new SslHandler(engine, true));
    tls = true;
    BackendMessages.sslResponse(answer, true);
    ctx.writeAndFlush(answer);
  }

  private void startupMessage(ChannelHandlerContext ctx, FrontendMessage message, int code)
      throws PgException {
    int major = code >>> 16;
    int minor = code & 0xFFFF;
    if (major != 3) {
      throw PgException.fatal(
          SqlState.PROTOCOL_VIOLATION, "unsupported frontend protocol " + major + "." + minor);
    }
    if (!tls && !context.loopback) {
      // off loopback every connection negotiates TLS first
      context.refusedUnencrypted.incrementAndGet();
      throw PgException.fatal(
          SqlState.INVALID_AUTHORIZATION,
          "encryption required: connect with SSL (sslmode=require)");
    }
    List<String> unsupportedOptions = new ArrayList<>();
    while (message.remaining() > 1) {
      String name = message.readCString();
      String value = message.readCString();
      if (name.startsWith("_pq_.")) {
        unsupportedOptions.add(name);
      } else {
        startupParameters.put(name.toLowerCase(Locale.ROOT), value);
      }
    }
    user = startupParameters.get("user");
    if (user == null || user.isEmpty()) {
      throw PgException.fatal(SqlState.INVALID_AUTHORIZATION, "no PostgreSQL user name specified");
    }
    String encoding = startupParameters.get("client_encoding");
    if (encoding != null && !isUtf8(encoding)) {
      throw PgException.fatal(
          SqlState.FEATURE_NOT_SUPPORTED, "only client_encoding UTF8 is supported");
    }
    decoder.startupComplete();
    ByteBuf out = ctx.alloc().buffer();
    if (minor > 0 || !unsupportedOptions.isEmpty()) {
      BackendMessages.negotiateProtocolVersion(out, 0, unsupportedOptions);
    }
    InetSocketAddress remote =
        ctx.channel().remoteAddress() instanceof InetSocketAddress
            ? (InetSocketAddress) ctx.channel().remoteAddress()
            : new InetSocketAddress(0); // not a TCP channel (tests)
    dbSession = new ExternalServiceClientSession("postgresql", remote);
    // The password inside TLS (off loopback every connection is encrypted, see above). The
    // database stores no SCRAM verifiers, so SCRAM-SHA-256 cannot be offered.
    state = State.PASSWORD;
    BackendMessages.authenticationCleartextPassword(out);
    ctx.writeAndFlush(out);
  }

  private static boolean isUtf8(String encoding) {
    String normalized = encoding.replace("'", "").replace("-", "").trim().toUpperCase(Locale.ROOT);
    return normalized.equals("UTF8") || normalized.equals("UNICODE");
  }

  // ---------------------------------------------------------------------------- authentication

  private void authentication(ChannelHandlerContext ctx, FrontendMessage message)
      throws PgException {
    if (message.type() != 'p') {
      throw PgException.fatal(SqlState.PROTOCOL_VIOLATION, "expected an authentication message");
    }
    if (state != State.PASSWORD) {
      throw PgException.fatal(SqlState.PROTOCOL_VIOLATION, "unexpected authentication message");
    }
    String password = message.readCString();
    state = State.CLOSED; // until the login is done; no further message is valid
    serial.execute(() -> passwordLogin(ctx, password));
  }

  private void passwordLogin(ChannelHandlerContext ctx, String password) {
    try {
      BasicOpenSessionResp response =
          SessionManager.getInstance()
              .login(
                  dbSession,
                  user,
                  password,
                  zone().getId(),
                  TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3,
                  IoTDBConstant.ClientVersion.V_1_0,
                  SqlDialect.TABLE);
      if (response.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        loginFailed(ctx, response);
        return;
      }
      loggedIn(ctx, ctx.alloc().buffer());
    } catch (RuntimeException e) {
      LOGGER.warn(PostgreSqlMessages.LOG_POSTGRESQL_LOGIN_FAILED_7130B38D, e);
      fail(ctx, PgException.fatal(SqlState.CONNECTION_FAILURE, "login failed"));
    }
  }

  private void loginFailed(ChannelHandlerContext ctx, BasicOpenSessionResp response) {
    context.failedLogins.incrementAndGet();
    int code = response.getCode();
    String sqlState =
        code == TSStatusCode.USER_LOGIN_LOCKED.getStatusCode()
                || code == TSStatusCode.NO_PERMISSION.getStatusCode()
            ? SqlState.INVALID_AUTHORIZATION
            : code == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
                ? SqlState.CONNECTION_FAILURE
                : SqlState.INVALID_PASSWORD;
    fail(ctx, PgException.fatal(sqlState, response.getMessage()));
  }

  /** AuthenticationOk, the parameters, BackendKeyData and ReadyForQuery. */
  private void loggedIn(ChannelHandlerContext ctx, ByteBuf out) {
    PgSession session = new PgSession(dbSession, user, zone());
    startupParameters.forEach(session::setParameter);
    BackendMessages.authenticationOk(out);
    Map<String, String> reported = new LinkedHashMap<>();
    reported.put("server_version", context.serverVersion);
    reported.put("server_encoding", "UTF8");
    reported.put("client_encoding", "UTF8");
    reported.put("DateStyle", "ISO, MDY");
    reported.put("IntervalStyle", "postgres");
    // as the client sent it (a POSIX name keeps its sign convention), or the server's zone
    reported.put("TimeZone", zoneName());
    reported.put("integer_datetimes", "on");
    reported.put("standard_conforming_strings", "on");
    reported.put("is_superuser", "off");
    reported.put("session_authorization", user);
    reported.put("application_name", startupParameters.getOrDefault("application_name", ""));
    reported.forEach(
        (name, value) -> {
          BackendMessages.parameterStatus(out, name, value);
          session.setParameter(name.toLowerCase(Locale.ROOT), value);
        });
    session.markStartup();
    SessionManager.getInstance().registerExternalServiceSession(dbSession);
    QueryProtocol created = context.protocols.apply(session);
    protocol.set(created);
    key = context.cancels.register(created::cancel);
    session.setProcessId(key.processId());
    BackendMessages.backendKeyData(out, key.processId(), key.secretKey());
    BackendMessages.readyForQuery(out, 'I');
    state = State.READY;
    if (startupTimeout != null) {
      startupTimeout.cancel(false);
    }
    ctx.writeAndFlush(out);
  }

  private ZoneId zone() {
    String zone = startupParameters.get("timezone");
    if (zone != null) {
      try {
        return PgTimeZones.parse(zone.replace("'", ""));
      } catch (DateTimeException e) {
        // fall back to the server's zone
      }
    }
    return ZoneId.systemDefault();
  }

  /** The TimeZone reported at startup: the client's own value if it is valid. */
  private String zoneName() {
    String zone = startupParameters.get("timezone");
    if (zone != null) {
      String given = zone.replace("'", "").trim();
      try {
        PgTimeZones.parse(given);
        return given;
      } catch (DateTimeException e) {
        // fall back to the server's zone
      }
    }
    return ZoneId.systemDefault().getId();
  }

  // ---------------------------------------------------------------------------- errors

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
    if (event instanceof io.netty.handler.timeout.IdleStateEvent) {
      // only connections without a running query are idle
      QueryProtocol current = protocol.get();
      if (state != State.READY || current == null || !current.busy()) {
        fail(
            ctx,
            PgException.fatal(
                SqlState.ADMIN_SHUTDOWN, "terminating connection due to idle timeout"));
      }
      return;
    }
    ctx.fireUserEventTriggered(event);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    Throwable root =
        cause instanceof DecoderException && cause.getCause() != null ? cause.getCause() : cause;
    if (root instanceof PgException) {
      fail(ctx, (PgException) root);
    } else {
      LOGGER.debug(
          PostgreSqlMessages.LOG_POSTGRESQL_CONNECTION_ARG_FAILED_C649F2E0,
          ctx.channel().remoteAddress(),
          cause);
      ctx.close();
    }
  }

  /** Sends the error; a FATAL one closes the connection afterwards. */
  private void fail(ChannelHandlerContext ctx, PgException error) {
    if (error.isFatal()) {
      state = State.CLOSED;
    }
    ByteBuf out = ctx.alloc().buffer();
    BackendMessages.errorResponse(out, error);
    if (error.isFatal()) {
      ctx.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
    } else {
      ctx.writeAndFlush(out);
    }
  }
}
