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

package org.apache.iotdb.db.query.control;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.audit.AuditLogger;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;

public class SessionManager implements SessionManagerMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
  // When the client abnormally exits, we can still know who to disconnect
  /** currSession can be only used in client-thread model services. */
  private final ThreadLocal<IClientSession> currSession = new ThreadLocal<>();

  private final ThreadLocal<Long> currSessionIdleTime = new ThreadLocal<>();

  // sessions does not contain MqttSessions..
  private final Map<IClientSession, Object> sessions = new ConcurrentHashMap<>();
  // used for sessions.
  private final Object placeHolder = new Object();

  private final AtomicLong sessionIdGenerator = new AtomicLong();

  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  private static final AuthorStatement AUTHOR_STATEMENT = new AuthorStatement(StatementType.AUTHOR);

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  private static final boolean enableAuditLog =
      IoTDBDescriptor.getInstance().getConfig().isEnableAuditLog();

  protected SessionManager() {
    // singleton
    String mbeanName =
        String.format(
            "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, "RpcSession");
    JMXService.registerMBean(this, mbeanName);
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion)
      throws TException {
    TSStatus loginStatus;
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();

    loginStatus = AuthorizerManager.getInstance().checkUser(username, password);
    if (loginStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // check the version compatibility
      if (!tsProtocolVersion.equals(CURRENT_RPC_VERSION)) {
        openSessionResp
            .sessionId(-1)
            .setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode())
            .setMessage("The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
      } else {
        supplySession(session, username, zoneId, clientVersion);

        openSessionResp
            .sessionId(session.getId())
            .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .setMessage("Login successfully");

        LOGGER.info(
            "{}: Login status: {}. User : {}, opens Session-{}",
            IoTDBConstant.GLOBAL_DB_NAME,
            openSessionResp.getMessage(),
            username,
            session);
        if (enableAuditLog) {
          AuditLogger.log(
              String.format(
                  "%s: Login status: %s. User : %s, opens Session-%s",
                  IoTDBConstant.GLOBAL_DB_NAME, openSessionResp.getMessage(), username, session),
              AUTHOR_STATEMENT);
        }
      }
    } else {
      if (enableAuditLog) {
        AuditLogger.log(
            String.format("User %s opens Session failed with an incorrect password", username),
            AUTHOR_STATEMENT);
      }
      openSessionResp.sessionId(-1).setMessage(loginStatus.message).setCode(loginStatus.code);
    }

    return openSessionResp;
  }

  public boolean closeSession(IClientSession session, Consumer<Long> releaseByQueryId) {
    releaseSessionResource(session, releaseByQueryId);
    // TODO we only need to do so when query is killed by time out
    //    // close the socket.
    //    // currently, we only focus on RPC service.
    //    // TODO do we need to consider MQTT ClientSession and Internal Client?
    //    if (session instanceof ClientSession) {
    //      ((ClientSession) session).shutdownStream();
    //    }
    IClientSession session1 = currSession.get();
    if (session1 != null && session != session1) {
      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "The client-%s is trying to close another session %s, pls check if it's a bug",
                session, session1),
            AUTHOR_STATEMENT);
      }
      return false;
    } else {
      if (enableAuditLog) {
        AuditLogger.log(String.format("Session-%s is closing", session), AUTHOR_STATEMENT);
      }
      return true;
    }
  }

  private void releaseSessionResource(IClientSession session, Consumer<Long> releaseQueryResource) {
    Iterable<Long> statementIds = session.getStatementIds();
    if (statementIds != null) {
      for (Long statementId : statementIds) {
        Set<Long> queryIdSet = session.removeStatementId(statementId);
        if (queryIdSet != null) {
          for (Long queryId : queryIdSet) {
            releaseQueryResource.accept(queryId);
          }
        }
      }
    }
  }

  public TSStatus closeOperation(
      IClientSession session,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId,
      Consumer<Long> releaseByQueryId) {
    if (!checkLogin(session)) {
      return RpcUtils.getStatus(
          TSStatusCode.NOT_LOGIN,
          "Log in failed. Either you are not authorized or the session has timed out.");
    }

    try {
      if (haveStatementId) {
        if (haveSetQueryId) {
          this.closeDataset(session, statementId, queryId, releaseByQueryId);
        } else {
          this.closeStatement(session, statementId, releaseByQueryId);
        }
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.CLOSE_OPERATION_ERROR, "statement id not set by client.");
      }
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CLOSE_OPERATION, TSStatusCode.CLOSE_OPERATION_ERROR);
    }
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  public boolean checkLogin(IClientSession session) {
    boolean isLoggedIn = session != null && session.isLogin();

    if (!isLoggedIn) {
      LOGGER.info("{}: Not login. ", IoTDBConstant.GLOBAL_DB_NAME);
    }

    return isLoggedIn;
  }

  public long requestStatementId(IClientSession session) {
    long statementId = statementIdGenerator.incrementAndGet();
    session.addStatementId(statementId);
    return statementId;
  }

  public void closeStatement(
      IClientSession session, long statementId, Consumer<Long> releaseByQueryId) {
    Set<Long> queryIdSet = session.removeStatementId(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseByQueryId.accept(queryId);
      }
    }
    session.removeStatementId(statementId);
  }

  public long requestQueryId(IClientSession session, Long statementId) {
    long queryId = requestQueryId();
    session.addQueryId(statementId, queryId);
    return queryId;
  }

  public long requestQueryId() {
    return QueryResourceManager.getInstance().assignQueryId();
  }

  /** this method can be only used in client-thread model. */
  public IClientSession getCurrSession() {
    return currSession.get();
  }

  /** get current session and update session idle time. */
  public IClientSession getCurrSessionAndUpdateIdleTime() {
    IClientSession clientSession = getCurrSession();
    Long idleTime = currSessionIdleTime.get();
    if (idleTime == null) {
      currSessionIdleTime.set(System.nanoTime());
    } else {
      MetricService.getInstance()
          .getOrCreateHistogram(
              Metric.SESSION_IDLE_TIME.toString(),
              MetricLevel.CORE,
              Tag.NAME.toString(),
              String.valueOf(clientSession.getId()))
          .update(System.nanoTime() - idleTime);
    }
    return clientSession;
  }

  /** update connection idle time after execution. */
  public void updateIdleTime() {
    currSessionIdleTime.set(System.nanoTime());
  }

  public TimeZone getSessionTimeZone() {
    IClientSession session = currSession.get();
    if (session != null) {
      return session.getTimeZone();
    } else {
      // only used for test
      return TimeZone.getTimeZone("+08:00");
    }
  }

  /**
   * this method can be only used in client-thread model. But, in message-thread model based
   * service, calling this method has no side effect. <br>
   * MUST CALL THIS METHOD IN client-thread model services. Fortunately, we can just call this
   * method in thrift's event handler.
   */
  public void removeCurrSession() {
    IClientSession session = currSession.get();
    sessions.remove(session);
    currSession.remove();
    currSessionIdleTime.remove();
  }

  /**
   * this method can be only used in client-thread model. Do not use this method in message-thread
   * model based service.
   *
   * @return false if the session has been initialized.
   */
  public boolean registerSession(IClientSession session) {
    if (this.currSession.get() != null) {
      LOGGER.error("the client session is registered repeatedly, pls check whether this is a bug.");
      return false;
    }
    this.currSession.set(session);
    this.currSessionIdleTime.set(System.nanoTime());
    sessions.put(session, placeHolder);
    return true;
  }

  /** must be called after registerSession()) will mark the session login. */
  public void supplySession(
      IClientSession session,
      String username,
      String zoneId,
      IoTDBConstant.ClientVersion clientVersion) {
    session.setId(sessionIdGenerator.incrementAndGet());
    session.setUsername(username);
    session.setZoneId(ZoneId.of(zoneId));
    session.setClientVersion(clientVersion);
    session.setLogin(true);
    session.setLogInTime(System.currentTimeMillis());
  }

  public void closeDataset(
      IClientSession session, Long statementId, Long queryId, Consumer<Long> releaseByQueryId) {
    releaseByQueryId.accept(queryId);
    session.removeQueryId(statementId, queryId);
  }

  public static SessionManager getInstance() {
    return SessionManagerHelper.INSTANCE;
  }

  public SessionInfo getSessionInfo(IClientSession session) {
    return new SessionInfo(session.getId(), session.getUsername(), session.getZoneId().getId());
  }

  @Override
  public Set<String> getAllRpcClients() {
    return this.sessions.keySet().stream()
        .map(IClientSession::toString)
        .collect(Collectors.toSet());
  }

  public TSConnectionInfoResp getAllConnectionInfo() {
    return new TSConnectionInfoResp(
        sessions.keySet().stream()
            .map(IClientSession::convertToTSConnectionInfo)
            .sorted(Comparator.comparingLong(TSConnectionInfo::getLogInTime))
            .collect(Collectors.toList()));
  }

  private static class SessionManagerHelper {

    private static final SessionManager INSTANCE = new SessionManager();

    private SessionManagerHelper() {
      // empty constructor
    }
  }
}
