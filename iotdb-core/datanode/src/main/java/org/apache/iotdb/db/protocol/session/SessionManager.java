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

package org.apache.iotdb.db.protocol.session;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AuditEventType;
import org.apache.iotdb.commons.audit.AuditLogFields;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.LoginLockManager;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.ConnectionInfo;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.PreparedStatementMemoryManager;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.utils.DataNodeAuthUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNpeOrUnexpectedException;

public class SessionManager implements SessionManagerMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
  private static final DNAuditLogger AUDIT_LOGGER = DNAuditLogger.getInstance();

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

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  protected SessionManager() {
    // singleton
    String mbeanName =
        String.format(
            "%s:%s=%s",
            IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
            IoTDBConstant.JMX_TYPE,
            ServiceType.SESSION_MANAGER.getJmxName());
    JMXService.registerMBean(this, mbeanName);
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion) {
    return login(
        session,
        username,
        password,
        zoneId,
        tsProtocolVersion,
        clientVersion,
        IClientSession.SqlDialect.TREE);
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion,
      IClientSession.SqlDialect sqlDialect) {
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();

    long userId = AuthorityChecker.getUserId(username).orElse(-1L);

    Long timeToExpire = DataNodeAuthUtils.checkPasswordExpiration(userId, password);
    if (timeToExpire != null && timeToExpire <= System.currentTimeMillis()) {
      openSessionResp
          .sessionId(-1)
          .setCode(TSStatusCode.ILLEGAL_PASSWORD.getStatusCode())
          .setMessage("Password has expired, please use \"ALTER USER\" to change to a new one");
      return openSessionResp;
    }

    boolean enableLoginLock = userId != -1;
    LoginLockManager loginLockManager = LoginLockManager.getInstance();
    if (enableLoginLock && loginLockManager.checkLock(userId, session.getClientAddress())) {
      // Generic authentication error
      openSessionResp
          .sessionId(-1)
          .setMessage("Account is blocked due to consecutive failed logins.")
          .setCode(TSStatusCode.USER_LOGIN_LOCKED.getStatusCode());
      return openSessionResp;
    }

    TSStatus loginStatus = AuthorityChecker.checkUser(username, password);
    if (loginStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // check the version compatibility
      if (!tsProtocolVersion.equals(CURRENT_RPC_VERSION)) {
        openSessionResp
            .sessionId(-1)
            .setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode())
            .setMessage("The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
      } else {
        session.setSqlDialect(sqlDialect);
        supplySession(session, userId, username, ZoneId.of(zoneId), clientVersion);
        String logInMessage = "Login successfully";
        if (timeToExpire != null && timeToExpire != Long.MAX_VALUE) {
          DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
          logInMessage +=
              ". Your password will expire at "
                  + dateFormat.format(
                      LocalDateTime.ofInstant(
                          Instant.ofEpochMilli(timeToExpire), ZoneId.systemDefault()));
        } else if (timeToExpire == null) {
          LOGGER.info(
              "No password history for user {}, using the current time to create a new one",
              username);
          long currentTime = CommonDateTimeUtils.currentTime();
          TSStatus tsStatus =
              DataNodeAuthUtils.recordPasswordHistory(
                  userId, password, AuthUtils.encryptPassword(password), currentTime);
          if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            openSessionResp
                .sessionId(-1)
                .setCode(tsStatus.getCode())
                .setMessage(tsStatus.getMessage());
            return openSessionResp;
          }
          timeToExpire =
              CommonDateTimeUtils.convertIoTDBTimeToMillis(currentTime)
                  + CommonDescriptor.getInstance().getConfig().getPasswordExpirationDays()
                      * 1000
                      * 86400;
          if (timeToExpire > System.currentTimeMillis()) {
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            logInMessage +=
                ". Your password will expire at "
                    + dateFormat.format(
                        LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(timeToExpire), ZoneId.systemDefault()));
          }
        }
        openSessionResp
            .sessionId(session.getId())
            .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .setMessage(logInMessage);
        AUDIT_LOGGER.log(
            new AuditLogFields(
                userId,
                username,
                session.getClientAddress(),
                AuditEventType.LOGIN,
                AuditLogOperation.CONTROL,
                true),
            () ->
                String.format(
                    "%s: Login status: %s. User %s (ID=%d), opens Session-%s",
                    IoTDBConstant.GLOBAL_DB_NAME,
                    openSessionResp.getMessage(),
                    username,
                    userId,
                    session));
        LOGGER.info(
            "{}: Login status: {}. User : {}, opens Session-{}",
            IoTDBConstant.GLOBAL_DB_NAME,
            openSessionResp.getMessage(),
            username,
            session);
        updateIdleTime();
        if (enableLoginLock) {
          loginLockManager.clearFailure(userId, session.getClientAddress());
        }
      }
    } else {
      openSessionResp.sessionId(-1).setMessage(loginStatus.message).setCode(loginStatus.code);
      if (enableLoginLock) {
        loginLockManager.recordFailure(userId, session.getClientAddress());
      }
      AUDIT_LOGGER.log(
          new AuditLogFields(
              userId,
              username,
              session.getClientAddress(),
              AuditEventType.LOGIN,
              AuditLogOperation.CONTROL,
              false),
          () ->
              String.format(
                  "User %s (ID=%d) login failed with code: %d, %s",
                  username, userId, loginStatus.getCode(), loginStatus.getMessage()));
    }
    return openSessionResp;
  }

  public boolean closeSession(IClientSession session, LongConsumer releaseByQueryId) {
    return closeSession(session, releaseByQueryId, true);
  }

  public boolean closeSession(
      IClientSession session, LongConsumer releaseByQueryId, boolean mustCurrent) {
    releaseSessionResource(session, releaseByQueryId);
    MetricService.getInstance()
        .remove(
            MetricType.HISTOGRAM,
            Metric.SESSION_IDLE_TIME.toString(),
            Tag.NAME.toString(),
            String.valueOf(session.getId()));
    // TODO we only need to do so when query is killed by time out  close the socket.
    IClientSession session1 = currSession.get();
    if (mustCurrent && session1 != null && session != session1) {
      LOGGER.info(
          String.format(
              "The client-%s is trying to close another session %s, pls check if it's a bug",
              session1, session));
      return false;
    } else {
      LOGGER.info(String.format("Session-%s is closing", session));
      return true;
    }
  }

  private void releaseSessionResource(IClientSession session, LongConsumer releaseQueryResource) {
    // Release query resources
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

    // Release PreparedStatement memory resources
    try {
      PreparedStatementMemoryManager.getInstance().releaseAllForSession(session);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to release PreparedStatement resources for session {}: {}",
          session,
          e.getMessage(),
          e);
    }
  }

  public TSStatus closeOperation(
      IClientSession session,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId,
      String preparedStatementName,
      LongConsumer releaseByQueryId) {
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
          this.closeStatement(session, statementId, preparedStatementName, releaseByQueryId);
        }
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.CLOSE_OPERATION_ERROR, "statement id not set by client.");
      }
    } catch (Exception e) {
      return onNpeOrUnexpectedException(
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
      IClientSession session,
      long statementId,
      String preparedStatementName,
      LongConsumer releaseByQueryId) {
    Set<Long> queryIdSet = session.removeStatementId(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseByQueryId.accept(queryId);
      }
    }

    // If preparedStatementName is provided, release the prepared statement resources
    if (preparedStatementName != null && !preparedStatementName.isEmpty()) {
      try {
        PreparedStatementInfo removedInfo = session.removePreparedStatement(preparedStatementName);
        if (removedInfo != null) {
          // Release the memory allocated for this PreparedStatement
          PreparedStatementMemoryManager.getInstance().release(removedInfo.getMemorySizeInBytes());
        }
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to release PreparedStatement '{}' resources when closing statement {} for session {}: {}",
            preparedStatementName,
            statementId,
            session,
            e.getMessage(),
            e);
      }
    }
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
    IClientSession session = currSession.get();
    if (session != null) {
      session.setLastActiveTime(CommonDateTimeUtils.currentTime());
    }
  }

  public TimeZone getSessionTimeZone() {
    IClientSession session = currSession.get();
    if (session != null) {
      return session.getTimeZone();
    } else {
      // only used for test
      return TimeZone.getTimeZone(ZoneId.systemDefault());
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
    if (session != null) {
      sessions.remove(session);
    }
    currSession.remove();
    currSessionIdleTime.remove();
  }

  public void removeCurrSessionForMqtt(MqttClientSession mqttClientSession) {
    if (mqttClientSession != null) {
      sessions.remove(mqttClientSession);
    }
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

  /**
   * this method can be only used in mqtt model. Do not use this method in client-thread model based
   * service.
   */
  public void registerSessionForMqtt(IClientSession session) {
    sessions.put(session, placeHolder);
  }

  /** must be called after registerSession()) will mark the session login. */
  public void supplySession(
      IClientSession session,
      long userId,
      String username,
      ZoneId zoneId,
      IoTDBConstant.ClientVersion clientVersion) {
    session.setId(sessionIdGenerator.incrementAndGet());
    session.setUserId(userId);
    session.setUsername(username);
    session.setZoneId(zoneId);
    session.setClientVersion(clientVersion);
    session.setLogInTime(System.currentTimeMillis());
    session.setLogin(true);
  }

  public void closeDataset(
      IClientSession session, Long statementId, Long queryId, LongConsumer releaseByQueryId) {
    releaseByQueryId.accept(queryId);
    session.removeQueryId(statementId, queryId);
  }

  public static SessionManager getInstance() {
    return SessionManagerHelper.INSTANCE;
  }

  public SessionInfo getSessionInfo(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
        session.getZoneId(),
        session.getClientVersion(),
        session.getDatabaseName(),
        session.getSqlDialect());
  }

  // Sometimes we need to switch from table model to tree model,
  // e.g., when loading a tree model TsFile under table model dialect.
  public SessionInfo copySessionInfoForTreeModel(final SessionInfo sessionInfo) {
    return new SessionInfo(
        sessionInfo.getSessionId(),
        sessionInfo.getUserEntity(),
        ZoneId.systemDefault(),
        sessionInfo.getVersion(),
        sessionInfo.getDatabaseName().orElse(null),
        IClientSession.SqlDialect.TREE);
  }

  public SessionInfo getSessionInfoOfTreeModel(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
        ZoneId.systemDefault(),
        session.getClientVersion(),
        session.getDatabaseName(),
        IClientSession.SqlDialect.TREE);
  }

  public SessionInfo getSessionInfoOfTableModel(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
        ZoneId.systemDefault(),
        session.getClientVersion(),
        session.getDatabaseName(),
        IClientSession.SqlDialect.TABLE);
  }

  public SessionInfo getSessionInfoOfPipeReceiver(IClientSession session, String databaseName) {
    return new SessionInfo(
        session.getId(),
        new UserEntity(session.getUserId(), session.getUsername(), session.getClientAddress()),
        ZoneId.systemDefault(),
        session.getClientVersion(),
        databaseName,
        IClientSession.SqlDialect.TABLE);
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
            .filter(s -> StringUtils.isNotEmpty(s.getUsername()))
            .map(IClientSession::convertToTSConnectionInfo)
            .sorted(Comparator.comparingLong(TSConnectionInfo::getLogInTime))
            .collect(Collectors.toList()));
  }

  public List<ConnectionInfo> getAllSessionConnectionInfo() {
    return sessions.keySet().stream()
        .filter(s -> StringUtils.isNotEmpty(s.getUsername()) && s.isLogin())
        .map(IClientSession::convertToConnectionInfo)
        .sorted(Comparator.comparingLong(ConnectionInfo::getLastActiveTime))
        .collect(Collectors.toList());
  }

  private static class SessionManagerHelper {

    private static final SessionManager INSTANCE = new SessionManager();

    private SessionManagerHelper() {
      // empty constructor
    }
  }
}
