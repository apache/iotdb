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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.audit.AuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.storageengine.dataregion.read.control.QueryResourceManager;
import org.apache.iotdb.db.utils.DataNodeAuthUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNpeOrUnexpectedException;

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

  private static final boolean ENABLE_AUDIT_LOG =
      IoTDBDescriptor.getInstance().getConfig().isEnableAuditLog();

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

  /**
   * Check if the password for the give user has expired.
   *
   * @return the timestamp when the password will expire. Long.MAX if the password never expires.
   *     Null if the password history cannot be found.
   */
  public Long checkPasswordExpiration(String username, String password) {
    // check password expiration
    long passwordExpirationDays =
        CommonDescriptor.getInstance().getConfig().getPasswordExpirationDays();
    boolean mayBypassPasswordCheckInException =
        CommonDescriptor.getInstance().getConfig().isMayBypassPasswordCheckInException();

    TSLastDataQueryReq lastDataQueryReq = new TSLastDataQueryReq();
    lastDataQueryReq.setSessionId(0);
    lastDataQueryReq.setPaths(
        Collections.singletonList(
            SystemConstant.PREFIX_PASSWORD_HISTORY + ".`_" + username + "`.password"));

    long queryId = -1;
    try {
      Statement statement = StatementGenerator.createStatement(lastDataQueryReq);
      SessionInfo sessionInfo =
          new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault());

      queryId = requestQueryId();
      ExecutionResult result =
          Coordinator.getInstance()
              .executeForTreeModel(
                  statement,
                  queryId,
                  sessionInfo,
                  "",
                  ClusterPartitionFetcher.getInstance(),
                  ClusterSchemaFetcher.getInstance());
      if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn("Fail to check password expiration: {}", result.status);
        throw new IoTDBRuntimeException(
            "Cannot query password history because: "
                + result
                + ", please log in later or disable password expiration.",
            result.status.getCode());
      }

      IQueryExecution queryExecution = Coordinator.getInstance().getQueryExecution(queryId);
      Optional<TsBlock> batchResult = queryExecution.getBatchResult();
      if (batchResult.isPresent()) {
        TsBlock tsBlock = batchResult.get();
        if (tsBlock.getPositionCount() <= 0) {
          // no password history, may have upgraded from an older version
          return null;
        }
        long lastPasswordTime =
            CommonDateTimeUtils.convertIoTDBTimeToMillis(tsBlock.getTimeByIndex(0));
        // columns of last query: [timeseriesName, value, dataType]
        String oldPassword = tsBlock.getColumn(1).getBinary(0).toString();
        if (oldPassword.equals(AuthUtils.encryptPassword(password))) {
          if (lastPasswordTime + passwordExpirationDays * 1000 * 86400 <= lastPasswordTime) {
            // overflow or passwordExpirationDays <= 0
            return Long.MAX_VALUE;
          } else {
            return lastPasswordTime + passwordExpirationDays * 1000 * 86400;
          }
        } else {
          // 1. the password is incorrect, later logIn will fail
          // 2. the password history does not record correctly, use the current time to create one
          return null;
        }
      } else {
        return null;
      }
    } catch (Throwable e) {
      LOGGER.error("Fail to check password expiration", e);
      if (mayBypassPasswordCheckInException) {
        return Long.MAX_VALUE;
      } else {
        throw new IoTDBRuntimeException(
            "Internal server error " + ", please log in later or disable password expiration.",
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    } finally {
      if (queryId != -1) {
        Coordinator.getInstance().cleanupQueryExecution(queryId);
      }
    }
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

    Long timeToExpire = checkPasswordExpiration(username, password);
    if (timeToExpire != null && timeToExpire <= System.currentTimeMillis()) {
      openSessionResp
          .sessionId(-1)
          .setCode(TSStatusCode.ILLEGAL_PASSWORD.getStatusCode())
          .setMessage("Password has expired, please use \"ALTER USER\" to change to a new one");
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
        supplySession(session, username, ZoneId.of(zoneId), clientVersion);
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
              DataNodeAuthUtils.recordPassword(username, password, null, currentTime);
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

        LOGGER.info(
            "{}: Login status: {}. User : {}, opens Session-{}",
            IoTDBConstant.GLOBAL_DB_NAME,
            openSessionResp.getMessage(),
            username,
            session);
        if (ENABLE_AUDIT_LOG) {
          AuditLogger.log(
              String.format(
                  "%s: Login status: %s. User : %s, opens Session-%s",
                  IoTDBConstant.GLOBAL_DB_NAME, openSessionResp.getMessage(), username, session),
              AUTHOR_STATEMENT);
        }
      }
    } else {
      if (ENABLE_AUDIT_LOG) {
        AuditLogger.log(
            String.format("User %s opens Session failed with an incorrect password", username),
            AUTHOR_STATEMENT);
      }
      openSessionResp.sessionId(-1).setMessage(loginStatus.message).setCode(loginStatus.code);
    }

    return openSessionResp;
  }

  public boolean closeSession(IClientSession session, LongConsumer releaseByQueryId) {
    releaseSessionResource(session, releaseByQueryId);
    MetricService.getInstance()
        .remove(
            MetricType.HISTOGRAM,
            Metric.SESSION_IDLE_TIME.toString(),
            Tag.NAME.toString(),
            String.valueOf(session.getId()));
    // TODO we only need to do so when query is killed by time out  close the socket.
    IClientSession session1 = currSession.get();
    if (session1 != null && session != session1) {
      if (ENABLE_AUDIT_LOG) {
        AuditLogger.log(
            String.format(
                "The client-%s is trying to close another session %s, pls check if it's a bug",
                session, session1),
            AUTHOR_STATEMENT);
      }
      return false;
    } else {
      if (ENABLE_AUDIT_LOG) {
        AuditLogger.log(String.format("Session-%s is closing", session), AUTHOR_STATEMENT);
      }
      return true;
    }
  }

  private void releaseSessionResource(IClientSession session, LongConsumer releaseQueryResource) {
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
          this.closeStatement(session, statementId, releaseByQueryId);
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
      IClientSession session, long statementId, LongConsumer releaseByQueryId) {
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
      String username,
      ZoneId zoneId,
      IoTDBConstant.ClientVersion clientVersion) {
    session.setId(sessionIdGenerator.incrementAndGet());
    session.setUsername(username);
    session.setZoneId(zoneId);
    session.setClientVersion(clientVersion);
    session.setLogin(true);
    session.setLogInTime(System.currentTimeMillis());
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
        session.getUsername(),
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
        sessionInfo.getUserName(),
        ZoneId.systemDefault(),
        sessionInfo.getVersion(),
        sessionInfo.getDatabaseName().orElse(null),
        IClientSession.SqlDialect.TREE);
  }

  public SessionInfo getSessionInfoOfTreeModel(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        session.getUsername(),
        ZoneId.systemDefault(),
        session.getClientVersion(),
        session.getDatabaseName(),
        IClientSession.SqlDialect.TREE);
  }

  public SessionInfo getSessionInfoOfTableModel(IClientSession session) {
    return new SessionInfo(
        session.getId(),
        session.getUsername(),
        ZoneId.systemDefault(),
        session.getClientVersion(),
        session.getDatabaseName(),
        IClientSession.SqlDialect.TABLE);
  }

  public SessionInfo getSessionInfoOfPipeReceiver(IClientSession session, String databaseName) {
    return new SessionInfo(
        session.getId(),
        session.getUsername(),
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

  private static class SessionManagerHelper {

    private static final SessionManager INSTANCE = new SessionManager();

    private SessionManagerHelper() {
      // empty constructor
    }
  }
}
