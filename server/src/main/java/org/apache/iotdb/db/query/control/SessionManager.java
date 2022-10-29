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
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfo;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class SessionManager implements SessionManagerMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
  public static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);
  // When the client abnormally exits, we can still know who to disconnect
  /** currSession can be only used in client-thread model services. */
  private final ThreadLocal<IClientSession> currSession = new ThreadLocal<>();

  // sessions does not contain MqttSessions..
  private final Map<IClientSession, Object> sessions = new ConcurrentHashMap<>();
  // used for sessions.
  private final Object placeHolder = new Object();

  // we keep this sessionIdGenerator just for keep Compatible with v0.13
  @Deprecated private final AtomicLong sessionIdGenerator = new AtomicLong();
  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  // (statementId -> Set(queryId))
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  // (queryId -> QueryDataSet)
  private final Map<Long, QueryDataSet> queryIdToDataSet = new ConcurrentHashMap<>();

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

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
    boolean loginStatus = false;
    String loginMessage = null;

    try {
      loginStatus = AuthorizerManager.getInstance().login(username, password);
    } catch (AuthException e) {
      loginMessage = e.getMessage();
      LOGGER.info("meet error while logging in.", e);
    }

    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();
    if (loginStatus) {
      // check the version compatibility
      if (!tsProtocolVersion.equals(CURRENT_RPC_VERSION)) {
        openSessionResp
            .sessionId(-1)
            .setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode())
            .setMessage("The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
      } else {
        supplySession(session, username, zoneId, clientVersion);

        SessionTimeoutManager.getInstance().register(session);

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
      }
    } else {
      AUDIT_LOGGER.info("User {} opens Session failed with an incorrect password", username);

      openSessionResp
          .sessionId(-1)
          .setMessage(loginMessage != null ? loginMessage : "Authentication failed.")
          .setCode(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR.getStatusCode());
    }
    return openSessionResp;
  }

  public BasicOpenSessionResp login(
      IClientSession session,
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion)
      throws TException {
    return login(
        session, username, password, zoneId, tsProtocolVersion, IoTDBConstant.ClientVersion.V_0_12);
  }

  public boolean closeSession(IClientSession session) {
    AUDIT_LOGGER.info("Session-{} is closing", session);
    currSession.remove();
    return SessionTimeoutManager.getInstance().unregister(session);
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
          TSStatusCode.NOT_LOGIN_ERROR,
          "Log in failed. Either you are not authorized or the session has timed out.");
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "{}: receive close operation from Session {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          currSession.get());
    }

    try {
      if (haveStatementId) {
        if (haveSetQueryId) {
          this.closeDataset(statementId, queryId, releaseByQueryId);
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

  public TSStatus closeOperation(
      IClientSession session,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId) {
    return closeOperation(
        session,
        queryId,
        statementId,
        haveStatementId,
        haveSetQueryId,
        this::releaseQueryResourceNoExceptions);
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
    } else {
      SessionTimeoutManager.getInstance().refresh(session);
    }
    return isLoggedIn;
  }

  public boolean releaseSessionResource(IClientSession session) {
    return releaseSessionResource(session, this::releaseQueryResourceNoExceptions);
  }

  public boolean releaseSessionResource(
      IClientSession session, Consumer<Long> releaseQueryResource) {
    Set<Long> statementIdSet = session.getStatementIds();
    if (statementIdSet != null) {
      for (Long statementId : statementIdSet) {
        Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
        if (queryIdSet != null) {
          for (Long queryId : queryIdSet) {
            releaseQueryResource.accept(queryId);
          }
        }
      }
      return true;
    }

    // TODO if there is no statement for the session, how to return (true or false?)
    return false;
  }

  public IClientSession getSessionIdByQueryId(long queryId) {
    // TODO: make this more efficient with a queryId -> sessionId map
    for (Map.Entry<Long, Set<Long>> statementToQueries : statementIdToQueryId.entrySet()) {
      if (statementToQueries.getValue().contains(queryId)) {
        Long statementId = statementToQueries.getKey();
        for (IClientSession session : sessions.keySet()) {
          if (session.getStatementIds().contains(statementId)) {
            return session;
          }
        }
      }
    }
    return null;
  }

  public long requestStatementId(IClientSession session) {
    long statementId = statementIdGenerator.incrementAndGet();
    session.getStatementIds().add(statementId);
    return statementId;
  }

  public void closeStatement(
      IClientSession session, long statementId, Consumer<Long> releaseByQueryId) {
    Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseByQueryId.accept(queryId);
      }
    }
    session.getStatementIds().remove(statementId);
  }

  public long requestQueryId(Long statementId, boolean isDataQuery) {
    long queryId = requestQueryId(isDataQuery);
    statementIdToQueryId
        .computeIfAbsent(statementId, k -> new CopyOnWriteArraySet<>())
        .add(queryId);
    return queryId;
  }

  public long requestQueryId(boolean isDataQuery) {
    return QueryResourceManager.getInstance().assignQueryId(isDataQuery);
  }

  public void releaseQueryResource(long queryId) throws StorageEngineException {
    QueryDataSet dataSet = queryIdToDataSet.remove(queryId);
    if (dataSet instanceof UDTFDataSet) {
      ((UDTFDataSet) dataSet).finalizeUDFs(queryId);
    }
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  public void releaseQueryResourceNoExceptions(long queryId) {
    if (queryId != -1) {
      try {
        releaseQueryResource(queryId);
      } catch (Exception e) {
        LOGGER.warn("Error occurred while releasing query resource: ", e);
      }
    }
  }

  /** Check whether specific user has the authorization to given plan. */
  public boolean checkAuthorization(PhysicalPlan plan, String username) throws AuthException {
    if (!plan.isAuthenticationRequired()) {
      return true;
    }

    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(
        username, plan.getAuthPaths(), plan.getOperatorType(), targetUser);
  }

  /** Check whether specific Session has the authorization to given plan. */
  public TSStatus checkAuthority(PhysicalPlan plan, IClientSession session) {
    try {
      if (!checkAuthorization(plan, session.getUsername())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation, please add privilege "
                + PrivilegeType.values()[
                    AuthorityChecker.translateToPermissionId(plan.getOperatorType())]);
      }
    } catch (AuthException e) {
      LOGGER.warn("meet error while checking authorization.", e);
      return RpcUtils.getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      return onQueryException(
          e, OperationType.CHECK_AUTHORITY.getName(), TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return null;
  }

  /**
   * this method can be only used in client-thread model.
   *
   * @return
   */
  public IClientSession getCurrSession() {
    return currSession.get();
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
   *
   * @return
   */
  public void removeCurrSession() {
    IClientSession session = currSession.get();
    sessions.remove(session);
    currSession.remove();
  }

  /**
   * this method can be only used in client-thread model. Do not use this method in message-thread
   * model based service.
   *
   * @param session
   * @return false if the session has been initialized.
   */
  public boolean registerSession(IClientSession session) {
    if (this.currSession.get() != null) {
      LOGGER.error("the client session is registered repeatedly, pls check whether this is a bug.");
      return false;
    }
    this.currSession.set(session);
    sessions.put(session, placeHolder);
    return true;
  }

  /**
   * must be called after registerSession()) will mark the session login.
   *
   * @param username
   * @param zoneId
   * @param clientVersion
   */
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

  public boolean hasDataset(Long queryId) {
    return queryIdToDataSet.containsKey(queryId);
  }

  public QueryDataSet getDataset(Long queryId) {
    return queryIdToDataSet.get(queryId);
  }

  public void setDataset(Long queryId, QueryDataSet dataSet) {
    queryIdToDataSet.put(queryId, dataSet);
  }

  public void closeDataset(Long statementId, Long queryId, Consumer<Long> releaseByQueryId) {
    releaseByQueryId.accept(queryId);
    if (statementIdToQueryId.containsKey(statementId)) {
      statementIdToQueryId.get(statementId).remove(queryId);
    }
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

    private SessionManagerHelper() {}
  }
}
