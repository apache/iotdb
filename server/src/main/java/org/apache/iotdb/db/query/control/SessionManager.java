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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);
  public static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);
  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currSessionId = new ThreadLocal<>();
  // Record the username for every rpc connection (session).
  private final Map<Long, String> sessionIdToUsername = new ConcurrentHashMap<>();
  private final Map<Long, ZoneId> sessionIdToZoneId = new ConcurrentHashMap<>();

  // The sessionId is unique in one IoTDB instance.
  private final AtomicLong sessionIdGenerator = new AtomicLong();
  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  // (sessionId -> Set(statementId))
  private final Map<Long, Set<Long>> sessionIdToStatementId = new ConcurrentHashMap<>();
  // (statementId -> Set(queryId))
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  // (queryId -> QueryDataSet)
  private final Map<Long, QueryDataSet> queryIdToDataSet = new ConcurrentHashMap<>();

  // (sessionId -> client version number)
  private final Map<Long, IoTDBConstant.ClientVersion> sessionIdToClientVersion =
      new ConcurrentHashMap<>();

  // TODO sessionIdToUsername and sessionIdToZoneId should be replaced with this
  private final Map<Long, SessionInfo> sessionIdToSessionInfo = new ConcurrentHashMap<>();

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  protected SessionManager() {
    // singleton
  }

  public BasicOpenSessionResp openSession(
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
        long sessionId = requestSessionId(username, zoneId, clientVersion);

        SessionTimeoutManager.getInstance().register(sessionId);

        openSessionResp
            .sessionId(sessionId)
            .setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .setMessage("Login successfully");

        LOGGER.info(
            "{}: Login status: {}. User : {}, opens Session-{}",
            IoTDBConstant.GLOBAL_DB_NAME,
            openSessionResp.getMessage(),
            username,
            sessionId);
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

  public BasicOpenSessionResp openSession(
      String username, String password, String zoneId, TSProtocolVersion tsProtocolVersion)
      throws TException {
    return openSession(
        username, password, zoneId, tsProtocolVersion, IoTDBConstant.ClientVersion.V_0_12);
  }

  public boolean closeSession(long sessionId) {
    AUDIT_LOGGER.info("Session-{} is closing", sessionId);
    currSessionId.remove();
    return SessionTimeoutManager.getInstance().unregister(sessionId);
  }

  public TSStatus closeOperation(
      long sessionId,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId,
      Consumer<Long> releaseByQueryId) {
    if (!checkLogin(sessionId)) {
      return RpcUtils.getStatus(
          TSStatusCode.NOT_LOGIN_ERROR,
          "Log in failed. Either you are not authorized or the session has timed out.");
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "{}: receive close operation from Session {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          currSessionId);
    }

    try {
      if (haveStatementId) {
        if (haveSetQueryId) {
          this.closeDataset(statementId, queryId, releaseByQueryId);
        } else {
          this.closeStatement(sessionId, statementId, releaseByQueryId);
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
      long sessionId,
      long queryId,
      long statementId,
      boolean haveStatementId,
      boolean haveSetQueryId) {
    return closeOperation(
        sessionId,
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
  public boolean checkLogin(long sessionId) {
    Long currentSessionId = getCurrSessionId();
    boolean isLoggedIn = currentSessionId != null && currentSessionId == sessionId;
    if (!isLoggedIn) {
      LOGGER.info("{}: Not login. ", IoTDBConstant.GLOBAL_DB_NAME);
    } else {
      SessionTimeoutManager.getInstance().refresh(sessionId);
    }
    return isLoggedIn;
  }

  public long requestSessionId(
      String username, String zoneId, IoTDBConstant.ClientVersion clientVersion) {
    long sessionId = sessionIdGenerator.incrementAndGet();

    currSessionId.set(sessionId);
    sessionIdToUsername.put(sessionId, username);
    sessionIdToZoneId.put(sessionId, ZoneId.of(zoneId));
    sessionIdToClientVersion.put(sessionId, clientVersion);
    sessionIdToSessionInfo.put(sessionId, new SessionInfo(sessionId, username, zoneId));

    return sessionId;
  }

  public boolean releaseSessionResource(long sessionId) {
    return releaseSessionResource(sessionId, this::releaseQueryResourceNoExceptions);
  }

  public boolean releaseSessionResource(long sessionId, Consumer<Long> releaseQueryResource) {
    sessionIdToZoneId.remove(sessionId);
    sessionIdToClientVersion.remove(sessionId);

    Set<Long> statementIdSet = sessionIdToStatementId.remove(sessionId);
    if (statementIdSet != null) {
      for (Long statementId : statementIdSet) {
        Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
        if (queryIdSet != null) {
          for (Long queryId : queryIdSet) {
            releaseQueryResource.accept(queryId);
          }
        }
      }
    }

    return sessionIdToUsername.remove(sessionId) != null;
  }

  public long getSessionIdByQueryId(long queryId) {
    // TODO: make this more efficient with a queryId -> sessionId map
    for (Map.Entry<Long, Set<Long>> statementToQueries : statementIdToQueryId.entrySet()) {
      if (statementToQueries.getValue().contains(queryId)) {
        for (Map.Entry<Long, Set<Long>> sessionToStatements : sessionIdToStatementId.entrySet()) {
          if (sessionToStatements.getValue().contains(statementToQueries.getKey())) {
            return sessionToStatements.getKey();
          }
        }
      }
    }
    return -1;
  }

  public long requestStatementId(long sessionId) {
    long statementId = statementIdGenerator.incrementAndGet();
    sessionIdToStatementId
        .computeIfAbsent(sessionId, s -> new CopyOnWriteArraySet<>())
        .add(statementId);
    return statementId;
  }

  public void closeStatement(long sessionId, long statementId, Consumer<Long> releaseByQueryId) {
    Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseByQueryId.accept(queryId);
      }
    }

    if (sessionIdToStatementId.containsKey(sessionId)) {
      sessionIdToStatementId.get(sessionId).remove(statementId);
    }
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
  public TSStatus checkAuthority(PhysicalPlan plan, long sessionId) {
    try {
      if (!checkAuthorization(plan, getUsername(sessionId))) {
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

  public Long getCurrSessionId() {
    return currSessionId.get();
  }

  public TimeZone getCurrSessionTimeZone() {
    if (getCurrSessionId() != null) {
      return TimeZone.getTimeZone(SessionManager.getInstance().getZoneId(getCurrSessionId()));
    } else {
      // only used for test
      return TimeZone.getTimeZone("+08:00");
    }
  }

  public String getUsername(Long sessionId) {
    String username = sessionIdToUsername.get(sessionId);
    if (username == null) {
      throw new RuntimeException(
          new IoTDBException(
              "session expired, please re-login.", TSStatusCode.SESSION_EXPIRED.getStatusCode()));
    }
    return username;
  }

  public ZoneId getZoneId(Long sessionId) {
    return sessionIdToZoneId.get(sessionId);
  }

  public void setTimezone(Long sessionId, String zone) {
    sessionIdToZoneId.put(sessionId, ZoneId.of(zone));
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

  public void removeDataset(Long queryId) {
    queryIdToDataSet.remove(queryId);
  }

  public void closeDataset(Long statementId, Long queryId, Consumer<Long> releaseByQueryId) {
    releaseByQueryId.accept(queryId);
    if (statementIdToQueryId.containsKey(statementId)) {
      statementIdToQueryId.get(statementId).remove(queryId);
    }
  }

  public IoTDBConstant.ClientVersion getClientVersion(Long sessionId) {
    return sessionIdToClientVersion.get(sessionId);
  }

  public static SessionManager getInstance() {
    return SessionManagerHelper.INSTANCE;
  }

  public SessionInfo getSessionInfo(long sessionId) {
    return sessionIdToSessionInfo.get(sessionId);
  }

  private static class SessionManagerHelper {

    private static final SessionManager INSTANCE = new SessionManager();

    private SessionManagerHelper() {}
  }
}
