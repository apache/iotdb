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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
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

  public Long getCurrSessionId() {
    return currSessionId.get();
  }

  public void removeCurrSessionId() {
    currSessionId.remove();
  }

  public TimeZone getCurrSessionTimeZone() {
    if (getCurrSessionId() != null) {
      return TimeZone.getTimeZone(SessionManager.getInstance().getZoneId(getCurrSessionId()));
    } else {
      // only used for test
      return TimeZone.getTimeZone("+08:00");
    }
  }

  public BasicOpenSessionResp openSession(
      String username,
      String password,
      String zoneId,
      TSProtocolVersion tsProtocolVersion,
      IoTDBConstant.ClientVersion clientVersion)
      throws TException {
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    String loginMessage = null;
    try {
      status = authorizer.login(username, password);
    } catch (AuthException e) {
      LOGGER.info("meet error while logging in.", e);
      status = false;
      loginMessage = e.getMessage();
    }

    long sessionId = -1;
    if (status) {
      // check the version compatibility
      boolean compatible = tsProtocolVersion.equals(CURRENT_RPC_VERSION);
      if (!compatible) {
        openSessionResp.setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
        openSessionResp.setMessage(
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        return openSessionResp.sessionId(sessionId);
      }

      openSessionResp.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      openSessionResp.setMessage("Login successfully");

      sessionId = requestSessionId(username, zoneId, clientVersion);

      LOGGER.info(
          "{}: Login status: {}. User : {}, opens Session-{}",
          IoTDBConstant.GLOBAL_DB_NAME,
          openSessionResp.getMessage(),
          username,
          sessionId);
    } else {
      openSessionResp.setMessage(loginMessage != null ? loginMessage : "Authentication failed.");
      openSessionResp.setCode(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR.getStatusCode());

      sessionId = requestSessionId(username, zoneId, clientVersion);
      AUDIT_LOGGER.info("User {} opens Session failed with an incorrect password", username);
    }

    SessionTimeoutManager.getInstance().register(sessionId);
    return openSessionResp.sessionId(sessionId);
  }

  public boolean closeSession(long sessionId) {
    AUDIT_LOGGER.info("Session-{} is closing", sessionId);

    removeCurrSessionId();

    return SessionTimeoutManager.getInstance().unregister(sessionId);
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  public boolean checkLogin(long sessionId) {
    boolean isLoggedIn = getUsername(sessionId) != null;
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

    return sessionId;
  }

  public boolean releaseSessionResource(long sessionId) {
    sessionIdToZoneId.remove(sessionId);
    sessionIdToClientVersion.remove(sessionId);

    Set<Long> statementIdSet = sessionIdToStatementId.remove(sessionId);
    if (statementIdSet != null) {
      for (Long statementId : statementIdSet) {
        Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
        if (queryIdSet != null) {
          for (Long queryId : queryIdSet) {
            releaseQueryResourceNoExceptions(queryId);
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

  public void closeStatement(long sessionId, long statementId) {
    Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseQueryResourceNoExceptions(queryId);
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

  public String getUsername(Long sessionId) {
    return sessionIdToUsername.get(sessionId);
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

  public void closeDataset(Long statementId, Long queryId) {
    releaseQueryResourceNoExceptions(queryId);
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
