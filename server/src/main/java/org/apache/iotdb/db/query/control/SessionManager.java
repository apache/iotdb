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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Session Manager is for mananging active sessions. It will be used by both Thrift based services
 * (i.e., TSServiceImpl and InfluxdbService) and Mqtt based service. <br>
 * Thrift based services are client-thread model, i.e., each client has a thread. So we can use
 * threadLocal for such services.<br>
 * However, Mqtt based service use message-thread model, i.e, each message has a short thread. So,
 * we can not use threadLocal for such services.
 */
public class SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

  // When the client abnormally exits, we can still know who to disconnect
  /** currSession can be only used in client-thread model services. */
  private final ThreadLocal<IClientSession> currSession = new ThreadLocal<>();

  // we keep this sessionIdGenerator just for keep Compatible with v0.13
  @Deprecated private final AtomicLong sessionIdGenerator = new AtomicLong();

  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  // (sessionId -> Set(statementId))
  private final Map<IClientSession, Set<Long>> sessionToStatementId = new ConcurrentHashMap<>();
  // (statementId -> Set(queryId))
  private final Map<Long, Set<Long>> statementIdToQueryId = new ConcurrentHashMap<>();
  // (queryId -> QueryDataSet)
  private final Map<Long, QueryDataSet> queryIdToDataSet = new ConcurrentHashMap<>();

  protected SessionManager() {
    // singleton
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
  }

  /**
   * @param session
   * @return true if releasing successfully, false otherwise (e.g., the session does not exist)
   */
  public boolean releaseSessionResource(IClientSession session) {
    Set<Long> statementIdSet = sessionToStatementId.remove(session);
    if (statementIdSet != null) {
      for (Long statementId : statementIdSet) {
        Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
        if (queryIdSet != null) {
          for (Long queryId : queryIdSet) {
            releaseQueryResourceNoExceptions(queryId);
          }
        }
      }
      return true;
    }
    // TODO if there is no statement for the session, how to return (true or false?)
    return false;
  }

  /**
   * @param queryId
   * @return null if not found. (e.g., the client session is closed already. TODO: do we really have
   *     this case?)
   */
  public IClientSession getSessionIdByQueryId(long queryId) {
    // TODO: make this more efficient with a queryId -> sessionId map
    for (Map.Entry<Long, Set<Long>> statementToQueries : statementIdToQueryId.entrySet()) {
      if (statementToQueries.getValue().contains(queryId)) {
        for (Map.Entry<IClientSession, Set<Long>> sessionToStatements :
            sessionToStatementId.entrySet()) {
          if (sessionToStatements.getValue().contains(statementToQueries.getKey())) {
            return sessionToStatements.getKey();
          }
        }
      }
    }
    return null;
  }

  public long requestStatementId(IClientSession session) {
    long statementId = statementIdGenerator.incrementAndGet();
    sessionToStatementId
        .computeIfAbsent(session, s -> new CopyOnWriteArraySet<>())
        .add(statementId);
    return statementId;
  }

  public void closeStatement(IClientSession session, long statementId) {
    Set<Long> queryIdSet = statementIdToQueryId.remove(statementId);
    if (queryIdSet != null) {
      for (Long queryId : queryIdSet) {
        releaseQueryResourceNoExceptions(queryId);
      }
    }

    if (sessionToStatementId.containsKey(session)) {
      sessionToStatementId.get(session).remove(statementId);
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

  public static SessionManager getInstance() {
    return SessionManagerHelper.INSTANCE;
  }

  private static class SessionManagerHelper {

    private static final SessionManager INSTANCE = new SessionManager();

    private SessionManagerHelper() {}
  }
}
