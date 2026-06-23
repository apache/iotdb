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

package org.apache.iotdb.db.queryengine.udf;

import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.UDFResultSet;
import org.apache.iotdb.udf.api.exception.UDFException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Server-side implementation of {@link IoTDBLocal}. */
public class IoTDBLocalImpl implements IoTDBLocal {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLocalImpl.class);
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private final SessionInfo sessionInfo;
  private final InternalClientSession internalSession;
  private final long outerQueryStartTimeMs;
  private final long outerQueryTimeoutMs;
  private final List<UDFResultSetImpl> openResultSets = new ArrayList<>();
  private boolean closed;

  public IoTDBLocalImpl(
      SessionInfo sessionInfo,
      String internalClientId,
      long outerQueryStartTimeMs,
      long outerQueryTimeoutMs) {
    this.sessionInfo = sessionInfo;
    this.outerQueryStartTimeMs = outerQueryStartTimeMs;
    this.outerQueryTimeoutMs = outerQueryTimeoutMs;
    this.internalSession = new InternalClientSession(internalClientId);
    internalSession.setSqlDialect(sessionInfo.getSqlDialect());
    sessionInfo.getDatabaseName().ifPresent(internalSession::setDatabaseName);
    SESSION_MANAGER.supplySession(
        internalSession,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getZoneId(),
        ClientVersion.V_1_0);
  }

  public static String formatInternalClientId(
      FragmentInstanceId fragmentInstanceId, PlanNodeId planNodeId) {
    return "udf-local-" + fragmentInstanceId + "-" + planNodeId;
  }

  public static IoTDBLocalImpl create(
      SessionInfo sessionInfo, FragmentInstanceId fragmentInstanceId, PlanNodeId planNodeId) {
    long outerStart = System.currentTimeMillis();
    long outerTimeout =
        org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance()
            .getConfig()
            .getQueryTimeoutThreshold();
    String globalQueryId = fragmentInstanceId.getQueryId().getId();
    for (IQueryExecution execution : COORDINATOR.getAllQueryExecutions()) {
      if (globalQueryId.equals(execution.getQueryId())) {
        outerStart = execution.getStartExecutionTime();
        outerTimeout = execution.getTimeout();
        break;
      }
    }
    return new IoTDBLocalImpl(
        sessionInfo,
        formatInternalClientId(fragmentInstanceId, planNodeId),
        outerStart,
        outerTimeout);
  }

  @Override
  public UDFResultSet query(String sql) throws UDFException {
    if (closed) {
      throw new UDFException("IoTDBLocal is already closed");
    }
    try {
      InternalQueryResult result =
          InternalQueryExecutor.executeInternalQuery(
              internalSession, sessionInfo, sql, outerQueryStartTimeMs, outerQueryTimeoutMs);
      int index = openResultSets.size();
      UDFResultSetImpl rs = new UDFResultSetImpl(result, this, index);
      openResultSets.add(rs);
      return rs;
    } catch (IoTDBException e) {
      throw new UDFException(e.getMessage(), e);
    }
  }

  void markResultSetClosed(int index) {
    if (index >= 0 && index < openResultSets.size()) {
      openResultSets.set(index, null);
    }
  }

  public void closeAllResultSets() {
    for (UDFResultSetImpl rs : openResultSets) {
      if (rs != null) {
        rs.close();
      }
    }
    openResultSets.clear();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    closeAllResultSets();
    SESSION_MANAGER.closeSession(internalSession, COORDINATOR::cleanupQueryExecution);
  }

  @Override
  public void info(String msg) {
    LOGGER.info(msg);
  }

  @Override
  public void info(String format, Object... args) {
    LOGGER.info(format, args);
  }

  @Override
  public void info(String msg, Throwable t) {
    LOGGER.info(msg, t);
  }

  @Override
  public void warn(String msg) {
    LOGGER.warn(msg);
  }

  @Override
  public void warn(String format, Object... args) {
    LOGGER.warn(format, args);
  }

  @Override
  public void warn(String msg, Throwable t) {
    LOGGER.warn(msg, t);
  }

  @Override
  public void error(String msg) {
    LOGGER.error(msg);
  }

  @Override
  public void error(String format, Object... args) {
    LOGGER.error(format, args);
  }

  @Override
  public void error(String msg, Throwable t) {
    LOGGER.error(msg, t);
  }
}
