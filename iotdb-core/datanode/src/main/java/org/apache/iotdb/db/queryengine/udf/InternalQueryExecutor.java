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

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;

/** Stateless utility for UDF embedded table-model queries. */
public final class InternalQueryExecutor {

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private static final Metadata METADATA = LocalExecutionPlanner.getInstance().metadata;
  private static final SqlParser RELATION_SQL_PARSER = new SqlParser();

  private InternalQueryExecutor() {}

  public static InternalQueryResult executeInternalQuery(
      SessionInfo sessionInfo,
      String fragmentInstanceId,
      QueryId outerQueryId,
      String sql,
      long timeoutMs)
      throws IoTDBException {

    IClientSession previousSession = SESSION_MANAGER.getCurrSession();

    InternalClientSession internalSession =
        new InternalClientSession(formatInternalClientId(fragmentInstanceId, outerQueryId));
    internalSession.setSqlDialect(sessionInfo.getSqlDialect());
    sessionInfo.getDatabaseName().ifPresent(internalSession::setDatabaseName);

    SESSION_MANAGER.supplySession(
        internalSession,
        sessionInfo.getUserId(),
        sessionInfo.getUserName(),
        sessionInfo.getZoneId(),
        sessionInfo.getVersion());

    long statementId = -1;
    long queryId = -1;
    try {
      SESSION_MANAGER.setCurrSession(internalSession);

      statementId = SESSION_MANAGER.requestStatementId(internalSession);
      queryId = SESSION_MANAGER.requestQueryId(internalSession, statementId);

      Statement parsedStatement =
          RELATION_SQL_PARSER.createStatement(sql, sessionInfo.getZoneId(), internalSession);

      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              parsedStatement,
              RELATION_SQL_PARSER,
              internalSession,
              queryId,
              sessionInfo,
              sql,
              METADATA,
              timeoutMs,
              false,
              false,
              true);

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(result.status.message, result.status.code);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      if (queryExecution == null) {
        throw new IoTDBException(
            "Internal query execution not found",
            TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }

      return new InternalQueryResult(queryExecution, internalSession, statementId, queryId, sql);
    } catch (IoTDBException e) {
      ClientRPCServiceImpl.clearUp(internalSession, statementId, queryId, () -> sql, e);
      throw e;
    } catch (IoTDBRuntimeException e) {
      ClientRPCServiceImpl.clearUp(internalSession, statementId, queryId, () -> sql, e);
      throw e;
    } catch (Exception e) {
      ClientRPCServiceImpl.clearUp(internalSession, statementId, queryId, () -> sql, e);
      throw new IoTDBException(e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    } finally {
      SESSION_MANAGER.restoreSession(previousSession, internalSession);
    }
  }

  static String formatInternalClientId(String fragmentInstanceId, QueryId outerQueryId) {
    return String.format("udf-local-%s-%s", fragmentInstanceId, outerQueryId);
  }

  public static void validateReadOnlyQuery(IQueryExecution execution) {
    if (execution.getQueryType() != QueryType.READ) {
      throw new SemanticException("Only query is supported for IoTDBLocal query interface");
    }
  }
}
