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
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
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

  public static long computeRemainingTimeoutMs(
      long outerQueryStartTimeMs, long outerQueryTimeoutMs) {
    return outerQueryTimeoutMs - (System.currentTimeMillis() - outerQueryStartTimeMs);
  }

  public static InternalQueryResult executeInternalQuery(
      IClientSession internalSession,
      SessionInfo sessionInfo,
      String sql,
      long outerQueryStartTimeMs,
      long outerQueryTimeoutMs)
      throws IoTDBException {

    long timeoutMs = computeRemainingTimeoutMs(outerQueryStartTimeMs, outerQueryTimeoutMs);
    if (timeoutMs <= 0) {
      throw new QueryTimeoutException(
          "Outer query timeout exceeded before UDF internal query starts");
    }

    Statement parsedStatement = parseTableStatement(internalSession, sessionInfo, sql);

    long statementId = SESSION_MANAGER.requestStatementId(internalSession);
    long queryId = SESSION_MANAGER.requestQueryId(internalSession, statementId);

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

    if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      COORDINATOR.cleanupQueryExecution(queryId, () -> sql, null);
      internalSession.removeQueryId(statementId, queryId);
      throw new IoTDBException(result.status.message, result.status.code);
    }

    IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
    if (queryExecution == null) {
      COORDINATOR.cleanupQueryExecution(queryId, () -> sql, null);
      internalSession.removeQueryId(statementId, queryId);
      throw new IoTDBException(
          "Internal query execution not found", TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    return new InternalQueryResult(
        queryExecution,
        () -> {
          COORDINATOR.cleanupQueryExecution(queryId, () -> sql, null);
          internalSession.removeQueryId(statementId, queryId);
        });
  }

  static void validateReadOnlyQuery(IQueryExecution execution) throws IoTDBException {
    if (execution.getQueryType() != QueryType.READ) {
      execution.stopAndCleanup(null);
      throw new SemanticException("Only query is allowed when used IoTDBLocal in UDF");
    }
  }

  private static Statement parseTableStatement(
      IClientSession internalSession, SessionInfo sessionInfo, String sql) throws IoTDBException {
    try {
      Statement statement =
          RELATION_SQL_PARSER.createStatement(sql, sessionInfo.getZoneId(), internalSession);
      return statement;
    } catch (Exception e) {
      throw new IoTDBException(e.getMessage(), TSStatusCode.SQL_PARSE_ERROR.getStatusCode());
    }
  }
}
