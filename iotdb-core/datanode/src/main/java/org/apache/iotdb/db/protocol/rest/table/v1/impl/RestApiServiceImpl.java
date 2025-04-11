/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.table.v1.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.table.v1.NotFoundException;
import org.apache.iotdb.db.protocol.rest.table.v1.RestApiService;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.ExecuteStatementHandler;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.StatementConstructionHandler;
import org.apache.iotdb.db.protocol.rest.table.v1.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.table.v1.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.table.v1.model.SQL;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

public class RestApiServiceImpl extends RestApiService {
  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Integer defaultQueryRowLimit;

  public RestApiServiceImpl() {
    defaultQueryRowLimit =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestQueryDefaultRowSizeLimit();
  }

  public Response executeQueryStatement(SQL sql, SecurityContext securityContext)
      throws NotFoundException {
    SqlParser relationSqlParser = new SqlParser();
    Long queryId = null;
    Statement statement = null;
    long startTime = System.nanoTime();
    try {
      RequestValidationHandler.validateSQL(sql);
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      clientSession.setDatabaseName(sql.getDatabase());
      clientSession.setSqlDialect(IClientSession.SqlDialect.TABLE);
      statement =
          relationSqlParser.createStatement(sql.getSql(), ZoneId.systemDefault(), clientSession);
      if (statement == null) {
        return Response.ok()
            .entity(
                new org.apache.iotdb.db.protocol.rest.model.ExecutionStatus()
                    .code(TSStatusCode.SQL_PARSE_ERROR.getStatusCode())
                    .message("This operation type is not supported"))
            .build();
      }

      if (ExecuteStatementHandler.validateStatement(statement)) {
        return Response.ok()
            .entity(
                new org.apache.iotdb.db.protocol.rest.model.ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }

      queryId = SESSION_MANAGER.requestQueryId();
      Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              statement,
              relationSqlParser,
              clientSession,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              sql.getSql(),
              metadata,
              config.getQueryTimeoutThreshold(),
              true);
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(result.status.getCode())
                    .message(result.status.getMessage()))
            .build();
      }
      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        return QueryDataSetHandler.fillQueryDataSet(
            queryExecution,
            statement,
            sql.getRowLimit() == null ? defaultQueryRowLimit : sql.getRowLimit());
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(statement)
          .ifPresent(
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.EXECUTE_QUERY_STATEMENT, s.toString(), costTime));
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response insertTablet(
      InsertTabletRequest insertTabletRequest, SecurityContext securityContext)
      throws NotFoundException {
    Long queryId = null;
    long startTime = System.nanoTime();
    InsertTabletStatement insertTabletStatement = null;
    try {
      RequestValidationHandler.validateInsertTabletRequest(insertTabletRequest);
      insertTabletStatement =
          StatementConstructionHandler.constructInsertTabletStatement(insertTabletRequest);
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      clientSession.setDatabaseName(insertTabletRequest.getDatabase());
      clientSession.setSqlDialect(IClientSession.SqlDialect.TABLE);
      queryId = SESSION_MANAGER.requestQueryId();
      Metadata metadata = LocalExecutionPlanner.getInstance().metadata;

      SqlParser relationSqlParser = new SqlParser();
      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              insertTabletStatement,
              relationSqlParser,
              clientSession,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              metadata,
              config.getQueryTimeoutThreshold());

      return responseGenerateHelper(result);
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(insertTabletStatement)
          .ifPresent(
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.INSERT_TABLET, s.getType().name(), costTime));
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext)
      throws NotFoundException {
    SqlParser relationSqlParser = new SqlParser();
    Long queryId = null;
    Statement statement = null;
    long startTime = System.nanoTime();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      RequestValidationHandler.validateSQL(sql);
      clientSession.setDatabaseName(sql.getDatabase());
      clientSession.setSqlDialect(IClientSession.SqlDialect.TABLE);
      statement =
          relationSqlParser.createStatement(sql.getSql(), ZoneId.systemDefault(), clientSession);

      if (statement == null) {
        return Response.ok()
            .entity(
                new org.apache.iotdb.db.protocol.rest.model.ExecutionStatus()
                    .code(TSStatusCode.SQL_PARSE_ERROR.getStatusCode())
                    .message("This operation type is not supported"))
            .build();
      }
      if (!ExecuteStatementHandler.validateStatement(statement)) {
        return Response.ok()
            .entity(
                new org.apache.iotdb.db.protocol.rest.model.ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }
      queryId = SESSION_MANAGER.requestQueryId();
      Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
      ExecutionResult result =
          COORDINATOR.executeForTableModel(
              statement,
              relationSqlParser,
              clientSession,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              sql.getSql(),
              metadata,
              config.getQueryTimeoutThreshold(),
              false);
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(result.status.getCode())
                    .message(result.status.getMessage()))
            .build();
      }
      return responseGenerateHelper(result);
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(statement)
          .ifPresent(
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.EXECUTE_NON_QUERY_PLAN, s.toString(), costTime));
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  private Response responseGenerateHelper(ExecutionResult result) {
    if (result.status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || result.status.code == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      return Response.ok()
          .entity(
              new ExecutionStatus()
                  .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                  .message(TSStatusCode.SUCCESS_STATUS.name()))
          .build();
    } else if (result.status.code == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      List<TSStatus> subStatus = result.status.getSubStatus();
      StringBuilder errMsg = new StringBuilder();
      for (TSStatus status : subStatus) {
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          errMsg.append(status.getMessage()).append("; ");
        }
      }
      return Response.ok()
          .entity(
              new ExecutionStatus()
                  .code(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
                  .message(errMsg.toString()))
          .build();
    } else {
      return Response.ok()
          .entity(
              new ExecutionStatus()
                  .code(result.status.getCode())
                  .message(result.status.getMessage()))
          .build();
    }
  }
}
