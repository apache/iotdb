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

package org.apache.iotdb.db.protocol.rest.v1.impl;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.utils.InsertTabletSortDataUtils;
import org.apache.iotdb.db.protocol.rest.v1.RestApiService;
import org.apache.iotdb.db.protocol.rest.v1.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.v1.handler.ExecuteStatementHandler;
import org.apache.iotdb.db.protocol.rest.v1.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.v1.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.v1.handler.StatementConstructionHandler;
import org.apache.iotdb.db.protocol.rest.v1.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.v1.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.v1.model.SQL;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;
import java.util.Optional;

public class RestApiServiceImpl extends RestApiService {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;
  private final AuthorizationHandler authorizationHandler;

  private final Integer defaultQueryRowLimit;

  public RestApiServiceImpl() {
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
    authorizationHandler = new AuthorizationHandler();
    defaultQueryRowLimit =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestQueryDefaultRowSizeLimit();
  }

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    long startTime = System.nanoTime();
    boolean finish = false;
    Statement statement = null;
    try {
      RequestValidationHandler.validateSQL(sql);
      statement = StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());
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

      Response response = authorizationHandler.checkAuthority(securityContext, statement);
      if (response != null) {
        return response;
      }
      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              sql.getSql(),
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold());
      finish = true;
      return Response.ok()
          .entity(
              (result.status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                      || result.status.code == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(result.status.getCode())
                      .message(result.status.getMessage()))
          .build();
    } catch (Exception e) {
      finish = true;
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(statement)
          .ifPresent(
              s -> {
                CommonUtils.addStatementExecutionLatency(
                    OperationType.EXECUTE_NON_QUERY_PLAN, s.getType().name(), costTime);
              });
      if (queryId != null) {
        if (finish) {
          long executeTime = COORDINATOR.getTotalExecutionTime(queryId);
          CommonUtils.addQueryLatency(
              statement.getType(), executeTime > 0 ? executeTime : costTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    long startTime = System.nanoTime();
    boolean finish = false;
    Statement statement = null;
    try {
      RequestValidationHandler.validateSQL(sql);
      statement = StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());
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

      Response response = authorizationHandler.checkAuthority(securityContext, statement);
      if (response != null) {
        return response;
      }
      queryId = SESSION_MANAGER.requestQueryId();
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              sql.getSql(),
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold());
      finish = true;
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
      finish = true;
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(statement)
          .ifPresent(
              s -> {
                CommonUtils.addStatementExecutionLatency(
                    OperationType.EXECUTE_QUERY_STATEMENT, s.getType().name(), costTime);
              });
      if (queryId != null) {
        if (finish) {
          long executeTime = COORDINATOR.getTotalExecutionTime(queryId);
          CommonUtils.addQueryLatency(
              statement.getType(), executeTime > 0 ? executeTime : costTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response insertTablet(
      InsertTabletRequest insertTabletRequest, SecurityContext securityContext) {
    Long queryId = null;
    long startTime = System.nanoTime();
    InsertTabletStatement insertTabletStatement = null;
    try {
      RequestValidationHandler.validateInsertTabletRequest(insertTabletRequest);

      if (!InsertTabletSortDataUtils.checkSorted(insertTabletRequest.getTimestamps())) {

        int[] index =
            InsertTabletSortDataUtils.sortTimeStampList(insertTabletRequest.getTimestamps());
        insertTabletRequest.getTimestamps().sort(Long::compareTo);
        insertTabletRequest.setValues(
            InsertTabletSortDataUtils.sortList(
                insertTabletRequest.getValues(), index, insertTabletRequest.getDataTypes().size()));
      }
      startTime = System.nanoTime();
      insertTabletStatement =
          StatementConstructionHandler.constructInsertTabletStatement(insertTabletRequest);

      Response response =
          authorizationHandler.checkAuthority(securityContext, insertTabletStatement);
      if (response != null) {
        return response;
      }
      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              insertTabletStatement,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold());

      return Response.ok()
          .entity(
              (result.status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()
                      || result.status.code == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                  ? new ExecutionStatus()
                      .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                      .message(TSStatusCode.SUCCESS_STATUS.name())
                  : new ExecutionStatus()
                      .code(result.status.getCode())
                      .message(result.status.getMessage()))
          .build();
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(insertTabletStatement)
          .ifPresent(
              s -> {
                CommonUtils.addStatementExecutionLatency(
                    OperationType.INSERT_TABLET, s.getType().name(), costTime);
              });
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }
}
