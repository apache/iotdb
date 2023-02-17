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

package org.apache.iotdb.db.protocol.rest.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStatement;
import org.apache.iotdb.db.protocol.rest.GrafanaApiService;
import org.apache.iotdb.db.protocol.rest.NotFoundException;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.model.ExpressionRequest;
import org.apache.iotdb.db.protocol.rest.model.SQL;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;
import java.util.List;

public class GrafanaApiServiceImpl extends GrafanaApiService {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;
  private final AuthorizationHandler authorizationHandler;

  private final long timePrecision; // the default timestamp precision is ms

  public GrafanaApiServiceImpl() {
    PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
    SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();
    authorizationHandler = new AuthorizationHandler();

    switch (IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision()) {
      case "ns":
        timePrecision = 1000000;
        break;
      case "us":
        timePrecision = 1000;
        break;
      default:
        timePrecision = 1;
    }
  }

  @Override
  public Response variables(SQL sql, SecurityContext securityContext) {
    try {
      RequestValidationHandler.validateSQL(sql);

      Statement statement =
          StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());
      if (!(statement instanceof ShowStatement) && !(statement instanceof QueryStatement)) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }

      Response response = authorizationHandler.checkAuthority(securityContext, statement);
      if (response != null) {
        return response;
      }

      final long queryId = SESSION_MANAGER.requestQueryId();
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              null,
              sql.getSql(),
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
              config.getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }
      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        return QueryDataSetHandler.fillGrafanaVariablesResult(queryExecution, statement);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response expression(ExpressionRequest expressionRequest, SecurityContext securityContext)
      throws NotFoundException {
    try {
      RequestValidationHandler.validateExpressionRequest(expressionRequest);

      final String expression = Joiner.on(",").join(expressionRequest.getExpression());
      final String prefixPaths = Joiner.on(",").join(expressionRequest.getPrefixPath());
      final long startTime =
          (long) (expressionRequest.getStartTime().doubleValue() * timePrecision);
      final long endTime = (long) (expressionRequest.getEndTime().doubleValue() * timePrecision);
      String sql =
          "select "
              + expression
              + " from "
              + prefixPaths
              + " where timestamp>="
              + startTime
              + " and timestamp<= "
              + endTime;
      if (StringUtils.isNotEmpty(expressionRequest.getCondition())) {
        sql += " and " + expressionRequest.getCondition();
      }
      if (StringUtils.isNotEmpty(expressionRequest.getControl())) {
        sql += " " + expressionRequest.getControl();
      }

      Statement statement = StatementGenerator.createStatement(sql, ZoneId.systemDefault());

      Response response = authorizationHandler.checkAuthority(securityContext, statement);
      if (response != null) {
        return response;
      }

      final long queryId = SESSION_MANAGER.requestQueryId();
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              null,
              sql,
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
              config.getQueryTimeoutThreshold());
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                    .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
            .build();
      }
      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        if (((QueryStatement) statement).isGroupByLevel()) {
          return QueryDataSetHandler.fillAggregationPlanDataSet(queryExecution, 0);
        } else {
          return QueryDataSetHandler.fillDataSetWithTimestamps(queryExecution, 0, timePrecision);
        }
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }

  @Override
  public Response login(SecurityContext securityContext) throws NotFoundException {
    return Response.ok()
        .entity(
            new ExecutionStatus()
                .code(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                .message(TSStatusCode.SUCCESS_STATUS.name()))
        .build();
  }

  @Override
  public Response node(List<String> requestBody, SecurityContext securityContext)
      throws NotFoundException {
    try {
      if (requestBody != null && !requestBody.isEmpty()) {
        PartialPath path = new PartialPath(Joiner.on(".").join(requestBody));
        String sql = "show child paths " + path;
        Statement statement = StatementGenerator.createStatement(sql, ZoneId.systemDefault());

        Response response = authorizationHandler.checkAuthority(securityContext, statement);
        if (response != null) {
          return response;
        }

        final long queryId = SESSION_MANAGER.requestQueryId();
        // create and cache dataset
        ExecutionResult result =
            COORDINATOR.execute(
                statement,
                queryId,
                null,
                sql,
                PARTITION_FETCHER,
                SCHEMA_FETCHER,
                config.getQueryTimeoutThreshold());
        if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          return Response.ok()
              .entity(
                  new ExecutionStatus()
                      .code(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                      .message(TSStatusCode.EXECUTE_STATEMENT_ERROR.name()))
              .build();
        }
        IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

        try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
          return QueryDataSetHandler.fillGrafanaNodesResult(queryExecution);
        }
      } else {
        return QueryDataSetHandler.fillGrafanaNodesResult(null);
      }
    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    }
  }
}
