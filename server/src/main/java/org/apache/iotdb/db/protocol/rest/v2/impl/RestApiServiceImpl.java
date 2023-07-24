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

package org.apache.iotdb.db.protocol.rest.v2.impl;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.utils.InsertTabletSortDataUtils;
import org.apache.iotdb.db.protocol.rest.v2.RestApiService;
import org.apache.iotdb.db.protocol.rest.v2.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.ExecuteStatementHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.StatementConstructionHandler;
import org.apache.iotdb.db.protocol.rest.v2.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.SQL;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;

public class RestApiServiceImpl extends RestApiService {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;
  private final AuthorizationHandler authorizationHandler;

  private final Integer defaultQueryRowLimit;

  public RestApiServiceImpl() {
    PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
    SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();
    authorizationHandler = new AuthorizationHandler();
    defaultQueryRowLimit =
        IoTDBRestServiceDescriptor.getInstance().getConfig().getRestQueryDefaultRowSizeLimit();
  }

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    try {
      RequestValidationHandler.validateSQL(sql);

      Statement statement =
          StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());

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
          COORDINATOR.execute(
              statement,
              queryId,
              null,
              sql.getSql(),
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
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
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    try {
      RequestValidationHandler.validateSQL(sql);

      Statement statement =
          StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());

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
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response insertTablet(
      InsertTabletRequest insertTabletRequest, SecurityContext securityContext) {
    Long queryId = null;
    try {
      RequestValidationHandler.validateInsertTabletRequest(insertTabletRequest);

      if (!InsertTabletSortDataUtils.checkSorted(insertTabletRequest.getTimestamps())) {
        Integer[] index = new Integer[insertTabletRequest.getTimestamps().size()];
        for (int i = 0; i < index.length; i++) {
          index[i] = i;
        }
        Arrays.sort(index, Comparator.comparingLong(insertTabletRequest.getTimestamps()::get));
        insertTabletRequest.getTimestamps().sort(Long::compareTo);
        insertTabletRequest.setValues(
            InsertTabletSortDataUtils.sortList(
                insertTabletRequest.getValues(), index, insertTabletRequest.getDataTypes().size()));
      }

      InsertTabletStatement insertTabletStatement =
          StatementConstructionHandler.constructInsertTabletStatement(insertTabletRequest);

      Response response =
          authorizationHandler.checkAuthority(securityContext, insertTabletStatement);
      if (response != null) {
        return response;
      }
      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              insertTabletStatement,
              queryId,
              null,
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
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
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }
}
