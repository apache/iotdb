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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.protocol.rest.handler.AuthorizationHandler;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.db.protocol.rest.utils.InsertTabletSortDataUtils;
import org.apache.iotdb.db.protocol.rest.v2.NotFoundException;
import org.apache.iotdb.db.protocol.rest.v2.RestApiService;
import org.apache.iotdb.db.protocol.rest.v2.handler.ExceptionHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.ExecuteStatementHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.FastLastHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.RequestValidationHandler;
import org.apache.iotdb.db.protocol.rest.v2.handler.StatementConstructionHandler;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertRecordsRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.InsertTabletRequest;
import org.apache.iotdb.db.protocol.rest.v2.model.PrefixPathList;
import org.apache.iotdb.db.protocol.rest.v2.model.QueryDataSet;
import org.apache.iotdb.db.protocol.rest.v2.model.SQL;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  public Response executeFastLastQueryStatement(
      PrefixPathList prefixPathList, SecurityContext securityContext) {
    Long queryId = null;
    Statement statement = null;
    boolean finish = false;
    long startTime = System.nanoTime();

    try {
      RequestValidationHandler.validatePrefixPaths(prefixPathList);

      PartialPath prefixPath =
          new PartialPath(prefixPathList.getPrefixPaths().toArray(new String[0]));
      final Map<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>> resultMap =
          new HashMap<>();
      int sensorNum = 0;

      final String prefixString = prefixPath.toString();
      for (final ISchemaRegion region : SchemaEngine.getInstance().getAllSchemaRegions()) {
        if (!prefixString.startsWith(region.getDatabaseFullPath())
            && !region.getDatabaseFullPath().startsWith(prefixString)) {
          continue;
        }
        sensorNum += region.fillLastQueryMap(prefixPath, resultMap);
      }
      // Check cache first
      if (!TableDeviceSchemaCache.getInstance().getLastCache(resultMap)) {
        IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
        TSLastDataQueryReq tsLastDataQueryReq =
            FastLastHandler.createTSLastDataQueryReq(clientSession, prefixPathList);
        statement = StatementGenerator.createStatement(tsLastDataQueryReq);

        if (ExecuteStatementHandler.validateStatement(statement)) {
          return FastLastHandler.buildErrorResponse(TSStatusCode.EXECUTE_STATEMENT_ERROR);
        }

        Optional.ofNullable(authorizationHandler.checkAuthority(securityContext, statement))
            .ifPresent(Response.class::cast);

        queryId = SESSION_MANAGER.requestQueryId();
        SessionInfo sessionInfo = SESSION_MANAGER.getSessionInfo(clientSession);

        ExecutionResult result =
            COORDINATOR.executeForTreeModel(
                statement,
                queryId,
                sessionInfo,
                "",
                partitionFetcher,
                schemaFetcher,
                config.getQueryTimeoutThreshold(),
                true);

        finish = true;

        if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          return FastLastHandler.buildExecutionStatusResponse(result.status);
        }

        IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
        try (SetThreadName ignored = new SetThreadName(result.queryId.getId())) {
          return QueryDataSetHandler.fillQueryDataSet(
              queryExecution, statement, defaultQueryRowLimit);
        }
      }

      // Cache hit: build response directly
      QueryDataSet targetDataSet = new QueryDataSet();

      FastLastHandler.setupTargetDataSet(targetDataSet);
      List<Object> timeseries = new ArrayList<>();
      List<Object> valueList = new ArrayList<>();
      List<Object> dataTypeList = new ArrayList<>();

      for (final Map.Entry<TableId, Map<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>>
          result : resultMap.entrySet()) {
        for (final Map.Entry<IDeviceID, Map<String, Pair<TSDataType, TimeValuePair>>>
            device2MeasurementLastEntry : result.getValue().entrySet()) {
          final String deviceWithSeparator =
              device2MeasurementLastEntry.getKey().toString() + TsFileConstant.PATH_SEPARATOR;
          for (final Map.Entry<String, Pair<TSDataType, TimeValuePair>> measurementLastEntry :
              device2MeasurementLastEntry.getValue().entrySet()) {
            final TimeValuePair tvPair = measurementLastEntry.getValue().getRight();
            valueList.add(tvPair.getValue().getStringValue());
            dataTypeList.add(tvPair.getValue().getDataType().name());
            targetDataSet.addTimestampsItem(tvPair.getTimestamp());
            timeseries.add(deviceWithSeparator + measurementLastEntry.getKey());
          }
        }
      }
      if (!timeseries.isEmpty()) {
        targetDataSet.addValuesItem(timeseries);
        targetDataSet.addValuesItem(valueList);
        targetDataSet.addValuesItem(dataTypeList);
      }
      return Response.ok().entity(targetDataSet).build();

    } catch (Exception e) {
      finish = true;
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(statement)
          .ifPresent(
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.EXECUTE_QUERY_STATEMENT, s.getType().name(), costTime));
      if (queryId != null) {
        if (finish) {
          long executionTime = COORDINATOR.getTotalExecutionTime(queryId);
          CommonUtils.addQueryLatency(
              statement != null ? statement.getType() : null,
              executionTime > 0 ? executionTime : costTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response executeNonQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    Statement statement = null;
    long startTime = System.nanoTime();
    boolean finish = false;
    try {
      RequestValidationHandler.validateSQL(sql);
      statement = StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());
      if (statement == null) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.SQL_PARSE_ERROR.getStatusCode())
                    .message("This operation type is not supported"))
            .build();
      }
      if (!ExecuteStatementHandler.validateStatement(statement)) {
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
      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              sql.getSql(),
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold(),
              false);
      finish = true;
      return responseGenerateHelper(result);
    } catch (Exception e) {
      finish = true;
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      if (statement != null)
        CommonUtils.addStatementExecutionLatency(
            OperationType.EXECUTE_NON_QUERY_PLAN, statement.getType().name(), costTime);
      if (queryId != null) {
        if (finish) {
          long executionTime = COORDINATOR.getTotalExecutionTime(queryId);
          CommonUtils.addQueryLatency(
              statement.getType(), executionTime > 0 ? executionTime : costTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response executeQueryStatement(SQL sql, SecurityContext securityContext) {
    Long queryId = null;
    Statement statement = null;
    long startTime = System.nanoTime();
    boolean finish = false;
    try {
      RequestValidationHandler.validateSQL(sql);
      statement = StatementGenerator.createStatement(sql.getSql(), ZoneId.systemDefault());

      if (statement == null) {
        return Response.ok()
            .entity(
                new ExecutionStatus()
                    .code(TSStatusCode.SQL_PARSE_ERROR.getStatusCode())
                    .message("This operation type is not supported"))
            .build();
      }

      if (ExecuteStatementHandler.validateStatement(statement)) {
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
              config.getQueryTimeoutThreshold(),
              true);
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
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.EXECUTE_QUERY_STATEMENT, s.getType().name(), costTime));
      if (queryId != null) {
        if (finish) {
          long executionTime = COORDINATOR.getTotalExecutionTime(queryId);
          CommonUtils.addQueryLatency(
              statement.getType(), executionTime > 0 ? executionTime : costTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  @Override
  public Response insertRecords(
      InsertRecordsRequest insertRecordsRequest, SecurityContext securityContext)
      throws NotFoundException {
    Long queryId = null;
    long startTime = System.nanoTime();
    InsertRowsStatement insertRowsStatement = null;
    try {
      RequestValidationHandler.validateInsertRecordsRequest(insertRecordsRequest);

      insertRowsStatement =
          StatementConstructionHandler.createInsertRowsStatement(insertRecordsRequest);

      Response response = authorizationHandler.checkAuthority(securityContext, insertRowsStatement);
      if (response != null) {
        return response;
      }
      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.executeForTreeModel(
              insertRowsStatement,
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold(),
              false);
      return responseGenerateHelper(result);

    } catch (Exception e) {
      return Response.ok().entity(ExceptionHandler.tryCatchException(e)).build();
    } finally {
      long costTime = System.nanoTime() - startTime;
      Optional.ofNullable(insertRowsStatement)
          .ifPresent(
              s ->
                  CommonUtils.addStatementExecutionLatency(
                      OperationType.INSERT_RECORDS, s.getType().name(), costTime));
      if (queryId != null) {
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
              SESSION_MANAGER.requestQueryId(),
              SESSION_MANAGER.getSessionInfo(SESSION_MANAGER.getCurrSession()),
              "",
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold(),
              false);
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
