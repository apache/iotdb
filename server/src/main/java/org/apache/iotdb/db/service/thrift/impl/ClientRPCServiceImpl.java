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
package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.audit.AuditLogger;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.metadata.template.TemplateQueryType;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.ASTVisitor;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSAggregationQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSAppendSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSDropSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onIoTDBException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class ClientRPCServiceImpl implements IClientRPCServiceWithHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientRPCServiceImpl.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  private static final boolean enableAuditLog = config.isEnableAuditLog();

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  @FunctionalInterface
  public interface SelectResult {
    boolean apply(TSExecuteStatementResp resp, IQueryExecution queryExecution, int fetchSize)
        throws IoTDBException, IOException;
  }

  private static final SelectResult SELECT_RESULT =
      (resp, queryExecution, fetchSize) -> {
        Pair<List<ByteBuffer>, Boolean> pair =
            QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, fetchSize);
        resp.setQueryResult(pair.left);
        return pair.right;
      };

  private static final SelectResult OLD_SELECT_RESULT =
      (resp, queryExecution, fetchSize) -> {
        Pair<TSQueryDataSet, Boolean> pair =
            QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, fetchSize);
        resp.setQueryDataSet(pair.left);
        return pair.right;
      };

  public ClientRPCServiceImpl() {
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  private TSExecuteStatementResp executeStatementInternal(
      TSExecuteStatementReq req, SelectResult setResult) {
    boolean finished = false;
    long queryId = Long.MIN_VALUE;
    String statement = req.getStatement();
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }

    long startTime = System.currentTimeMillis();
    StatementType statementType = null;
    try {
      Statement s = StatementGenerator.createStatement(statement, clientSession.getZoneId());

      if (s == null) {
        return RpcUtils.getTSExecuteStatementResp(
            RpcUtils.getStatus(
                TSStatusCode.SQL_PARSE_ERROR, "This operation type is not supported"));
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      statementType = s.getType();
      if (enableAuditLog) {
        AuditLogger.log(statement, s);
      }

      queryId = SESSION_MANAGER.requestQueryId(clientSession, req.statementId);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              statement,
              partitionFetcher,
              schemaFetcher,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        TSExecuteStatementResp resp;
        if (queryExecution != null && queryExecution.isQuery()) {
          resp = createResponse(queryExecution.getDatasetHeader(), queryId);
          resp.setStatus(result.status);
          finished = setResult.apply(resp, queryExecution, req.fetchSize);
          resp.setMoreData(!finished);
        } else {
          finished = true;
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_STATEMENT));
    } finally {
      COORDINATOR.recordExecutionTime(queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        if (statementType != null) {
          long executionTime = COORDINATOR.getTotalExecutionTime(queryId);
          addStatementExecutionLatency(
              OperationType.EXECUTE_STATEMENT,
              statementType,
              executionTime > 0 ? executionTime : System.currentTimeMillis() - startTime);
        }
        COORDINATOR.cleanupQueryExecution(queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  private TSExecuteStatementResp executeRawDataQueryInternal(
      TSRawDataQueryReq req, SelectResult setResult) {
    boolean finished = false;
    long queryId = Long.MIN_VALUE;
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }
    long startTime = System.currentTimeMillis();
    try {
      Statement s = StatementGenerator.createStatement(req, clientSession.getZoneId());

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      if (enableAuditLog) {
        AuditLogger.log(String.format("execute Raw Data Query: %s", req), s);
      }
      queryId = SESSION_MANAGER.requestQueryId(clientSession, req.statementId);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException("error code: " + result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        TSExecuteStatementResp resp;
        if (queryExecution.isQuery()) {
          resp = createResponse(queryExecution.getDatasetHeader(), queryId);
          resp.setStatus(result.status);
          finished = setResult.apply(resp, queryExecution, req.fetchSize);
          resp.setMoreData(!finished);
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + req + "\". " + OperationType.EXECUTE_RAW_DATA_QUERY));
    } finally {
      COORDINATOR.recordExecutionTime(queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        addStatementExecutionLatency(
            OperationType.EXECUTE_RAW_DATA_QUERY,
            StatementType.QUERY,
            COORDINATOR.getTotalExecutionTime(queryId));
        COORDINATOR.cleanupQueryExecution(queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  private TSExecuteStatementResp executeLastDataQueryInternal(
      TSLastDataQueryReq req, SelectResult setResult) {
    boolean finished = false;
    long queryId = Long.MIN_VALUE;
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }
    long startTime = System.currentTimeMillis();
    try {
      Statement s = StatementGenerator.createStatement(req, clientSession.getZoneId());
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      if (enableAuditLog) {
        AuditLogger.log(String.format("Last Data Query: %s", req), s);
      }
      queryId = SESSION_MANAGER.requestQueryId(clientSession, req.statementId);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException("error code: " + result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        TSExecuteStatementResp resp;
        if (queryExecution.isQuery()) {
          resp = createResponse(queryExecution.getDatasetHeader(), queryId);
          resp.setStatus(result.status);
          finished = setResult.apply(resp, queryExecution, req.fetchSize);
          resp.setMoreData(!finished);
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }

    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + req + "\". " + OperationType.EXECUTE_LAST_DATA_QUERY));
    } finally {
      COORDINATOR.recordExecutionTime(queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        addStatementExecutionLatency(
            OperationType.EXECUTE_LAST_DATA_QUERY,
            StatementType.QUERY,
            COORDINATOR.getTotalExecutionTime(queryId));
        COORDINATOR.cleanupQueryExecution(queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  private TSExecuteStatementResp executeAggregationQueryInternal(
      TSAggregationQueryReq req, SelectResult setResult) {
    boolean finished = false;
    long queryId = Long.MIN_VALUE;
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }
    long startTime = System.currentTimeMillis();
    try {
      Statement s = StatementGenerator.createStatement(req, clientSession.getZoneId());
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      queryId = SESSION_MANAGER.requestQueryId(clientSession, req.statementId);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException("error code: " + result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        TSExecuteStatementResp resp;
        if (queryExecution.isQuery()) {
          resp = createResponse(queryExecution.getDatasetHeader(), queryId);
          resp.setStatus(result.status);
          finished = setResult.apply(resp, queryExecution, req.fetchSize);
          resp.setMoreData(!finished);
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }

    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + req + "\". " + OperationType.EXECUTE_LAST_DATA_QUERY));
    } finally {
      COORDINATOR.recordExecutionTime(queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        addStatementExecutionLatency(
            OperationType.EXECUTE_LAST_DATA_QUERY,
            StatementType.QUERY,
            COORDINATOR.getTotalExecutionTime(queryId));
        COORDINATOR.cleanupQueryExecution(queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatementV2(TSExecuteStatementReq req) {
    return executeStatementV2(req);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatementV2(TSExecuteStatementReq req) {
    return executeStatementV2(req);
  }

  @Override
  public TSExecuteStatementResp executeStatementV2(TSExecuteStatementReq req) {
    return executeStatementInternal(req, SELECT_RESULT);
  }

  @Override
  public TSExecuteStatementResp executeRawDataQueryV2(TSRawDataQueryReq req) {
    return executeRawDataQueryInternal(req, SELECT_RESULT);
  }

  @Override
  public TSExecuteStatementResp executeLastDataQueryV2(TSLastDataQueryReq req) {
    return executeLastDataQueryInternal(req, SELECT_RESULT);
  }

  @Override
  public TSExecuteStatementResp executeAggregationQueryV2(TSAggregationQueryReq req) {
    return executeAggregationQueryInternal(req, SELECT_RESULT);
  }

  @Override
  public TSFetchResultsResp fetchResultsV2(TSFetchResultsReq req) {
    long startTime = System.currentTimeMillis();
    boolean finished = false;
    StatementType statementType = null;
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }
      TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(req.queryId);

      if (queryExecution == null) {
        resp.setHasResultSet(false);
        resp.setMoreData(false);
        return resp;
      }
      statementType = queryExecution.getStatement().getType();

      try (SetThreadName queryName = new SetThreadName(queryExecution.getQueryId())) {
        Pair<List<ByteBuffer>, Boolean> pair =
            QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, req.fetchSize);
        List<ByteBuffer> result = pair.left;
        finished = pair.right;
        boolean hasResultSet = !result.isEmpty();
        resp.setHasResultSet(hasResultSet);
        resp.setIsAlign(true);
        resp.setQueryResult(result);
        resp.setMoreData(!finished);
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSFetchResultsResp(onQueryException(e, OperationType.FETCH_RESULTS));
    } finally {
      COORDINATOR.recordExecutionTime(req.queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        if (statementType != null) {
          addStatementExecutionLatency(
              OperationType.FETCH_RESULTS,
              statementType,
              COORDINATOR.getTotalExecutionTime(req.queryId));
        }
        COORDINATOR.cleanupQueryExecution(req.queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    IoTDBConstant.ClientVersion clientVersion = parseClientVersion(req);
    BasicOpenSessionResp openSessionResp =
        SESSION_MANAGER.login(
            SESSION_MANAGER.getCurrSession(),
            req.username,
            req.password,
            req.zoneId,
            req.client_protocol,
            clientVersion);
    TSStatus tsStatus = RpcUtils.getStatus(openSessionResp.getCode(), openSessionResp.getMessage());
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
    return resp.setSessionId(openSessionResp.getSessionId());
  }

  private IoTDBConstant.ClientVersion parseClientVersion(TSOpenSessionReq req) {
    Map<String, String> configuration = req.configuration;
    if (configuration != null && configuration.containsKey("version")) {
      return IoTDBConstant.ClientVersion.valueOf(configuration.get("version"));
    }
    return IoTDBConstant.ClientVersion.V_0_12;
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    return new TSStatus(
        !SESSION_MANAGER.closeSession(
                SESSION_MANAGER.getCurrSession(), COORDINATOR::cleanupQueryExecution)
            ? RpcUtils.getStatus(TSStatusCode.NOT_LOGIN)
            : RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    // TODO implement
    return RpcUtils.getStatus(TSStatusCode.QUERY_NOT_ALLOWED, "Cancellation is not implemented");
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    return SESSION_MANAGER.closeOperation(
        SESSION_MANAGER.getCurrSession(),
        req.queryId,
        req.statementId,
        req.isSetStatementId(),
        req.isSetQueryId(),
        COORDINATOR::cleanupQueryExecution);
  }

  @Override
  public TSGetTimeZoneResp getTimeZone(long sessionId) {
    try {
      ZoneId zoneId = SESSION_MANAGER.getCurrSession().getZoneId();
      return new TSGetTimeZoneResp(
          RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
          zoneId != null ? zoneId.toString() : "Unknown time zone");
    } catch (Exception e) {
      return new TSGetTimeZoneResp(
          onNPEOrUnexpectedException(
              e, OperationType.GET_TIME_ZONE, TSStatusCode.GENERATE_TIME_ZONE_ERROR),
          "Unknown time zone");
    }
  }

  @Override
  public TSStatus setTimeZone(TSSetTimeZoneReq req) {
    try {
      SESSION_MANAGER.getCurrSession().setZoneId(ZoneId.of(req.timeZone));
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.SET_TIME_ZONE, TSStatusCode.SET_TIME_ZONE_ERROR);
    }
  }

  @Override
  public ServerProperties getProperties() {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    properties.setBuildInfo(IoTDBConstant.BUILD_INFO);
    LOGGER.info("IoTDB server version: {}", IoTDBConstant.VERSION_WITH_BUILD);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    properties.setTimestampPrecision(
        IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    properties.setMaxConcurrentClientNum(
        IoTDBDescriptor.getInstance().getConfig().getRpcMaxConcurrentClientNum());
    properties.setWatermarkSecretKey(
        IoTDBDescriptor.getInstance().getConfig().getWatermarkSecretKey());
    properties.setWatermarkBitString(
        IoTDBDescriptor.getInstance().getConfig().getWatermarkBitString());
    properties.setWatermarkParamMarkRate(
        IoTDBDescriptor.getInstance().getConfig().getWatermarkParamMarkRate());
    properties.setWatermarkParamMaxRightBit(
        IoTDBDescriptor.getInstance().getConfig().getWatermarkParamMaxRightBit());
    properties.setIsReadOnly(CommonDescriptor.getInstance().getConfig().isReadOnly());
    properties.setThriftMaxFrameSize(
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize());
    return properties;
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // Step 1: Create SetStorageGroupStatement
      DatabaseSchemaStatement statement =
          (DatabaseSchemaStatement) StatementGenerator.createStatement(storageGroup);
      if (enableAuditLog) {
        AuditLogger.log(String.format("create database %s", storageGroup), statement);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.SET_STORAGE_GROUP, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.SET_STORAGE_GROUP, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // measurementAlias is also a nodeName
      req.setMeasurementAlias(PathUtils.checkAndReturnSingleMeasurement(req.getMeasurementAlias()));
      // Step 1: transfer from TSCreateTimeseriesReq to Statement
      CreateTimeSeriesStatement statement = StatementGenerator.createStatement(req);
      if (enableAuditLog) {
        AuditLogger.log(String.format("create timeseries %s", req.getPath()), statement);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus createAlignedTimeseries(TSCreateAlignedTimeseriesReq req) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementAlias(
          PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurementAlias()));

      req.setMeasurements(PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurements()));

      // Step 1: transfer from CreateAlignedTimeSeriesReq to Statement
      CreateAlignedTimeSeriesStatement statement = StatementGenerator.createStatement(req);
      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "create aligned timeseries %s.%s", req.getPrefixPath(), req.getMeasurements()),
            statement);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_ALIGNED_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_ALIGNED_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementAliasList(
          PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurementAliasList()));

      // Step 1: transfer from CreateMultiTimeSeriesReq to Statement
      CreateMultiTimeSeriesStatement statement = StatementGenerator.createStatement(req);
      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "create %s timeseries, the first is %s",
                req.getPaths().size(), req.getPaths().get(0)),
            statement);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_MULTI_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_MULTI_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> path) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // Step 1: transfer from DeleteStorageGroupsReq to Statement
      DeleteTimeSeriesStatement statement =
          StatementGenerator.createDeleteTimeSeriesStatement(path);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // Step 1: transfer from DeleteStorageGroupsReq to Statement
      DeleteStorageGroupStatement statement = StatementGenerator.createStatement(storageGroups);

      if (enableAuditLog) {
        AuditLogger.log(String.format("delete databases: %s", storageGroups), statement);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_STORAGE_GROUPS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_STORAGE_GROUPS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    return new TSFetchMetadataResp(
        RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION, "Fetch Metadata is not supported."));
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    return executeStatementInternal(req, OLD_SELECT_RESULT);
  }

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<TSStatus> results = new ArrayList<>();
    boolean isAllSuccessful = true;
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return getNotLoggedInStatus();
    }

    try {
      for (int i = 0; i < req.getStatements().size(); i++) {
        String statement = req.getStatements().get(i);
        long t2 = System.currentTimeMillis();
        StatementType type = null;
        try {
          Statement s = StatementGenerator.createStatement(statement, clientSession.getZoneId());
          if (s == null) {
            return RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR, "This operation type is not supported");
          }
          // permission check
          TSStatus status = AuthorityChecker.checkAuthority(s, clientSession);
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return status;
          }

          if (enableAuditLog) {
            AuditLogger.log(statement, s);
          }

          long queryId = SESSION_MANAGER.requestQueryId();
          type = s.getType();
          // create and cache dataset
          ExecutionResult result =
              COORDINATOR.execute(
                  s,
                  queryId,
                  SESSION_MANAGER.getSessionInfo(clientSession),
                  statement,
                  partitionFetcher,
                  schemaFetcher,
                  config.getQueryTimeoutThreshold());
          results.add(result.status);
        } catch (Exception e) {
          LOGGER.warn("Error occurred when executing executeBatchStatement: ", e);
          TSStatus status =
              onQueryException(
                  e, "\"" + statement + "\". " + OperationType.EXECUTE_BATCH_STATEMENT);
          if (status.getCode() != TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()) {
            isAllSuccessful = false;
          }
          results.add(status);
        } finally {
          addStatementExecutionLatency(
              OperationType.EXECUTE_STATEMENT, type, System.currentTimeMillis() - t2);
        }
      }
    } finally {
      addStatementExecutionLatency(
          OperationType.EXECUTE_BATCH_STATEMENT,
          StatementType.NULL,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
    return isAllSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully")
        : RpcUtils.getStatus(results);
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    return executeStatement(req);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    return executeStatement(req);
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    boolean finished = false;
    long startTime = System.currentTimeMillis();
    StatementType statementType = null;
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }

      TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(req.queryId);
      if (queryExecution == null) {
        resp.setHasResultSet(false);
        resp.setMoreData(true);
        return resp;
      }
      statementType = queryExecution.getStatement().getType();

      try (SetThreadName queryName = new SetThreadName(queryExecution.getQueryId())) {
        Pair<TSQueryDataSet, Boolean> pair =
            QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize);
        TSQueryDataSet result = pair.left;
        finished = pair.right;
        boolean hasResultSet = result.bufferForTime().limit() != 0;
        resp.setHasResultSet(hasResultSet);
        resp.setQueryDataSet(result);
        resp.setIsAlign(true);
        resp.setMoreData(finished);
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      return RpcUtils.getTSFetchResultsResp(onQueryException(e, OperationType.FETCH_RESULTS));
    } finally {
      COORDINATOR.recordExecutionTime(req.queryId, System.currentTimeMillis() - startTime);
      if (finished) {
        if (statementType != null) {
          addStatementExecutionLatency(
              OperationType.FETCH_RESULTS,
              statementType,
              COORDINATOR.getTotalExecutionTime(req.queryId));
        }
        COORDINATOR.cleanupQueryExecution(req.queryId);
      }
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementsList(
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(req.getMeasurementsList()));

      // Step 1:  transfer from TSInsertRecordsReq to Statement
      InsertRowsStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertRecords, first device %s, first time %s",
                req.prefixPaths.get(0), req.getTimestamps().get(0)),
            statement,
            true);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORDS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_RECORDS,
          StatementType.BATCH_INSERT_ROWS,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementsList(
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(req.getMeasurementsList()));

      // Step 1: transfer from TSInsertRecordsOfOneDeviceReq to Statement
      InsertRowsOfOneDeviceStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertRecords, first device %s, first time %s",
                req.prefixPath, req.getTimestamps().get(0)),
            statement,
            true);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS_OF_ONE_DEVICE, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORDS_OF_ONE_DEVICE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_RECORDS_OF_ONE_DEVICE,
          StatementType.BATCH_INSERT_ONE_DEVICE,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementsList(
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(req.getMeasurementsList()));

      // Step 1: transfer from TSInsertStringRecordsOfOneDeviceReq to Statement
      InsertRowsOfOneDeviceStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertRecords, first device %s, first time %s",
                req.prefixPath, req.getTimestamps().get(0)),
            statement,
            true);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(
          e, OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e,
          OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE,
          TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE,
          StatementType.BATCH_INSERT_ONE_DEVICE,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertRecord(TSInsertRecordReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurements(PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurements()));

      InsertRowStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertRecord, device %s, time %s", req.getPrefixPath(), req.getTimestamp()),
            statement,
            true);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORD, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_RECORD, StatementType.INSERT, System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      req.setMeasurementsList(
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(req.getMeasurementsList()));

      // Step 1: transfer from TSInsertTabletsReq to Statement
      InsertMultiTabletsStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_TABLETS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLETS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_TABLETS,
          StatementType.MULTI_BATCH_INSERT,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertTablet(TSInsertTabletReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurements(PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurements()));
      // Step 1: transfer from TSInsertTabletReq to Statement
      InsertTabletStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_TABLET, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_TABLET, StatementType.BATCH_INSERT, System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurementsList(
          PathUtils.checkIsLegalSingleMeasurementListsAndUpdate(req.getMeasurementsList()));

      InsertRowsStatement statement = StatementGenerator.createStatement(req);
      // return success when this statement is empty because server doesn't need to execute it
      if (statement.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertRecords, first device %s, first time %s",
                req.prefixPaths.get(0), req.getTimestamps().get(0)),
            statement,
            true);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_STRING_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_STRING_RECORDS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_STRING_RECORDS,
          StatementType.BATCH_INSERT_ROWS,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus testInsertTablet(TSInsertTabletReq req) {
    LOGGER.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertTablets(TSInsertTabletsReq req) {
    LOGGER.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecord(TSInsertRecordReq req) {
    LOGGER.debug("Test insert row request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecord(TSInsertStringRecordReq req) {
    LOGGER.debug("Test insert string record request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecords(TSInsertRecordsReq req) {
    LOGGER.debug("Test insert row in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
    LOGGER.debug("Test insert rows in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecords(TSInsertStringRecordsReq req) {
    LOGGER.debug("Test insert string records request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      DeleteDataStatement statement = StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_DATA, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_DATA, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    return executeRawDataQueryInternal(req, OLD_SELECT_RESULT);
  }

  @Override
  public TSExecuteStatementResp executeLastDataQuery(TSLastDataQueryReq req) {
    return executeLastDataQueryInternal(req, OLD_SELECT_RESULT);
  }

  @Override
  public TSExecuteStatementResp executeAggregationQuery(TSAggregationQueryReq req) {
    return executeAggregationQueryInternal(req, OLD_SELECT_RESULT);
  }

  @Override
  public long requestStatementId(long sessionId) {
    return SESSION_MANAGER.requestStatementId(SESSION_MANAGER.getCurrSession());
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      req.setName(checkIdentifierAndRemoveBackQuotesIfNecessary(req.getName()));

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement
      CreateSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      if (enableAuditLog) {
        AuditLogger.log(String.format("create schema template %s", req.getName()), statement);
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_SCHEMA_TEMPLATE, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_SCHEMA_TEMPLATE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
    return RpcUtils.getStatus(
        TSStatusCode.UNSUPPORTED_OPERATION, "Modify template has not been supported.");
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    return RpcUtils.getStatus(
        TSStatusCode.UNSUPPORTED_OPERATION, "Modify template has not been supported.");
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
    TSQueryTemplateResp resp = new TSQueryTemplateResp();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        resp.setStatus(getNotLoggedInStatus());
        return resp;
      }

      // "" and * have special meaning in implementation. Currently, we do not deal with them.
      if (!req.getName().equals("") && !req.getName().equals("*")) {
        req.setName(checkIdentifierAndRemoveBackQuotesIfNecessary(req.getName()));
      }

      Statement statement = StatementGenerator.createStatement(req);
      if (statement == null) {
        resp.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.UNSUPPORTED_OPERATION,
                TemplateQueryType.values()[req.getQueryType()].name() + "has not been supported."));
        return resp;
      }
      switch (TemplateQueryType.values()[req.getQueryType()]) {
        case SHOW_MEASUREMENTS:
          resp.setQueryType(TemplateQueryType.SHOW_MEASUREMENTS.ordinal());
          break;
        case SHOW_TEMPLATES:
          resp.setQueryType(TemplateQueryType.SHOW_TEMPLATES.ordinal());
          break;
        case SHOW_SET_TEMPLATES:
          resp.setQueryType(TemplateQueryType.SHOW_SET_TEMPLATES.ordinal());
          break;
        case SHOW_USING_TEMPLATES:
          resp.setQueryType(TemplateQueryType.SHOW_USING_TEMPLATES.ordinal());
          break;
        default:
          break;
      }
      return executeTemplateQueryStatement(statement, req, resp);
    } catch (Exception e) {
      resp.setStatus(
          onNPEOrUnexpectedException(
              e, OperationType.EXECUTE_QUERY_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR));
      return resp;
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  private TSQueryTemplateResp executeTemplateQueryStatement(
      Statement statement, TSQueryTemplateReq req, TSQueryTemplateResp resp) {
    long startTime = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        resp.setStatus(status);
        return resp;
      }

      if (enableAuditLog) {
        AuditLogger.log(String.format("execute Query: %s", statement), statement);
      }
      long queryId = SESSION_MANAGER.requestQueryId();
      // create and cache dataset
      ExecutionResult executionResult =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              null,
              partitionFetcher,
              schemaFetcher,
              config.getQueryTimeoutThreshold());

      if (executionResult.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && executionResult.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        resp.setStatus(executionResult.status);
        return resp;
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(executionResult.queryId.getId())) {
        List<String> result = new ArrayList<>();
        while (queryExecution.hasNextResult()) {
          Optional<TsBlock> tsBlock;
          try {
            tsBlock = queryExecution.getBatchResult();
          } catch (IoTDBException e) {
            throw new RuntimeException("Fetch Schema failed. ", e);
          }
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          Column column = tsBlock.get().getColumn(0);
          for (int i = 0; i < column.getPositionCount(); i++) {
            result.add(column.getBinary(i).getStringValue());
          }
        }
        resp.setMeasurements(result);
        resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully"));
        return resp;
      }
    } catch (Exception e) {
      resp.setStatus(
          onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_STATEMENT));
      return null;
    } finally {
      addStatementExecutionLatency(
          OperationType.EXECUTE_STATEMENT,
          statement.getType(),
          System.currentTimeMillis() - startTime);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      req.setTemplateName(checkIdentifierAndRemoveBackQuotesIfNecessary(req.getTemplateName()));

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement
      SetSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      if (enableAuditLog) {
        AuditLogger.log(
            String.format("set schema template %s.%s", req.getTemplateName(), req.getPrefixPath()),
            statement);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.EXECUTE_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) throws TException {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      req.setTemplateName(checkIdentifierAndRemoveBackQuotesIfNecessary(req.getTemplateName()));

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement
      UnsetSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "unset schema template %s from %s", req.getTemplateName(), req.getPrefixPath()),
            statement);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.EXECUTE_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus dropSchemaTemplate(TSDropSchemaTemplateReq req) throws TException {
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      req.setTemplateName(checkIdentifierAndRemoveBackQuotesIfNecessary(req.getTemplateName()));

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement
      DropSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      if (enableAuditLog) {
        AuditLogger.log(String.format("drop schema template %s", req.getTemplateName()), statement);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.EXECUTE_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      SESSION_MANAGER.updateIdleTime();
    }
  }

  @Override
  public TSStatus handshake(TSyncIdentityInfo info) throws TException {
    // TODO(sync): Check permissions here
    return SyncService.getInstance()
        .handshake(
            info,
            SESSION_MANAGER.getCurrSession().getClientAddress(),
            partitionFetcher,
            schemaFetcher);
  }

  @Override
  public TSStatus sendPipeData(ByteBuffer buff) throws TException {
    return SyncService.getInstance().transportPipeData(buff);
  }

  @Override
  public TSStatus sendFile(TSyncTransportMetaInfo metaInfo, ByteBuffer buff) throws TException {
    return SyncService.getInstance().transportFile(metaInfo, buff);
  }

  @Override
  public TSBackupConfigurationResp getBackupConfiguration() {
    return new TSBackupConfigurationResp(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSConnectionInfoResp fetchAllConnectionsInfo() throws TException {
    return SESSION_MANAGER.getAllConnectionInfo();
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) {
    long t1 = System.currentTimeMillis();
    try {
      IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
      if (!SESSION_MANAGER.checkLogin(clientSession)) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention
      req.setMeasurements(PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurements()));

      InsertRowStatement statement = StatementGenerator.createStatement(req);

      if (enableAuditLog) {
        AuditLogger.log(
            String.format(
                "insertStringRecord, device %s, time %s", req.getPrefixPath(), req.getTimestamp()),
            statement,
            true);
      }

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, clientSession);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(clientSession),
              "",
              partitionFetcher,
              schemaFetcher);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_STRING_RECORD, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_STRING_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addStatementExecutionLatency(
          OperationType.INSERT_STRING_RECORD,
          StatementType.INSERT,
          System.currentTimeMillis() - t1);
      SESSION_MANAGER.updateIdleTime();
    }
  }

  private TSExecuteStatementResp createResponse(DatasetHeader header, long queryId) {
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
    resp.setColumnNameIndexMap(header.getColumnNameIndexMap());
    // TODO deal with the sg name here
    resp.setSgColumns(new ArrayList<>());
    resp.setColumns(header.getRespColumns());
    resp.setDataTypeList(header.getRespDataTypeList());
    resp.setAliasColumns(header.getRespAliasColumns());
    resp.setIgnoreTimeStamp(header.isIgnoreTimestamp());
    resp.setQueryId(queryId);
    return resp;
  }

  private TSStatus getNotLoggedInStatus() {
    return RpcUtils.getStatus(
        TSStatusCode.NOT_LOGIN,
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  /** Add stat of operation into metrics */
  private void addStatementExecutionLatency(
      OperationType operation, StatementType statementType, long costTime) {
    if (statementType == null) {
      return;
    }

    MetricService.getInstance()
        .timer(
            costTime,
            TimeUnit.MILLISECONDS,
            Metric.PERFORMANCE_OVERVIEW.toString(),
            MetricLevel.IMPORTANT,
            Tag.INTERFACE.toString(),
            operation.toString(),
            Tag.TYPE.toString(),
            statementType.name());
  }

  private String checkIdentifierAndRemoveBackQuotesIfNecessary(String identifier) {
    return identifier == null ? null : ASTVisitor.parseIdentifier(identifier);
  }

  @Override
  public void handleClientExit() {
    IClientSession session = SESSION_MANAGER.getCurrSession();
    if (session != null) {
      TSCloseSessionReq req = new TSCloseSessionReq();
      closeSession(req);
    }
    SyncService.getInstance().handleClientExit();
  }
}
