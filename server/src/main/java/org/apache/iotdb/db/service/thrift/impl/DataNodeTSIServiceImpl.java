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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.auth.authorizer.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandalonePartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.StandaloneSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.metrics.Operation;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSAppendSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
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
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.service.basic.ServiceProvider.AUDIT_LOGGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CONFIG;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CURRENT_RPC_VERSION;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_FREQUENCY_RECORDER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_TIME_MANAGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.SLOW_SQL_LOGGER;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class DataNodeTSIServiceImpl implements TSIEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeTSIServiceImpl.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;

  public DataNodeTSIServiceImpl() {
    if (config.isClusterMode()) {
      PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
      SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();
    } else {
      PARTITION_FETCHER = StandalonePartitionFetcher.getInstance();
      SCHEMA_FETCHER = StandaloneSchemaFetcher.getInstance();
    }
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    IoTDBConstant.ClientVersion clientVersion = parseClientVersion(req);
    BasicOpenSessionResp openSessionResp =
        AuthorizerManager.getInstance()
            .openSession(
                req.username, req.password, req.zoneId, req.client_protocol, clientVersion);
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
        !SESSION_MANAGER.closeSession(req.sessionId)
            ? RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR)
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
        req.sessionId, req.queryId, req.statementId, req.isSetStatementId(), req.isSetQueryId());
  }

  @Override
  public TSGetTimeZoneResp getTimeZone(long sessionId) {
    try {
      ZoneId zoneId = SESSION_MANAGER.getZoneId(sessionId);
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
      SESSION_MANAGER.setTimezone(req.sessionId, req.timeZone);
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
    LOGGER.info("IoTDB server version: {}", IoTDBConstant.VERSION);
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
    properties.setIsReadOnly(IoTDBDescriptor.getInstance().getConfig().isReadOnly());
    properties.setThriftMaxFrameSize(
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize());
    return properties;
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus createAlignedTimeseries(TSCreateAlignedTimeseriesReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroup) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    String statement = req.getStatement();
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }

    long startTime = System.currentTimeMillis();
    Statement s =
        StatementGenerator.createStatement(
            statement, SESSION_MANAGER.getZoneId(req.getSessionId()));

    // permission check
    TSStatus status = AuthorizerManager.getInstance().checkAuthority(s, req.sessionId);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return RpcUtils.getTSExecuteStatementResp(status);
    }

    QUERY_FREQUENCY_RECORDER.incrementAndGet();
    AUDIT_LOGGER.debug("Session {} execute Query: {}", req.sessionId, statement);

    try {
      long queryId = SESSION_MANAGER.requestQueryId(req.statementId, true);
      QueryId id = new QueryId(String.valueOf(queryId));
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              id,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              statement,
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException("error code: " + result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(id);

      TSExecuteStatementResp resp;
      if (queryExecution.isQuery()) {
        resp = createResponse(queryExecution.getDatasetHeader(), queryId);
        resp.setStatus(result.status);
        resp.setQueryDataSet(
            QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize));
      } else {
        resp = RpcUtils.getTSExecuteStatementResp(result.status);
      }

      return resp;
    } catch (Exception e) {
      // TODO call the coordinator to release query resource
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_STATEMENT));
    } finally {
      addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
    }
  }

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) throws TException {
    return executeStatement(req);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req)
      throws TException {
    return executeStatement(req);
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }

      TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
      TSQueryDataSet result =
          QueryDataSetUtils.convertTsBlockByFetchSize(
              COORDINATOR.getQueryExecution(new QueryId(String.valueOf(req.queryId))),
              req.fetchSize);
      boolean hasResultSet = result.bufferForTime().limit() != 0;

      resp.setHasResultSet(hasResultSet);
      resp.setQueryDataSet(result);
      resp.setIsAlign(true);

      QUERY_TIME_MANAGER.unRegisterQuery(req.queryId, false);
      return resp;

    } catch (Exception e) {
      return RpcUtils.getTSFetchResultsResp(onQueryException(e, OperationType.FETCH_RESULTS));
    }
  }

  @Override
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session {} insertRecords, first device {}, first time {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.prefixPaths.get(0),
            req.getTimestamps().get(0));
      }

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsStatement statement = (InsertRowsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session {} insertRecords, device {}, first time {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.prefixPath,
            req.getTimestamps().get(0));
      }

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsOfOneDeviceStatement statement =
          (InsertRowsOfOneDeviceStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session {} insertRecords, device {}, first time {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.prefixPath,
            req.getTimestamps().get(0));
      }

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsOfOneDeviceStatement statement =
          (InsertRowsOfOneDeviceStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertRecord(TSInsertRecordReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          SESSION_MANAGER.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTimestamp());

      InsertRowStatement statement = (InsertRowStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertMultiTabletsStatement statement =
          (InsertMultiTabletsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertTablet(TSInsertTabletReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      // Step 1: TODO(INSERT) transfer from TSInsertTabletReq to Statement
      InsertTabletStatement statement =
          (InsertTabletStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session {} insertRecords, first device {}, first time {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.prefixPaths.get(0),
            req.getTimestamps().get(0));
      }

      InsertRowsStatement statement = (InsertRowsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              genQueryId(queryId),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
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
    throw new UnsupportedOperationException();
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSExecuteStatementResp executeLastDataQuery(TSLastDataQueryReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long requestStatementId(long sessionId) {
    return SESSION_MANAGER.requestStatementId(sessionId);
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus dropSchemaTemplate(TSDropSchemaTemplateReq req) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          SESSION_MANAGER.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTimestamp());

      InsertRowStatement statement = (InsertRowStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorizerManager.getInstance().checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              new QueryId(String.valueOf(queryId)),
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
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
        TSStatusCode.NOT_LOGIN_ERROR,
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  /** Add stat of operation into metrics */
  private void addOperationLatency(Operation operation, long startTime) {
    if (CONFIG.isEnablePerformanceStat()) {
      MetricsService.getInstance()
          .getMetricManager()
          .histogram(
              System.currentTimeMillis() - startTime,
              "operation_histogram",
              MetricLevel.IMPORTANT,
              "name",
              operation.getName());
      MetricsService.getInstance()
          .getMetricManager()
          .count(1, "operation_count", MetricLevel.IMPORTANT, "name", operation.getName());
    }
  }

  @Override
  public void handleClientExit() {
    Long sessionId = SESSION_MANAGER.getCurrSessionId();
    if (sessionId != null) {
      SESSION_MANAGER.releaseSessionResource(sessionId, this::cleanupQueryExecution);
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
  }

  private void cleanupQueryExecution(Long queryId) {
    IQueryExecution queryExecution = COORDINATOR.getQueryExecution(genQueryId(queryId));
    if (queryExecution != null) {
      queryExecution.stopAndCleanup();
    }
  }

  private QueryId genQueryId(long id) {
    return new QueryId(String.valueOf(id));
  }
}
