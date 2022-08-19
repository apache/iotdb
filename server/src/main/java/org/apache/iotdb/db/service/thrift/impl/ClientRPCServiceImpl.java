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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.metadata.template.TemplateQueryType;
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
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.SessionTimeoutManager;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Operation;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.ConfigNodeConnectionException;
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
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import io.airlift.concurrent.SetThreadName;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.service.basic.ServiceProvider.AUDIT_LOGGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CONFIG;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CURRENT_RPC_VERSION;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_FREQUENCY_RECORDER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_TIME_MANAGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.SLOW_SQL_LOGGER;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onIoTDBException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class ClientRPCServiceImpl implements IClientRPCServiceWithHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientRPCServiceImpl.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;

  public ClientRPCServiceImpl() {
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
    TSStatus loginStatus;
    try {
      loginStatus = AuthorizerManager.getInstance().checkUser(req.username, req.password);
    } catch (ConfigNodeConnectionException e) {
      TSStatus tsStatus = RpcUtils.getStatus(TSStatusCode.AUTHENTICATION_ERROR, e.getMessage());
      return new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
    }
    BasicOpenSessionResp openSessionResp = new BasicOpenSessionResp();
    long sessionId = -1;
    if (loginStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // check the version compatibility
      boolean compatible = req.client_protocol.equals(SessionManager.CURRENT_RPC_VERSION);
      if (!compatible) {
        openSessionResp.setCode(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
        openSessionResp.setMessage(
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        openSessionResp = openSessionResp.sessionId(sessionId);
      } else {
        openSessionResp.setCode(loginStatus.getCode());
        openSessionResp.setMessage(loginStatus.getMessage());

        sessionId = SESSION_MANAGER.requestSessionId(req.username, req.zoneId, clientVersion);

        LOGGER.info(
            "{}: Login status: {}. User : {}, opens Session-{}",
            IoTDBConstant.GLOBAL_DB_NAME,
            openSessionResp.getMessage(),
            req.username,
            sessionId);

        SessionTimeoutManager.getInstance().register(sessionId);
        openSessionResp = openSessionResp.sessionId(sessionId);
      }
    } else {
      openSessionResp.setMessage(loginStatus.getMessage());
      openSessionResp.setCode(loginStatus.getCode());

      sessionId = SESSION_MANAGER.requestSessionId(req.username, req.zoneId, clientVersion);
      SessionManager.AUDIT_LOGGER.info(
          "User {} opens Session failed with an incorrect password", req.username);

      SessionTimeoutManager.getInstance().register(sessionId);
      openSessionResp = openSessionResp.sessionId(sessionId);
    }

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
    SESSION_MANAGER.releaseSessionResource(req.sessionId, this::cleanupQueryExecution);
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
        req.sessionId,
        req.queryId,
        req.statementId,
        req.isSetStatementId(),
        req.isSetQueryId(),
        this::cleanupQueryExecution);
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
    try {
      if (!SESSION_MANAGER.checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create storage group {}", SESSION_MANAGER.getCurrSessionId(), storageGroup);
      }

      // Step 1: Create SetStorageGroupStatement
      SetStorageGroupStatement statement =
          (SetStorageGroupStatement) StatementGenerator.createStatement(storageGroup);
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.SET_STORAGE_GROUP, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create timeseries {}", SESSION_MANAGER.getCurrSessionId(), req.getPath());
      }

      // measurementAlias is also a nodeName
      PathUtils.isLegalSingleMeasurements(Collections.singletonList(req.getMeasurementAlias()));
      // Step 1: transfer from TSCreateTimeseriesReq to Statement
      CreateTimeSeriesStatement statement =
          (CreateTimeSeriesStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus createAlignedTimeseries(TSCreateAlignedTimeseriesReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      // if measurements.size() == 1, convert to create timeseries
      if (req.measurements.size() == 1) {
        return createTimeseries(
            new TSCreateTimeseriesReq(
                req.sessionId,
                req.prefixPath + "." + req.measurements.get(0),
                req.dataTypes.get(0),
                req.encodings.get(0),
                req.compressors.get(0)));
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create aligned timeseries {}.{}",
            SESSION_MANAGER.getCurrSessionId(),
            req.getPrefixPath(),
            req.getMeasurements());
      }

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurements(req.getMeasurementAlias());

      PathUtils.isLegalSingleMeasurements(req.getMeasurements());

      // Step 1: transfer from CreateAlignedTimeSeriesReq to Statement
      CreateAlignedTimeSeriesStatement statement =
          (CreateAlignedTimeSeriesStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_ALIGNED_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create {} timeseries, the first is {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.getPaths().size(),
            req.getPaths().get(0));
      }

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurements(req.getMeasurementAliasList());

      // Step 1: transfer from CreateMultiTimeSeriesReq to Statement
      CreateMultiTimeSeriesStatement statement =
          (CreateMultiTimeSeriesStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_MULTI_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    try {
      if (!SESSION_MANAGER.checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} delete {} storage groups, the first is {}",
            SESSION_MANAGER.getCurrSessionId(),
            storageGroups.size(),
            storageGroups.get(0));
      }

      // Step 1: transfer from DeleteStorageGroupsReq to Statement
      DeleteStorageGroupStatement statement =
          (DeleteStorageGroupStatement) StatementGenerator.createStatement(storageGroups);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_STORAGE_GROUPS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
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
    try {
      Statement s =
          StatementGenerator.createStatement(
              statement, SESSION_MANAGER.getZoneId(req.getSessionId()));

      if (s == null) {
        return RpcUtils.getTSExecuteStatementResp(
            RpcUtils.getStatus(
                TSStatusCode.SQL_PARSE_ERROR, "This operation type is not supported"));
      }
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      QUERY_FREQUENCY_RECORDER.incrementAndGet();
      AUDIT_LOGGER.debug("Session {} execute Query: {}", req.sessionId, statement);

      long queryId = SESSION_MANAGER.requestQueryId(req.statementId, true);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              statement,
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {
        TSExecuteStatementResp resp;
        if (queryExecution != null && queryExecution.isQuery()) {
          resp = createResponse(queryExecution.getDatasetHeader(), queryId);
          resp.setStatus(result.status);
          resp.setQueryDataSet(
              QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize));
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }

        return resp;
      }
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
    long t1 = System.currentTimeMillis();
    List<TSStatus> results = new ArrayList<>();
    boolean isAllSuccessful = true;
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    for (int i = 0; i < req.getStatements().size(); i++) {
      String statement = req.getStatements().get(i);
      try {
        Statement s =
            StatementGenerator.createStatement(
                statement, SESSION_MANAGER.getZoneId(req.getSessionId()));
        if (s == null) {
          return RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "This operation type is not supported");
        }
        // permission check
        TSStatus status = AuthorityChecker.checkAuthority(s, req.sessionId);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }

        QUERY_FREQUENCY_RECORDER.incrementAndGet();
        AUDIT_LOGGER.debug("Session {} execute Query: {}", req.sessionId, s);

        long queryId = SESSION_MANAGER.requestQueryId(false);
        long t2 = System.currentTimeMillis();
        // create and cache dataset
        ExecutionResult result =
            COORDINATOR.execute(
                s,
                queryId,
                SESSION_MANAGER.getSessionInfo(req.sessionId),
                statement,
                PARTITION_FETCHER,
                SCHEMA_FETCHER,
                config.getQueryTimeoutThreshold());
        addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
        results.add(result.status);
      } catch (Exception e) {
        LOGGER.error("Error occurred when executing executeBatchStatement: ", e);
        TSStatus status =
            onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_BATCH_STATEMENT);
        if (status.getCode() != TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()) {
          isAllSuccessful = false;
        }
        results.add(status);
      }
    }
    addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    return isAllSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully")
        : RpcUtils.getStatus(results);
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

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(req.queryId);
      try (SetThreadName queryName = new SetThreadName(queryExecution.getQueryId())) {

        TSQueryDataSet result =
            QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize);
        boolean hasResultSet = result.bufferForTime().limit() != 0;

        resp.setHasResultSet(hasResultSet);
        resp.setQueryDataSet(result);
        resp.setIsAlign(true);

        QUERY_TIME_MANAGER.unRegisterQuery(req.queryId, false);
        if (!hasResultSet) {
          COORDINATOR.removeQueryExecution(req.queryId);
        }
        return resp;
      }
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsStatement statement = (InsertRowsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORDS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsOfOneDeviceStatement statement =
          (InsertRowsOfOneDeviceStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORDS_OF_ONE_DEVICE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertRowsOfOneDeviceStatement statement =
          (InsertRowsOfOneDeviceStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e,
          OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE,
          TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurements(req.getMeasurements());

      InsertRowStatement statement = (InsertRowStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());

      // Step 1: TODO(INSERT) transfer from TSInsertTabletsReq to Statement
      InsertMultiTabletsStatement statement =
          (InsertMultiTabletsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLETS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurements(req.getMeasurements());
      // Step 1: TODO(INSERT) transfer from TSInsertTabletReq to Statement
      InsertTabletStatement statement =
          (InsertTabletStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());

      InsertRowsStatement statement = (InsertRowsStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_STRING_RECORDS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      DeleteDataStatement statement = StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_DATA, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_DATA, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }
    long startTime = System.currentTimeMillis();
    try {
      Statement s =
          StatementGenerator.createStatement(req, SESSION_MANAGER.getZoneId(req.getSessionId()));

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      QUERY_FREQUENCY_RECORDER.incrementAndGet();
      AUDIT_LOGGER.debug("Session {} execute Raw Data Query: {}", req.sessionId, req);
      long queryId = SESSION_MANAGER.requestQueryId(req.statementId, true);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
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
          resp.setQueryDataSet(
              QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize));
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }
    } catch (Exception e) {
      // TODO call the coordinator to release query resource
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + req + "\". " + OperationType.EXECUTE_RAW_DATA_QUERY));
    } finally {
      addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, req);
      }
    }
  }

  @Override
  public TSExecuteStatementResp executeLastDataQuery(TSLastDataQueryReq req) {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }
    long startTime = System.currentTimeMillis();
    try {
      Statement s =
          StatementGenerator.createStatement(req, SESSION_MANAGER.getZoneId(req.getSessionId()));

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(s, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return RpcUtils.getTSExecuteStatementResp(status);
      }

      QUERY_FREQUENCY_RECORDER.incrementAndGet();
      AUDIT_LOGGER.debug("Session {} execute Last Data Query: {}", req.sessionId, req);
      long queryId = SESSION_MANAGER.requestQueryId(req.statementId, true);
      // create and cache dataset
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
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
          resp.setQueryDataSet(
              QueryDataSetUtils.convertTsBlockByFetchSize(queryExecution, req.fetchSize));
        } else {
          resp = RpcUtils.getTSExecuteStatementResp(result.status);
        }
        return resp;
      }

    } catch (Exception e) {
      // TODO call the coordinator to release query resource
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + req + "\". " + OperationType.EXECUTE_LAST_DATA_QUERY));
    } finally {
      addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, req);
      }
    }
  }

  @Override
  public long requestStatementId(long sessionId) {
    return SESSION_MANAGER.requestStatementId(sessionId);
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create schema template {}",
            SESSION_MANAGER.getCurrSessionId(),
            req.getName());
      }

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement
      CreateSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_SCHEMA_TEMPLATE, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_SCHEMA_TEMPLATE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
    // todo: check measurement using isLegalSingleMeasurements()
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
    TSQueryTemplateResp resp = new TSQueryTemplateResp();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        resp.setStatus(getNotLoggedInStatus());
        return resp;
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
      }
      return executeTemplateQueryStatement(statement, req, resp);
    } catch (Exception e) {
      resp.setStatus(
          onNPEOrUnexpectedException(
              e, OperationType.EXECUTE_QUERY_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR));
      return resp;
    }
  }

  private TSQueryTemplateResp executeTemplateQueryStatement(
      Statement statement, TSQueryTemplateReq req, TSQueryTemplateResp resp) {
    long startTime = System.currentTimeMillis();
    try {
      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        resp.setStatus(status);
        return resp;
      }

      QUERY_FREQUENCY_RECORDER.incrementAndGet();
      AUDIT_LOGGER.debug("Session {} execute Query: {}", req.sessionId, statement);

      long queryId = SESSION_MANAGER.requestQueryId(false);
      // create and cache dataset
      ExecutionResult executionResult =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              null,
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
              config.getQueryTimeoutThreshold());

      if (executionResult.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && executionResult.status.code != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
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
      addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
    }
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} set schema template {}.{}",
            SESSION_MANAGER.getCurrSessionId(),
            req.getTemplateName(),
            req.getPrefixPath());
      }

      // Step 1: transfer from TSCreateSchemaTemplateReq to Statement

      SetSchemaTemplateStatement statement = StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.EXECUTE_STATEMENT, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
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
  public TSStatus handshake(TSyncIdentityInfo info) throws TException {
    // TODO(sync): Check permissions here
    return SyncService.getInstance().handshake(info);
  }

  @Override
  public TSStatus transportData(TSyncTransportMetaInfo metaInfo, ByteBuffer buff, ByteBuffer digest)
      throws TException {
    return SyncService.getInstance().transportData(metaInfo, buff, digest);
  }

  @Override
  public TSStatus checkFileDigest(TSyncTransportMetaInfo metaInfo, ByteBuffer digest)
      throws TException {
    return SyncService.getInstance().checkFileDigest(metaInfo, digest);
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

      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurements(req.getMeasurements());

      InsertRowStatement statement = (InsertRowStatement) StatementGenerator.createStatement(req);

      // permission check
      TSStatus status = AuthorityChecker.checkAuthority(statement, req.sessionId);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return status;
      }

      // Step 2: call the coordinator
      long queryId = SESSION_MANAGER.requestQueryId(false);
      ExecutionResult result =
          COORDINATOR.execute(
              statement,
              queryId,
              SESSION_MANAGER.getSessionInfo(req.sessionId),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);

      return result.status;
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_STRING_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnablePerformanceStat()) {
      MetricService.getInstance()
          .histogram(
              System.currentTimeMillis() - startTime,
              "operation_histogram",
              MetricLevel.IMPORTANT,
              "name",
              operation.getName());
      MetricService.getInstance()
          .count(1, "operation_count", MetricLevel.IMPORTANT, "name", operation.getName());
    }
  }

  @Override
  public void handleClientExit() {
    Long sessionId = SESSION_MANAGER.getCurrSessionId();
    if (sessionId != null) {
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
    SyncService.getInstance().handleClientExit();
  }

  private void cleanupQueryExecution(Long queryId) {
    IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
    if (queryExecution != null) {
      try (SetThreadName threadName = new SetThreadName(queryExecution.getQueryId())) {
        LOGGER.info("stop and clean up");
        queryExecution.stopAndCleanup();
        COORDINATOR.removeQueryExecution(queryId);
      }
    }
  }
}
