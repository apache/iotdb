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
package org.apache.iotdb.db.service;

import static org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType.TIMESERIES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.BatchInsertionException;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thrift RPC implementation at server side.
 */
public class TSServiceImpl implements TSIService.Iface, ServerContext {

  private static final Logger auditLogger = LoggerFactory
      .getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);
  private static final Logger logger = LoggerFactory.getLogger(TSServiceImpl.class);
  private static final Logger SLOW_SQL_LOGGER = LoggerFactory.getLogger("SLOW_SQL");
  private static final Logger QUERY_FREQUENCY_LOGGER = LoggerFactory.getLogger("QUERY_FREQUENCY");
  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private static final int MAX_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getQueryCacheSizeInMetric();
  private static final int DELETE_SIZE = 20;
  private static final int DEFAULT_FETCH_SIZE = 10000;
  private static final String ERROR_PARSING_SQL =
      "meet error while parsing SQL to physical plan: {}";
  private static final String SERVER_INTERNAL_ERROR = "{}: server Internal Error: ";
  private static final String CHECK_METADATA_ERROR = "check metadata error: ";

  private static final List<SqlArgument> sqlArgumentList = new ArrayList<>(MAX_SIZE);
  protected Planner processor;
  protected IPlanExecutor executor;
  private boolean enableMetric = IoTDBDescriptor.getInstance().getConfig().isEnableMetricService();
  // Record the username for every rpc connection (session).
  private Map<Long, String> sessionIdUsernameMap = new ConcurrentHashMap<>();
  private Map<Long, ZoneId> sessionIdZoneIdMap = new ConcurrentHashMap<>();

  // The sessionId is unique in one IoTDB instance.
  private AtomicLong sessionIdGenerator = new AtomicLong();
  // The statementId is unique in one IoTDB instance.
  private AtomicLong statementIdGenerator = new AtomicLong();

  // (sessionId -> Set(statementId))
  private Map<Long, Set<Long>> sessionId2StatementId = new ConcurrentHashMap<>();
  // (statementId -> Set(queryId))
  private Map<Long, Set<Long>> statementId2QueryId = new ConcurrentHashMap<>();

  // (queryId -> QueryDataSet)
  private Map<Long, QueryDataSet> queryId2DataSet = new ConcurrentHashMap<>();

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  // When the client abnormally exits, we can still know who to disconnect
  private ThreadLocal<Long> currSessionId = new ThreadLocal<>();

  public static final TSProtocolVersion CURRENT_RPC_VERSION = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;


  private static final AtomicInteger queryCount = new AtomicInteger(0);


  public TSServiceImpl() throws QueryProcessException {
    processor = new Planner();
    executor = new PlanExecutor();

    ScheduledExecutorService timedQuerySqlCountThread = Executors
        .newSingleThreadScheduledExecutor(r -> new Thread(r, "timedQuerySqlCountThread"));
    timedQuerySqlCountThread.scheduleAtFixedRate(() -> {
          if (queryCount.get() != 0) {
            QUERY_FREQUENCY_LOGGER
                .info("Query count in current 1 minute: " + queryCount.getAndSet(0));
          }
        },
        config.getFrequencyIntervalInMinute(), config.getFrequencyIntervalInMinute(),
        TimeUnit.MINUTES);
  }

  public static List<SqlArgument> getSqlArgumentList() {
    return sqlArgumentList;
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    String loginMessage = null;
    try {
      status = authorizer.login(req.getUsername(), req.getPassword());
    } catch (AuthException e) {
      logger.info("meet error while logging in.", e);
      status = false;
      loginMessage = e.getMessage();
    }

    TSStatus tsStatus;
    long sessionId = -1;
    if (status) {
      //check the version compatibility
      boolean compatible = checkCompatibility(req.getClient_protocol());
      if (!compatible) {
        tsStatus = RpcUtils.getStatus(TSStatusCode.INCOMPATIBLE_VERSION,
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
        resp.setSessionId(sessionId);
        return resp;
      }

      tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Login successfully");
      sessionId = sessionIdGenerator.incrementAndGet();
      sessionIdUsernameMap.put(sessionId, req.getUsername());
      sessionIdZoneIdMap.put(sessionId, ZoneId.of(req.getZoneId()));
      currSessionId.set(sessionId);
      auditLogger.info("User {} opens Session-{}", req.getUsername(), sessionId);
      logger.info(
          "{}: Login status: {}. User : {}", IoTDBConstant.GLOBAL_DB_NAME, tsStatus.message,
          req.getUsername());
    } else {
      tsStatus = RpcUtils.getStatus(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR,
          loginMessage != null ? loginMessage : "Authentication failed.");
      auditLogger
          .info("User {} opens Session failed with an incorrect password", req.getUsername());
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        CURRENT_RPC_VERSION);
    resp.setSessionId(sessionId);
    return resp;
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(CURRENT_RPC_VERSION);
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    long sessionId = req.getSessionId();
    auditLogger.info("Session-{} is closing", sessionId);
    currSessionId.remove();

    TSStatus tsStatus;
    if (sessionIdUsernameMap.remove(sessionId) == null) {
      tsStatus = RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    } else {
      tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    sessionIdZoneIdMap.remove(sessionId);
    List<Exception> exceptions = new ArrayList<>();
    Set<Long> statementIds = sessionId2StatementId.getOrDefault(sessionId, Collections.emptySet());
    for (long statementId : statementIds) {
      Set<Long> queryIds = statementId2QueryId.getOrDefault(statementId, Collections.emptySet());
      for (long queryId : queryIds) {
        try {
          releaseQueryResource(queryId);
        } catch (StorageEngineException e) {
          // release as many as resources as possible, so do not break as soon as one exception is
          // raised
          exceptions.add(e);
          logger.error("Error in closeSession : ", e);
        }
      }
    }

    if (!exceptions.isEmpty()) {
      return new TSStatus(
          RpcUtils.getStatus(
              TSStatusCode.CLOSE_OPERATION_ERROR,
              String.format(
                  "%d errors in closeOperation, see server logs for detail", exceptions.size())));
    }

    return new TSStatus(tsStatus);
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    // TODO implement
    return RpcUtils.getStatus(TSStatusCode.QUERY_NOT_ALLOWED, "Cancellation is not implemented");
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    if (auditLogger.isDebugEnabled()) {
      auditLogger.debug("{}: receive close operation from Session {}", IoTDBConstant.GLOBAL_DB_NAME,
          currSessionId.get());
    }
    if (!checkLogin(req.getSessionId())) {
      auditLogger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    try {

      // ResultSet close
      if (req.isSetStatementId() && req.isSetQueryId()) {
        releaseQueryResource(req.queryId);
        // clear the statementId2QueryId map
        if (statementId2QueryId.containsKey(req.getStatementId())) {
          statementId2QueryId.get(req.getStatementId()).remove(req.getQueryId());
        }
      } else {
        // statement close
        long stmtId = req.getStatementId();
        Set<Long> queryIdSet = statementId2QueryId.remove(stmtId);
        if (queryIdSet != null) {
          for (long queryId : queryIdSet) {
            releaseQueryResource(queryId);
          }
        }
        // clear the sessionId2StatementId map
        if (sessionId2StatementId.containsKey(req.getSessionId())) {
          sessionId2StatementId.get(req.getSessionId()).remove(req.getStatementId());
        }
      }

    } catch (Exception e) {
      logger.error("Error in closeOperation : ", e);
      return RpcUtils.getStatus(TSStatusCode.CLOSE_OPERATION_ERROR, "Error in closeOperation");
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * release single operation resource
   */
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // remove the corresponding Physical Plan
    queryId2DataSet.remove(queryId);
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSStatus status;
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      return new TSFetchMetadataResp(status);
    }

    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          String metadataInJson = getMetadataInString();
          resp.setMetadataInJson(metadataInJson);
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesTypeByPath(new PartialPath(req.getColumnPath())).toString());
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(
              getPaths(new PartialPath(req.getColumnPath())).stream().map(PartialPath::getFullPath)
                  .collect(
                      Collectors.toList()));
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        default:
          status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, req.getType());
          break;
      }
    } catch (MetadataException | OutOfMemoryError e) {
      logger.error(
          String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()), e);
      status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
      resp.setStatus(status);
      return resp;
    } catch (Exception e) {
      logger.error("Error in fetchMetadata : ", e);
      status = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      resp.setStatus(status);
      return resp;
    }
    resp.setStatus(status);
    return resp;
  }

  private String getMetadataInString() {
    return IoTDB.metaManager.getMetadataInString();
  }

  protected List<PartialPath> getPaths(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<TSStatus> result = new ArrayList<>();
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }
      List<String> statements = req.getStatements();

      boolean isAllSuccessful = true;

      for (String statement : statements) {
        long t2 = System.currentTimeMillis();
        isAllSuccessful =
            executeStatementInBatch(statement, result, req.getSessionId())
                && isAllSuccessful;
        Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
      }
      if (isAllSuccessful) {
        return RpcUtils.getStatus(
            TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully");
      } else {
        return RpcUtils.getStatus(result);
      }
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils
          .getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    }
  }

  // execute one statement of a batch. Currently, query is not allowed in a batch statement and
  // on finding queries in a batch, such query will be ignored and an error will be generated
  private boolean executeStatementInBatch(String statement, List<TSStatus> result, long sessionId) {
    try {
      PhysicalPlan physicalPlan = processor
          .parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(sessionId));
      if (physicalPlan.isQuery()) {
        throw new QueryInBatchStatementException(statement);
      }
      TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan, sessionId);
      if (resp.getStatus().code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        result.add(resp.status);
      } else {
        result.add(resp.status);
        return false;
      }
    } catch (ParseCancellationException e) {
      logger.warn(ERROR_PARSING_SQL, statement + " " + e.getMessage());
      result.add(RpcUtils.getStatus(TSStatusCode.SQL_PARSE_ERROR,
          ERROR_PARSING_SQL + " " + statement + " " + e.getMessage()));
      return false;
    } catch (SQLParserException e) {
      logger.error("Error occurred when executing {}, check metadata error: ", statement, e);
      result.add(RpcUtils.getStatus(
          TSStatusCode.SQL_PARSE_ERROR,
          ERROR_PARSING_SQL + " " + statement + " " + e.getMessage()));
      return false;
    } catch (QueryProcessException e) {
      logger.info(
          "Error occurred when executing {}, meet error while parsing SQL to physical plan: {}",
          statement, e.getMessage());
      result.add(RpcUtils.getStatus(
          TSStatusCode.QUERY_PROCESS_ERROR, "Meet error in query process: " + e.getMessage()));
      return false;
    } catch (QueryInBatchStatementException e) {
      logger.info("Error occurred when executing {}, query statement not allowed: ", statement, e);
      result.add(
          RpcUtils.getStatus(TSStatusCode.QUERY_NOT_ALLOWED,
              "query statement not allowed: " + statement));
      return false;
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      result.add(RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR, "server Internal Error: " + e.getMessage()));
    }
    return true;
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }
      String statement = req.getStatement();

      PhysicalPlan physicalPlan = processor
          .parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(req.getSessionId()));
      if (physicalPlan.isQuery()) {
        return internalExecuteQueryStatement(statement, req.statementId, physicalPlan,
            req.fetchSize,
            sessionIdUsernameMap.get(req.getSessionId()));
      } else {
        return executeUpdateStatement(physicalPlan, req.getSessionId());
      }
    } catch (ParseCancellationException e) {
      logger.warn(ERROR_PARSING_SQL, req.getStatement() + " " + e.getMessage());
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR, e.getMessage());
    } catch (SQLParserException e) {
      logger.error(CHECK_METADATA_ERROR, e);
      return RpcUtils.getTSExecuteStatementResp(
          TSStatusCode.METADATA_ERROR, CHECK_METADATA_ERROR + e.getMessage());
    } catch (QueryProcessException e) {
      logger.info(ERROR_PARSING_SQL, e.getMessage());
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(TSStatusCode.QUERY_PROCESS_ERROR,
              "Meet error in query process: " + e.getMessage()));
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      String statement = req.getStatement();
      PhysicalPlan physicalPlan;
      try {
        physicalPlan = processor
            .parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(req.getSessionId()));
      } catch (QueryProcessException | SQLParserException e) {
        logger.info(ERROR_PARSING_SQL, req.getStatement() + " " + e.getMessage());
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR, e.getMessage());
      }

      if (!physicalPlan.isQuery()) {
        return RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
      }

      return internalExecuteQueryStatement(statement, req.statementId, physicalPlan, req.fetchSize,
          sessionIdUsernameMap.get(req.getSessionId()));

    } catch (ParseCancellationException e) {
      logger.warn(ERROR_PARSING_SQL, req.getStatement() + " " + e.getMessage());
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR,
          ERROR_PARSING_SQL + e.getMessage());
    } catch (SQLParserException e) {
      logger.error(CHECK_METADATA_ERROR, e);
      return RpcUtils.getTSExecuteStatementResp(
          TSStatusCode.METADATA_ERROR, CHECK_METADATA_ERROR + e.getMessage());
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      PhysicalPlan physicalPlan;
      try {
        physicalPlan =
            processor.rawDataQueryReqToPhysicalPlan(req);
      } catch (QueryProcessException | SQLParserException e) {
        logger.info(ERROR_PARSING_SQL, e.getMessage());
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR, e.getMessage());
      }

      if (!physicalPlan.isQuery()) {
        return RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
      }

      return internalExecuteQueryStatement("", req.statementId, physicalPlan, req.fetchSize,
          sessionIdUsernameMap.get(req.getSessionId()));

    } catch (ParseCancellationException e) {
      logger.warn(ERROR_PARSING_SQL, e.getMessage());
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR,
          ERROR_PARSING_SQL + e.getMessage());
    } catch (SQLParserException e) {
      logger.error(CHECK_METADATA_ERROR, e);
      return RpcUtils.getTSExecuteStatementResp(
          TSStatusCode.METADATA_ERROR, CHECK_METADATA_ERROR + e.getMessage());
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByTimePlan, some
   *             AuthorPlan
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private TSExecuteStatementResp internalExecuteQueryStatement(String statement,
      long statementId, PhysicalPlan plan, int fetchSize, String username) throws IOException {
    queryCount.incrementAndGet();
    auditLogger.debug("Session {} execute Query: {}", currSessionId.get(), statement);
    long startTime = System.currentTimeMillis();
    long queryId = -1;
    try {
      TSExecuteStatementResp resp = getQueryResp(plan, username); // column headers

      if (plan instanceof ShowTimeSeriesPlan) {
        //If the user does not pass the limit, then set limit = fetchSize and haslimit=false,else set haslimit = true
        if (((ShowTimeSeriesPlan) plan).getLimit() == 0) {
          ((ShowTimeSeriesPlan) plan).setLimit(fetchSize);
          ((ShowTimeSeriesPlan) plan).setHasLimit(false);
        } else {
          ((ShowTimeSeriesPlan) plan).setHasLimit(true);
        }
      }
      if (plan instanceof QueryPlan && !((QueryPlan) plan).isAlignByTime()) {
        if (plan.getOperatorType() == OperatorType.AGGREGATION) {
          throw new QueryProcessException("Aggregation doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.FILL) {
          throw new QueryProcessException("Fill doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.GROUPBYTIME) {
          throw new QueryProcessException("Group by doesn't support disable align clause.");
        }
      }
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        resp.setIgnoreTimeStamp(true);
      } // else default ignoreTimeStamp is false

      resp.setOperationType(plan.getOperatorType().toString());

      // generate the queryId for the operation
      queryId = generateQueryId(true);
      if (plan instanceof QueryPlan && config.isEnablePerformanceTracing()) {
        if (!(plan instanceof AlignByDevicePlan)) {
          TracingManager.getInstance()
              .writeQueryInfo(queryId, statement, startTime, plan.getPaths().size());
        } else {
          TracingManager.getInstance().writeQueryInfo(queryId, statement, startTime);
        }
      }
      // put it into the corresponding Set

      statementId2QueryId.computeIfAbsent(statementId, k -> new CopyOnWriteArraySet<>())
          .add(queryId);

      if (plan instanceof AuthorPlan) {
        plan.setLoginUserName(username);
      }
      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(queryId, plan);
      if (plan instanceof QueryPlan && !((QueryPlan) plan).isAlignByTime()
          && newDataSet instanceof NonAlignEngineDataSet) {
        TSQueryNonAlignDataSet result = fillRpcNonAlignReturnData(fetchSize, newDataSet, username);
        resp.setNonAlignQueryDataSet(result);
      } else {
        if (plan instanceof ShowPlan && ((ShowPlan) plan).getShowContentType() == TIMESERIES) {
          resp.setColumns(
              newDataSet.getPaths().stream().map(Path::getFullPath).collect(Collectors.toList()));
          resp.setDataTypeList(
              newDataSet.getDataTypes().stream().map(Enum::toString).collect(Collectors.toList()));
        }
        TSQueryDataSet result = fillRpcReturnData(fetchSize, newDataSet, username);
        resp.setQueryDataSet(result);
      }
      resp.setQueryId(queryId);

      if (plan instanceof AlignByDevicePlan && config.isEnablePerformanceTracing()) {
        TracingManager.getInstance()
            .writePathsNum(queryId, ((AlignByDeviceDataSet) newDataSet).getPathsNum());
      }

      if (enableMetric) {
        long endTime = System.currentTimeMillis();
        SqlArgument sqlArgument = new SqlArgument(resp, plan, statement, startTime, endTime);
        synchronized (sqlArgumentList) {
          sqlArgumentList.add(sqlArgument);
          if (sqlArgumentList.size() >= MAX_SIZE) {
            sqlArgumentList.subList(0, DELETE_SIZE).clear();
          }
        }
      }

      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      if (e instanceof NullPointerException) {
        e.printStackTrace();
      }
      if (queryId != -1) {
        try {
          releaseQueryResource(queryId);
        } catch (StorageEngineException ex) {
          logger.error("Error happened while releasing query resource: ", ex);
        }
      }
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= config.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: " + costTime + " ms, sql is " + statement);
      }
      if (plan.isDebug()) {
        SLOW_SQL_LOGGER.info("ChunkCache used memory proportion: " + ChunkCache.getInstance()
            .getUsedMemoryProportion() + "\nChunkMetadataCache used memory proportion: "
            + ChunkMetadataCache.getInstance().getUsedMemoryProportion()
            + "\nTimeSeriesMetadataCache used memory proportion: " + TimeSeriesMetadataCache
            .getInstance().getUsedMemoryProportion());
      }
    }
  }

  private TSExecuteStatementResp getQueryResp(PhysicalPlan plan, String username)
      throws QueryProcessException, AuthException, TException, MetadataException {
    if (plan instanceof AuthorPlan) {
      return getAuthQueryColumnHeaders(plan);
    } else if (plan instanceof ShowPlan) {
      return getShowQueryColumnHeaders((ShowPlan) plan);
    } else {
      return getQueryColumnHeaders(plan, username);
    }
  }

  private TSExecuteStatementResp getShowQueryColumnHeaders(ShowPlan showPlan)
      throws QueryProcessException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return StaticResps.TTL_RESP;
      case FLUSH_TASK_INFO:
        return StaticResps.FLUSH_INFO_RESP;
      case VERSION:
        return StaticResps.SHOW_VERSION_RESP;
      case TIMESERIES:
        return StaticResps.SHOW_TIMESERIES_RESP;
      case STORAGE_GROUP:
        return StaticResps.SHOW_STORAGE_GROUP;
      case CHILD_PATH:
        return StaticResps.SHOW_CHILD_PATHS;
      case DEVICES:
        return StaticResps.SHOW_DEVICES;
      case COUNT_NODE_TIMESERIES:
        return StaticResps.COUNT_NODE_TIMESERIES;
      case COUNT_NODES:
        return StaticResps.COUNT_NODES;
      case COUNT_TIMESERIES:
        return StaticResps.COUNT_TIMESERIES;
      case COUNT_DEVICES:
        return StaticResps.COUNT_DEVICES;
      case COUNT_STORAGE_GROUP:
        return StaticResps.COUNT_STORAGE_GROUP;
      case MERGE_STATUS:
        return StaticResps.MERGE_STATUS_RESP;
      default:
        logger.error("Unsupported show content type: {}", showPlan.getShowContentType());
        throw new QueryProcessException(
            "Unsupported show content type:" + showPlan.getShowContentType());
    }
  }

  private TSExecuteStatementResp getAuthQueryColumnHeaders(PhysicalPlan plan) {
    AuthorPlan authorPlan = (AuthorPlan) plan;
    switch (authorPlan.getAuthorType()) {
      case LIST_ROLE:
      case LIST_USER_ROLES:
        return StaticResps.LIST_ROLE_RESP;
      case LIST_USER:
      case LIST_ROLE_USERS:
        return StaticResps.LIST_USER_RESP;
      case LIST_ROLE_PRIVILEGE:
        return StaticResps.LIST_ROLE_PRIVILEGE_RESP;
      case LIST_USER_PRIVILEGE:
        return StaticResps.LIST_USER_PRIVILEGE_RESP;
      default:
        return RpcUtils.getTSExecuteStatementResp(
            RpcUtils.getStatus(TSStatusCode.SQL_PARSE_ERROR,
                String.format("%s is not an auth query", authorPlan.getAuthorType())));
    }
  }

  /**
   * get ResultSet schema
   */
  private TSExecuteStatementResp getQueryColumnHeaders(PhysicalPlan physicalPlan, String username)
      throws AuthException, TException, QueryProcessException, MetadataException {

    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(physicalPlan.getPaths(), physicalPlan, username)) {
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(TSStatusCode.NO_PERMISSION_ERROR,
              "No permissions for this operation " + physicalPlan.getOperatorType()));
    }

    TSExecuteStatementResp resp = RpcUtils
        .getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);

    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, respColumns, columnsTypes);
    } else if (plan instanceof LastQueryPlan) {
      // Last Query should return different respond instead of the static one
      // because the query dataset and query id is different although the header of last query is same.
      return StaticResps.LAST_RESP.deepCopy();
    } else if (plan instanceof AggregationPlan && ((AggregationPlan) plan).getLevel() >= 0) {
      Map<String, Long> finalPaths = FilePathUtils
          .getPathByLevel(((AggregationPlan) plan).getDeduplicatedPaths(),
              ((AggregationPlan) plan).getLevel(), null);
      for (Map.Entry<String, Long> entry : finalPaths.entrySet()) {
        respColumns.add("count(" + entry.getKey() + ")");
        columnsTypes.add(TSDataType.INT64.toString());
      }
    } else {
      getWideQueryHeaders(plan, respColumns, columnsTypes);
      resp.setColumnNameIndexMap(plan.getPathToIndex());
    }
    resp.setColumns(respColumns);
    resp.setDataTypeList(columnsTypes);
    return resp;
  }

  // wide means not align by device
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void getWideQueryHeaders(
      QueryPlan plan, List<String> respColumns, List<String> columnTypes)
      throws TException, MetadataException {
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    List<PartialPath> paths = plan.getPaths();
    List<TSDataType> seriesTypes = new ArrayList<>();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (PartialPath path : paths) {
          String column = path.getTsAlias();
          if (column == null) {
            column = path.getMeasurementAlias() != null ? path.getFullPathWithAlias()
                : path.getFullPath();
          }
          respColumns.add(column);
          seriesTypes.add(getSeriesTypeByPath(path));
        }
        break;
      case AGGREGATION:
      case GROUPBYTIME:
      case GROUP_BY_FILL:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          PartialPath path = paths.get(i);
          String column = path.getTsAlias();
          if (column == null) {
            column = path.getMeasurementAlias() != null
                ? aggregations.get(i) + "(" + paths.get(i).getFullPathWithAlias() + ")"
                : aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")";
          }
          respColumns.add(column);
        }
        seriesTypes = getSeriesTypesByPaths(paths, aggregations);
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }

    for (TSDataType seriesType : seriesTypes) {
      columnTypes.add(seriesType.toString());
    }
  }

  private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes) {
    // set columns in TSExecuteStatementResp.
    respColumns.add(SQLConstant.ALIGNBY_DEVICE_COLUMN_NAME);

    // get column types and do deduplication
    columnTypes.add(TSDataType.TEXT.toString()); // the DEVICE column of ALIGN_BY_DEVICE result
    List<TSDataType> deduplicatedColumnsType = new ArrayList<>();
    deduplicatedColumnsType.add(TSDataType.TEXT); // the DEVICE column of ALIGN_BY_DEVICE result

    Set<String> deduplicatedMeasurements = new LinkedHashSet<>();
    Map<String, TSDataType> measurementDataTypeMap = plan.getColumnDataTypeMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    Map<String, String> measurementAliasMap = plan.getMeasurementAliasMap();
    Map<String, MeasurementType> measurementTypeMap = plan.getMeasurementTypeMap();
    for (String measurement : measurements) {
      TSDataType type = TSDataType.TEXT;
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          type = measurementDataTypeMap.get(measurement);
          break;
        case NonExist:
        case Constant:
          type = TSDataType.TEXT;
      }
      respColumns.add(measurementAliasMap.getOrDefault(measurement, measurement));
      columnTypes.add(type.toString());

      if (!deduplicatedMeasurements.contains(measurement)) {
        deduplicatedMeasurements.add(measurement);
        deduplicatedColumnsType.add(type);
      }
    }

    // save deduplicated measurementColumn names and types in QueryPlan for the next stage to use.
    // i.e., used by AlignByDeviceDataSet constructor in `fetchResults` stage.
    plan.setMeasurements(new ArrayList<>(deduplicatedMeasurements));
    plan.setDataTypes(deduplicatedColumnsType);

    // set these null since they are never used henceforth in ALIGN_BY_DEVICE query processing.
    plan.setPaths(null);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSFetchResultsResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      if (!queryId2DataSet.containsKey(req.queryId)) {
        return RpcUtils.getTSFetchResultsResp(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed query"));
      }

      QueryDataSet queryDataSet = queryId2DataSet.get(req.queryId);
      if (req.isAlign) {
        TSQueryDataSet result =
            fillRpcReturnData(req.fetchSize, queryDataSet, sessionIdUsernameMap.get(req.sessionId));
        boolean hasResultSet = result.bufferForTime().limit() != 0;
        if (!hasResultSet) {
          releaseQueryResource(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setQueryDataSet(result);
        resp.setIsAlign(true);
        return resp;
      } else {
        TSQueryNonAlignDataSet nonAlignResult =
            fillRpcNonAlignReturnData(
                req.fetchSize, queryDataSet, sessionIdUsernameMap.get(req.sessionId));
        boolean hasResultSet = false;
        for (ByteBuffer timeBuffer : nonAlignResult.getTimeList()) {
          if (timeBuffer.limit() != 0) {
            hasResultSet = true;
            break;
          }
        }
        if (!hasResultSet) {
          queryId2DataSet.remove(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setNonAlignQueryDataSet(nonAlignResult);
        resp.setIsAlign(false);
        return resp;
      }
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      try {
        releaseQueryResource(req.queryId);
      } catch (StorageEngineException ex) {
        logger.error("Error happened while releasing query resource: ", ex);
      }
      return RpcUtils.getTSFetchResultsResp(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private TSQueryDataSet fillRpcReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, InterruptedException {
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    TSQueryDataSet result;

    if (config.isEnableWatermark() && authorizer.isUserUseWaterMark(userName)) {
      WatermarkEncoder encoder;
      if (config.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
        encoder = new GroupedLSBWatermarkEncoder(config);
      } else {
        throw new UnSupportedDataTypeException(
            String.format(
                "Watermark method is not supported yet: %s", config.getWatermarkMethodName()));
      }
      if (queryDataSet instanceof RawQueryDataSetWithoutValueFilter) {
        // optimize for query without value filter
        result = ((RawQueryDataSetWithoutValueFilter) queryDataSet).fillBuffer(fetchSize, encoder);
      } else {
        result = QueryDataSetUtils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize, encoder);
      }
    } else {
      if (queryDataSet instanceof RawQueryDataSetWithoutValueFilter) {
        // optimize for query without value filter
        result = ((RawQueryDataSetWithoutValueFilter) queryDataSet).fillBuffer(fetchSize, null);
      } else {
        result = QueryDataSetUtils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize);
      }
    }
    return result;
  }

  private TSQueryNonAlignDataSet fillRpcNonAlignReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, InterruptedException {
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    TSQueryNonAlignDataSet result;

    if (config.isEnableWatermark() && authorizer.isUserUseWaterMark(userName)) {
      WatermarkEncoder encoder;
      if (config.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
        encoder = new GroupedLSBWatermarkEncoder(config);
      } else {
        throw new UnSupportedDataTypeException(
            String.format(
                "Watermark method is not supported yet: %s", config.getWatermarkMethodName()));
      }
      result = ((NonAlignEngineDataSet) queryDataSet).fillBuffer(fetchSize, encoder);
    } else {
      result = ((NonAlignEngineDataSet) queryDataSet).fillBuffer(fetchSize, null);
    }
    return result;
  }

  /**
   * create QueryDataSet and buffer it for fetchResults
   */
  private QueryDataSet createQueryDataSet(long queryId, PhysicalPlan physicalPlan)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
      IOException, MetadataException, SQLException, TException, InterruptedException {

    QueryContext context = genQueryContext(queryId, physicalPlan.isDebug());
    QueryDataSet queryDataSet = executor.processQuery(physicalPlan, context);
    queryId2DataSet.put(queryId, queryDataSet);
    return queryDataSet;
  }

  protected QueryContext genQueryContext(long queryId, boolean debug) {
    return new QueryContext(queryId, debug);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement, req.getSessionId());
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan, long sessionId) {
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    status = executeNonQueryPlan(plan);
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(status);
    long queryId = generateQueryId(false);
    resp.setQueryId(queryId);
    return resp;
  }

  private boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement, long sessionId) {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor
          .parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(sessionId));
    } catch (QueryProcessException | SQLParserException e) {
      logger.warn(ERROR_PARSING_SQL, statement, e);
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SQL_PARSE_ERROR, e.getMessage());
    }

    if (physicalPlan.isQuery()) {
      return RpcUtils.getTSExecuteStatementResp(
          TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is a query statement.");
    }

    return executeUpdateStatement(physicalPlan, sessionId);
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  private boolean checkLogin(long sessionId) {
    return sessionIdUsernameMap.get(sessionId) != null;
  }

  private boolean checkAuthorization(List<PartialPath> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  protected void handleClientExit() {
    Long sessionId = currSessionId.get();
    if (sessionId != null) {
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
  }

  @Override
  public TSGetTimeZoneResp getTimeZone(long sessionId) {
    TSStatus tsStatus;
    TSGetTimeZoneResp resp = null;
    try {
      tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      ZoneId zoneId = sessionIdZoneIdMap.get(sessionId);
      if (zoneId != null) {
        resp = new TSGetTimeZoneResp(tsStatus, zoneId.toString());
      }
    } catch (Exception e) {
      logger.error("meet error while generating time zone.", e);
      tsStatus = RpcUtils.getStatus(TSStatusCode.GENERATE_TIME_ZONE_ERROR);
      resp = new TSGetTimeZoneResp(tsStatus, "Unknown time zone");
    }
    return resp;
  }

  @Override
  public TSStatus setTimeZone(TSSetTimeZoneReq req) {
    TSStatus tsStatus;
    try {
      String timeZoneID = req.getTimeZone();
      sessionIdZoneIdMap.put(req.getSessionId(), ZoneId.of(timeZoneID));
      tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      logger.error("meet error while setting time zone.", e);
      tsStatus = RpcUtils.getStatus(TSStatusCode.SET_TIME_ZONE_ERROR);
    }
    return new TSStatus(tsStatus);
  }

  @Override
  public ServerProperties getProperties() {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    logger.info("IoTDB server version: {}", IoTDBConstant.VERSION);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    properties.setTimestampPrecision(
        IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    return properties;
  }

  @Override
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    if (auditLogger.isDebugEnabled()) {
      auditLogger
          .debug("Session {} insertRecords, first device {}, first time {}", currSessionId.get(),
              req.deviceIds.get(0), req.getTimestamps().get(0));
    }
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    List<TSStatus> statusList = new ArrayList<>();

    for (int i = 0; i < req.deviceIds.size(); i++) {
      try {
        InsertRowPlan plan = new InsertRowPlan(
            new PartialPath(req.getDeviceIds().get(i)), req.getTimestamps().get(i),
            req.getMeasurementsList().get(i).toArray(new String[0]), req.valuesList.get(i)
        );
        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          statusList.add(status);
        } else {
          statusList.add(executeNonQueryPlan(plan));
        }
      } catch (Exception e) {
        logger.error("meet error when insert in batch", e);
        statusList.add(RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
      }
    }

    return RpcUtils.getStatus(statusList);
  }

  @Override
  public TSStatus insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) throws TException {
    if (auditLogger.isDebugEnabled()) {
      auditLogger
          .debug("Session {} insertRecords, device {}, first time {}", currSessionId.get(),
              req.deviceId, req.getTimestamps().get(0));
    }
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    List<TSStatus> statusList = new ArrayList<>();

    try {
      InsertRowsOfOneDevicePlan plan = new InsertRowsOfOneDevicePlan(
          new PartialPath(req.getDeviceId()),
          req.getTimestamps().toArray(new Long[0]),
          req.getMeasurementsList(),
          req.getValuesList().toArray(new ByteBuffer[0])
      );
      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        statusList.add(status);
      } else {
        statusList.add(executeNonQueryPlan(plan));
      }
    } catch (Exception e) {
      logger.error("meet error when insert in batch", e);
      statusList.add(RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
    }

    return RpcUtils.getStatus(statusList);
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) throws TException {
    if (auditLogger.isDebugEnabled()) {
      auditLogger
          .debug("Session {} insertRecords, first device {}, first time {}", currSessionId.get(),
              req.deviceIds.get(0), req.getTimestamps().get(0));
    }
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    List<TSStatus> statusList = new ArrayList<>();
    InsertRowPlan plan = new InsertRowPlan();
    for (int i = 0; i < req.deviceIds.size(); i++) {
      try {
        plan.setDeviceId(new PartialPath(req.getDeviceIds().get(i)));
        plan.setTime(req.getTimestamps().get(i));
        plan.setMeasurements(req.getMeasurementsList().get(i).toArray(new String[0]));
        plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
        plan.setValues(
            req.getValuesList().get(i).toArray(new Object[req.getValuesList().get(i).size()]));
        plan.setNeedInferType(true);
        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          statusList.add(status);
        } else {
          statusList.add(executeNonQueryPlan(plan));
        }
      } catch (Exception e) {
        logger.error("meet error when insert in batch", e);
        statusList.add(RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
      }
    }

    return RpcUtils.getStatus(statusList);
  }

  @Override
  public TSStatus testInsertTablet(TSInsertTabletReq req) {
    logger.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertTablets(TSInsertTabletsReq req) {
    logger.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecord(TSInsertRecordReq req) {
    logger.debug("Test insert row request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecord(TSInsertStringRecordReq req) throws TException {
    logger.debug("Test insert string record request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecords(TSInsertRecordsReq req) {
    logger.debug("Test insert row in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) throws TException {
    logger.debug("Test insert rows in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecords(TSInsertStringRecordsReq req) throws TException {
    logger.debug("Test insert string records request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus insertRecord(TSInsertRecordReq req) {
    try {
      auditLogger
          .debug("Session {} insertRecord, device {}, time {}", currSessionId.get(),
              req.getDeviceId(), req.getTimestamp());
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      InsertRowPlan plan = new InsertRowPlan(
          new PartialPath(req.getDeviceId()), req.getTimestamp(),
          req.getMeasurements().toArray(new String[0]), req.values
      );

      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        return status;
      }
      return executeNonQueryPlan(plan);
    } catch (Exception e) {
      logger.error("meet error when insert", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) throws TException {
    try {
      auditLogger
          .debug("Session {} insertRecord, device {}, time {}", currSessionId.get(),
              req.getDeviceId(), req.getTimestamp());
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      InsertRowPlan plan = new InsertRowPlan();
      plan.setDeviceId(new PartialPath(req.getDeviceId()));
      plan.setTime(req.getTimestamp());
      plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
      plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
      plan.setValues(req.getValues().toArray(new Object[req.getValues().size()]));
      plan.setNeedInferType(true);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        return status;
      }
      return executeNonQueryPlan(plan);
    } catch (Exception e) {
      logger.error("meet error when insert", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) throws TException {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      DeletePlan plan = new DeletePlan();
      plan.setDeleteStartTime(req.getStartTime());
      plan.setDeleteEndTime(req.getEndTime());
      List<PartialPath> paths = new ArrayList<>();
      for (String path : req.getPaths()) {
        paths.add(new PartialPath(path));
      }
      plan.addPaths(paths);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        return new TSStatus(status);
      }
      return new TSStatus(executeNonQueryPlan(plan));
    } catch (Exception e) {
      logger.error("meet error when delete data", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus insertTablet(TSInsertTabletReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      InsertTabletPlan insertTabletPlan = new InsertTabletPlan(new PartialPath(req.deviceId),
          req.measurements);
      insertTabletPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      insertTabletPlan.setColumns(
          QueryDataSetUtils.readValuesFromBuffer(
              req.values, req.types, req.measurements.size(), req.size));
      insertTabletPlan.setRowCount(req.size);
      insertTabletPlan.setDataTypes(req.types);

      TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());
      if (status != null) {
        return status;
      }

      return executeNonQueryPlan(insertTabletPlan);
    } catch (Exception e) {
      logger.error("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils
          .getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      List<TSStatus> statusList = new ArrayList<>();
      for (int i = 0; i < req.deviceIds.size(); i++) {
        InsertTabletPlan insertTabletPlan = new InsertTabletPlan(
            new PartialPath(req.deviceIds.get(i)),
            req.measurementsList.get(i));
        insertTabletPlan.setTimes(
            QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i)));
        insertTabletPlan.setColumns(
            QueryDataSetUtils.readValuesFromBuffer(
                req.valuesList.get(i), req.typesList.get(i), req.measurementsList.get(i).size(),
                req.sizeList.get(i)));
        insertTabletPlan.setRowCount(req.sizeList.get(i));
        insertTabletPlan.setDataTypes(req.typesList.get(i));

        TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());
        if (status != null) {
          statusList.add(status);
          continue;
        }

        statusList.add(executeNonQueryPlan(insertTabletPlan));
      }
      return RpcUtils.getStatus(statusList);
    } catch (Exception e) {
      logger.error("{}: error occurs when insertTablets", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils
          .getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      if (!checkLogin(sessionId)) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      SetStorageGroupPlan plan = new SetStorageGroupPlan(new PartialPath(storageGroup));
      TSStatus status = checkAuthority(plan, sessionId);
      if (status != null) {
        return new TSStatus(status);
      }
      return new TSStatus(executeNonQueryPlan(plan));
    } catch (Exception e) {
      logger.error("meet error when set storage group", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    try {
      if (!checkLogin(sessionId)) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }
      List<PartialPath> storageGroupList = new ArrayList<>();
      for (String storageGroup : storageGroups) {
        storageGroupList.add(new PartialPath(storageGroup));
      }
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
      TSStatus status = checkAuthority(plan, sessionId);
      if (status != null) {
        return new TSStatus(status);
      }
      return new TSStatus(executeNonQueryPlan(plan));
    } catch (Exception e) {
      logger.error("meet error when delete storage groups", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      if (auditLogger.isDebugEnabled()) {
        auditLogger.debug("Session-{} create timeseries {}", currSessionId.get(), req.getPath());
      }

      CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new PartialPath(req.path),
          TSDataType.values()[req.dataType], TSEncoding.values()[req.encoding],
          CompressionType.values()[req.compressor], req.props, req.tags, req.attributes,
          req.measurementAlias);
      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        return status;
      }
      return executeNonQueryPlan(plan);
    } catch (Exception e) {
      logger.error("meet error when create timeseries", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }
      if (auditLogger.isDebugEnabled()) {
        auditLogger.debug("Session-{} create {} timeseries, the first is {}", currSessionId.get(),
            req.getPaths().size(), req.getPaths().get(0));
      }
      List<TSStatus> statusList = new ArrayList<>(req.paths.size());
      CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan();
      CreateMultiTimeSeriesPlan createMultiTimeSeriesPlan = new CreateMultiTimeSeriesPlan();

      List<PartialPath> paths = new ArrayList<>(req.paths.size());
      List<TSDataType> dataTypes = new ArrayList<>(req.paths.size());
      List<TSEncoding> encodings = new ArrayList<>(req.paths.size());
      List<CompressionType> compressors = new ArrayList<>(req.paths.size());
      List<String> alias = null;
      if (req.measurementAliasList != null) {
        alias = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> props = null;
      if (req.propsList != null) {
        props = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> tags = null;
      if (req.tagsList != null) {
        tags = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> attributes = null;
      if (req.attributesList != null) {
        attributes = new ArrayList<>(req.paths.size());
      }

      // record the result of creation of time series
      List<Integer> indexes = new ArrayList<>(req.paths.size());

      for (int i = 0; i < req.paths.size(); i++) {
        plan.setPath(new PartialPath(req.paths.get(i)));

        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          // not authorized
          statusList.add(status);
          continue;
        }

        paths.add(new PartialPath(req.paths.get(i)));
        dataTypes.add(TSDataType.values()[req.dataTypes.get(i)]);
        encodings.add(TSEncoding.values()[req.encodings.get(i)]);
        compressors.add(CompressionType.values()[req.compressors.get(i)]);
        if (alias != null) {
          alias.add(req.measurementAliasList.get(i));
        }
        if (props != null) {
          props.add(req.propsList.get(i));
        }
        if (tags != null) {
          tags.add(req.tagsList.get(i));
        }
        if (attributes != null) {
          attributes.add(req.attributesList.get(i));
        }

        indexes.add(i);
        statusList.add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, ""));
      }

      createMultiTimeSeriesPlan.setPaths(paths);
      createMultiTimeSeriesPlan.setDataTypes(dataTypes);
      createMultiTimeSeriesPlan.setEncodings(encodings);
      createMultiTimeSeriesPlan.setCompressors(compressors);
      createMultiTimeSeriesPlan.setAlias(alias);
      createMultiTimeSeriesPlan.setProps(props);
      createMultiTimeSeriesPlan.setTags(tags);
      createMultiTimeSeriesPlan.setAttributes(attributes);
      createMultiTimeSeriesPlan.setIndexes(indexes);

      executeNonQuery(createMultiTimeSeriesPlan);

      boolean isAllSuccessful = true;

      if (createMultiTimeSeriesPlan.getResults().entrySet().size() > 0) {
        isAllSuccessful = false;
        for (Map.Entry<Integer, Exception> entry : createMultiTimeSeriesPlan.getResults()
            .entrySet()) {
          statusList.set(entry.getKey(),
              RpcUtils
                  .getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, entry.getValue().getMessage()));
        }
      }

      if (isAllSuccessful) {
        if (logger.isDebugEnabled()) {
          logger.debug("Create multiple timeseries successfully");
        }
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        logger.debug("Create multiple timeseries failed!");
        return RpcUtils.getStatus(statusList);
      }
    } catch (Exception e) {
      logger.error("meet error when create multi timeseries", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> paths) {
    try {
      if (!checkLogin(sessionId)) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }
      List<PartialPath> pathList = new ArrayList<>();
      for (String path : paths) {
        pathList.add(new PartialPath(path));
      }
      DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
      TSStatus status = checkAuthority(plan, sessionId);
      if (status != null) {
        return status;
      }
      return executeNonQueryPlan(plan);
    } catch (Exception e) {
      logger.error("meet error when delete timeseries", e);
    }
    return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  @Override
  public long requestStatementId(long sessionId) {
    long statementId = statementIdGenerator.incrementAndGet();
    sessionId2StatementId.computeIfAbsent(sessionId, s -> new CopyOnWriteArraySet<>())
        .add(statementId);
    return statementId;
  }

  private TSStatus checkAuthority(PhysicalPlan plan, long sessionId) {
    List<PartialPath> paths = plan.getPaths();
    try {
      if (!checkAuthorization(paths, plan, sessionIdUsernameMap.get(sessionId))) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation " + plan.getOperatorType().toString());
      }
    } catch (AuthException e) {
      logger.error("meet error while checking authorization.", e);
      return RpcUtils.getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      logger.error(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return null;
  }

  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (BatchInsertionException e) {
      return RpcUtils.getStatus(Arrays.asList(e.getFailingStatus()));
    } catch (QueryProcessException e) {
      logger.warn("meet error while processing non-query. ", e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (Exception e) {
      logger.warn(SERVER_INTERNAL_ERROR, IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return execRet
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
        : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }


  private long generateQueryId(boolean isDataQuery) {
    return QueryResourceManager.getInstance().assignQueryId(isDataQuery);
  }

  protected List<TSDataType> getSeriesTypesByPaths(List<PartialPath> paths,
      List<String> aggregations)
      throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths, aggregations);
  }


  protected TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return SchemaUtils.getSeriesTypeByPaths(path);
  }
}
