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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSBatchInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteInsertRowInBatchResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertInBatchReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSStatusType;
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

  private static final Logger logger = LoggerFactory.getLogger(TSServiceImpl.class);
  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private static final int MAX_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getQueryCacheSizeInMetric();
  private static final int DELETE_SIZE = 50;
  private static final String ERROR_PARSING_SQL =
      "meet error while parsing SQL to physical plan: {}";
  public static Vector<SqlArgument> sqlArgumentsList = new Vector<>();

  protected Planner processor;
  protected IPlanExecutor executor;

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

  public TSServiceImpl() throws QueryProcessException {
    processor = new Planner();
    executor = new PlanExecutor();
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    logger.info(
        "{}: receive open session request from username {}",
        IoTDBConstant.GLOBAL_DB_NAME,
        req.getUsername());

    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    try {
      status = authorizer.login(req.getUsername(), req.getPassword());
    } catch (AuthException e) {
      logger.info("meet error while logging in.", e);
      status = false;
    }

    TSStatus tsStatus;
    long sessionId = -1;
    if (status) {
      //check the version compatibility
      boolean compatible = checkCompatibility(req.getClient_protocol());
      if (!compatible) {
        tsStatus = getStatus(TSStatusCode.INCOMPATIBLE_VERSION,
            "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
            TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2);
        resp.setSessionId(sessionId);
        return resp;
      }

      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS, "Login successfully");
      sessionId = sessionIdGenerator.incrementAndGet();
      sessionIdUsernameMap.put(sessionId, req.getUsername());
      sessionIdZoneIdMap.put(sessionId, config.getZoneID());
      currSessionId.set(sessionId);
    } else {
      tsStatus = getStatus(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR);
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2);
    resp.setSessionId(sessionId);
    logger.info(
        "{}: Login status: {}. User : {}",
        IoTDBConstant.GLOBAL_DB_NAME,
        tsStatus.getStatusType().getMessage(),
        req.getUsername());

    return resp;
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2);
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    logger.info("{}: receive close session", IoTDBConstant.GLOBAL_DB_NAME);
    long sessionId = req.getSessionId();
    TSStatus tsStatus;
    if (sessionIdUsernameMap.remove(sessionId) == null) {
      tsStatus = getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    } else {
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    sessionIdZoneIdMap.remove(sessionId);
    List<Exception> exceptions = new ArrayList<>();
    Set<Long> statementIds = sessionId2StatementId.getOrDefault(sessionId, Collections.emptySet());
    for (long statementId : statementIds) {
      Set<Long> queryIds = statementId2QueryId.getOrDefault(statementId, Collections.emptySet());
      for (long queryId : queryIds) {
        queryId2DataSet.remove(queryId);

        try {
          QueryResourceManager.getInstance().endQuery(queryId);
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
          getStatus(
              TSStatusCode.CLOSE_OPERATION_ERROR,
              String.format(
                  "%d errors in closeOperation, see server logs for detail", exceptions.size())));
    }

    return new TSStatus(tsStatus);
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    // TODO implement
    return getStatus(TSStatusCode.QUERY_NOT_ALLOWED, "Cancellation is not implemented");
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    logger.info("{}: receive close operation", IoTDBConstant.GLOBAL_DB_NAME);
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    try {
      // statement close
      if (req.isSetStatementId()) {
        long stmtId = req.getStatementId();
        Set<Long> queryIdSet = statementId2QueryId.remove(stmtId);
        if (queryIdSet != null) {
          for (long queryId : queryIdSet) {
            releaseQueryResource(queryId);
          }
        }
      } else {
        // ResultSet close
        releaseQueryResource(req.queryId);
      }

    } catch (Exception e) {
      logger.error("Error in closeOperation : ", e);
      return getStatus(TSStatusCode.CLOSE_OPERATION_ERROR, "Error in closeOperation");
    }
    return getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * release single operation resource
   */
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // remove the corresponding Physical Plan
    queryId2DataSet.remove(queryId);
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  /**
   * convert from TSStatusCode to TSStatus according to status code and status message
   *
   * @param statusType status type
   */
  static TSStatus getStatus(TSStatusCode statusType) {
    TSStatusType statusCodeAndMessage = new TSStatusType(statusType.getStatusCode(), "");
    return new TSStatus(statusCodeAndMessage);
  }

  /**
   * convert from TSStatusCode to TSStatus, which has message appending with existed status message
   *
   * @param statusType    status type
   * @param appendMessage appending message
   */
  private TSStatus getStatus(TSStatusCode statusType, String appendMessage) {
    TSStatusType statusCodeAndMessage = new TSStatusType(statusType.getStatusCode(), appendMessage);
    return new TSStatus(statusCodeAndMessage);
  }

  protected TSDataType getSeriesType(String path) throws QueryProcessException, MetadataException {
    try {
      return SchemaUtils.getSeriesType(path);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSStatus status;
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      return new TSFetchMetadataResp(status);
    }

    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          String metadataInJson = getMetadataInString();
          resp.setMetadataInJson(metadataInJson);
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesType(req.getColumnPath()).toString());
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(getPaths(req.getColumnPath()));
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        default:
          status = getStatus(TSStatusCode.METADATA_ERROR, req.getType());
          break;
      }
    } catch (MetadataException | OutOfMemoryError | QueryProcessException e) {
      logger.error(
          String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()), e);
      status = getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
      resp.setStatus(status);
      return resp;
    }
    resp.setStatus(status);
    return resp;
  }

  private String getMetadataInString() {
    return MManager.getInstance().getMetadataInString();
  }

  protected List<String> getPaths(String path) throws MetadataException {
    return MManager.getInstance().getAllTimeseriesName(path);
  }

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, execute it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   */
  private boolean execAdminCommand(String statement, long sessionId) throws StorageEngineException {
    if (!"root".equals(sessionIdUsernameMap.get(sessionId))) {
      return false;
    }
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase();
    if (statement.startsWith("flush")) {
      try {
        execFlush(statement);
      } catch (StorageGroupNotSetException e) {
        throw new StorageEngineException(e);
      }
      return true;
    }
    switch (statement) {
      case "merge":
        StorageEngine.getInstance()
            .mergeAll(IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
        return true;
      case "full merge":
        StorageEngine.getInstance().mergeAll(true);
        return true;
      default:
        return false;
    }
  }

  private void execFlush(String statement) throws StorageGroupNotSetException {
    String[] args = statement.split("\\s+");
    if (args.length == 1) {
      StorageEngine.getInstance().syncCloseAllProcessor();
    } else if (args.length == 2) {
      String[] storageGroups = args[1].split(",");
      for (String storageGroup : storageGroups) {
        StorageEngine.getInstance().asyncCloseProcessor(storageGroup, true);
        StorageEngine.getInstance().asyncCloseProcessor(storageGroup, false);
      }
    } else {
      String[] storageGroups = args[1].split(",");
      boolean isSeq = Boolean.parseBoolean(args[2]);
      for (String storageGroup : storageGroups) {
        StorageEngine.getInstance().asyncCloseProcessor(storageGroup, isSeq);
      }
    }
  }

  @Override
  public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<Integer> result = new ArrayList<>();
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR), null);
      }
      List<String> statements = req.getStatements();

      boolean isAllSuccessful = true;
      StringBuilder batchErrorMessage = new StringBuilder();

      for (String statement : statements) {
        long t2 = System.currentTimeMillis();
        isAllSuccessful =
            executeStatementInBatch(statement, batchErrorMessage, result, req.getSessionId())
                && isAllSuccessful;
        Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
      }
      if (isAllSuccessful) {
        return getTSBatchExecuteStatementResp(
            getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully"),
            result);
      } else {
        return getTSBatchExecuteStatementResp(
            getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, batchErrorMessage.toString()), result);
      }
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    }
  }

  // execute one statement of a batch. Currently, query is not allowed in a batch statement and
  // on finding queries in a batch, such query will be ignored and an error will be generated
  private boolean executeStatementInBatch(
      String statement, StringBuilder batchErrorMessage, List<Integer> result, long sessionId) {
    try {
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(sessionId));
      if (physicalPlan.isQuery()) {
        throw new QueryInBatchStatementException(statement);
      }
      TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan, sessionId);
      if (resp.getStatus().getStatusType().getCode()
          == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        result.add(Statement.SUCCESS_NO_INFO);
      } else {
        result.add(Statement.EXECUTE_FAILED);
        batchErrorMessage.append(resp.getStatus().getStatusType().getCode()).append("\n");
        return false;
      }
    } catch (ParseCancellationException e) {
      logger.debug(e.getMessage());
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(TSStatusCode.SQL_PARSE_ERROR.getStatusCode()).append("\n");
      return false;
    } catch (SQLParserException e) {
      logger.error("Error occurred when executing {}, check metadata error: ", statement, e);
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(TSStatusCode.METADATA_ERROR.getStatusCode()).append("\n");
      return false;
    } catch (QueryProcessException e) {
      logger.info(
          "Error occurred when executing {}, meet error while parsing SQL to physical plan: {}",
          statement,
          e.getMessage());
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(TSStatusCode.SQL_PARSE_ERROR.getStatusCode()).append("\n");
      return false;
    } catch (QueryInBatchStatementException e) {
      logger.info("Error occurred when executing {}, query statement not allowed: ", statement, e);
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(TSStatusCode.QUERY_NOT_ALLOWED.getStatusCode()).append("\n");
      return false;
    }
    return true;
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    long startTime = System.currentTimeMillis();
    TSExecuteStatementResp resp;
    SqlArgument sqlArgument;
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }
      String statement = req.getStatement();

      if (execAdminCommand(statement, req.getSessionId())) {
        return getTSExecuteStatementResp(
            getStatus(TSStatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS"));
      }
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(req.getSessionId()));
      if (physicalPlan.isQuery()) {
        resp =
            internalExecuteQueryStatement(
                req.statementId,
                physicalPlan,
                req.fetchSize,
                sessionIdUsernameMap.get(req.getSessionId()));
        long endTime = System.currentTimeMillis();
        sqlArgument = new SqlArgument(resp, physicalPlan, statement, startTime, endTime);
        sqlArgumentsList.add(sqlArgument);
        if (sqlArgumentsList.size() > MAX_SIZE) {
          sqlArgumentsList.subList(0, DELETE_SIZE).clear();
        }
        return resp;
      } else {
        return executeUpdateStatement(physicalPlan, req.getSessionId());
      }
    } catch (ParseCancellationException e) {
      logger.debug(e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    } catch (SQLParserException e) {
      logger.error("check metadata error: ", e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.METADATA_ERROR, "Check metadata error: " + e.getMessage()));
    } catch (QueryProcessException e) {
      logger.info(ERROR_PARSING_SQL, e.getMessage());
      return getTSExecuteStatementResp(
          getStatus(
              TSStatusCode.SQL_PARSE_ERROR, "Statement format is not right: " + e.getMessage()));
    } catch (StorageEngineException e) {
      logger.info(ERROR_PARSING_SQL, e.getMessage());
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.READ_ONLY_SYSTEM_ERROR, e.getMessage()));
    }
  }

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByPlan, some
   *             AuthorPlan
   */
  private TSExecuteStatementResp internalExecuteQueryStatement(
      long statementId, PhysicalPlan plan, int fetchSize, String username) {
    long t1 = System.currentTimeMillis();
    try {
      TSExecuteStatementResp resp = getQueryResp(plan, username); // column headers

      if (plan instanceof QueryPlan && !((QueryPlan) plan).isAlignByTime()) {
        if (plan.getOperatorType() == OperatorType.AGGREGATION) {
          throw new QueryProcessException("Aggregation doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.FILL) {
          throw new QueryProcessException("Fill doesn't support disable align clause.");
        }
        if (plan.getOperatorType() == OperatorType.GROUPBY) {
          throw new QueryProcessException("Group by doesn't support disable align clause.");
        }
      }
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        resp.setIgnoreTimeStamp(true);
      } // else default ignoreTimeStamp is false
      resp.setOperationType(plan.getOperatorType().toString());
      // generate the queryId for the operation
      long queryId = generateQueryId(true);
      // put it into the corresponding Set

      statementId2QueryId.computeIfAbsent(statementId, k -> new HashSet<>()).add(queryId);

      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(queryId, plan);
      if (plan instanceof QueryPlan && !((QueryPlan) plan).isAlignByTime()) {
        TSQueryNonAlignDataSet result = fillRpcNonAlignReturnData(fetchSize, newDataSet, username);
        resp.setNonAlignQueryDataSet(result);
      } else {
        TSQueryDataSet result = fillRpcReturnData(fetchSize, newDataSet, username);
        resp.setQueryDataSet(result);
      }
      resp.setQueryId(queryId);
      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, t1);
    }
  }

  private TSExecuteStatementResp getQueryResp(PhysicalPlan plan, String username)
      throws QueryProcessException, AuthException, TException {
    if (plan instanceof AuthorPlan) {
      return getAuthQueryColumnHeaders(plan);
    } else if (plan instanceof ShowPlan) {
      return getShowQueryColumnHeaders((ShowPlan) plan);
    } else {
      return getQueryColumnHeaders(plan, username);
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    long startTime = System.currentTimeMillis();
    TSExecuteStatementResp resp;
    SqlArgument sqlArgument;

    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
    }

    String statement = req.getStatement();
    PhysicalPlan physicalPlan;
    try {
      physicalPlan =
          processor.parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(req.getSessionId()));
    } catch (QueryProcessException | SQLParserException e) {
      logger.info(ERROR_PARSING_SQL, e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    }

    if (!physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement."));
    }

    resp = internalExecuteQueryStatement(
        req.statementId, physicalPlan, req.fetchSize, sessionIdUsernameMap.get(req.getSessionId()));
    long endTime = System.currentTimeMillis();
    sqlArgument = new SqlArgument(resp, physicalPlan, statement, startTime, endTime);
    sqlArgumentsList.add(sqlArgument);
    if (sqlArgumentsList.size() > MAX_SIZE) {
      sqlArgumentsList.subList(0, DELETE_SIZE).clear();
    }
    return resp;
  }

  private TSExecuteStatementResp getShowQueryColumnHeaders(ShowPlan showPlan)
      throws QueryProcessException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return StaticResps.TTL_RESP;
      case FLUSH_TASK_INFO:
        return StaticResps.FLUSH_INFO_RESP;
      case DYNAMIC_PARAMETER:
        return StaticResps.DYNAMIC_PARAMETER_RESP;
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
        return getTSExecuteStatementResp(
            getStatus(
                TSStatusCode.SQL_PARSE_ERROR,
                String.format("%s is not an auth query", authorPlan.getAuthorType())));
    }
  }

  /**
   * get ResultSet schema
   */
  private TSExecuteStatementResp getQueryColumnHeaders(PhysicalPlan physicalPlan, String username)
      throws AuthException, TException, QueryProcessException {

    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(physicalPlan.getPaths(), physicalPlan, username)) {
      return getTSExecuteStatementResp(
          getStatus(
              TSStatusCode.NO_PERMISSION_ERROR,
              "No permissions for this operation " + physicalPlan.getOperatorType()));
    }

    TSExecuteStatementResp resp = getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));

    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, respColumns, columnsTypes);
    } else if (plan instanceof LastQueryPlan) {
      return StaticResps.LAST_RESP;
    } else {
      getWideQueryHeaders(plan, respColumns, columnsTypes);
    }
    resp.setColumns(respColumns);
    resp.setDataTypeList(columnsTypes);
    return resp;
  }

  // wide means not align by device
  private void getWideQueryHeaders(
      QueryPlan plan, List<String> respColumns, List<String> columnTypes)
      throws TException, QueryProcessException {
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    List<Path> paths = plan.getPaths();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (Path p : paths) {
          respColumns.add(p.getFullPath());
        }
        break;
      case AGGREGATION:
      case GROUPBY:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          respColumns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
        }
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }

    for (String column : respColumns) {
      try {
        columnTypes.add(getSeriesType(column).toString());
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
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
    Map<String, TSDataType> checker = plan.getMeasurementDataTypeMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    Map<String, MeasurementType> measurementTypeMap = plan.getMeasurementTypeMap();
    for (String measurement : measurements) {
      TSDataType type = null;
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          type = checker.get(measurement);
          break;
        case NonExist:
        case Constant:
          type = TSDataType.TEXT;
      }
      respColumns.add(measurement);
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

  private void getLastQueryHeaders(
      QueryPlan plan, List<String> respColumns, List<String> columnTypes)
      throws TException, QueryProcessException {
    respColumns.add(COLUMN_TIMESERIES);
    respColumns.add(COLUMN_VALUE);
    columnTypes.add(TSDataType.TEXT.toString());
    columnTypes.add(TSDataType.TEXT.toString());
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return getTSFetchResultsResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }

      if (!queryId2DataSet.containsKey(req.queryId)) {
        return getTSFetchResultsResp(
            getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed query"));
      }

      QueryDataSet queryDataSet = queryId2DataSet.get(req.queryId);
      if (req.isAlign) {
        TSQueryDataSet result =
            fillRpcReturnData(req.fetchSize, queryDataSet, sessionIdUsernameMap.get(req.sessionId));
        boolean hasResultSet = result.bufferForTime().limit() != 0;
        if (!hasResultSet) {
          QueryResourceManager.getInstance().endQuery(req.queryId);
          queryId2DataSet.remove(req.queryId);
        }
        TSFetchResultsResp resp =
            getTSFetchResultsResp(
                getStatus(
                    TSStatusCode.SUCCESS_STATUS,
                    "FetchResult successfully. Has more result: " + hasResultSet));
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
        TSFetchResultsResp resp =
            getTSFetchResultsResp(
                getStatus(
                    TSStatusCode.SUCCESS_STATUS,
                    "FetchResult successfully. Has more result: " + hasResultSet));
        resp.setHasResultSet(hasResultSet);
        resp.setNonAlignQueryDataSet(nonAlignResult);
        resp.setIsAlign(false);
        return resp;
      }
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSFetchResultsResp(getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private TSQueryDataSet fillRpcReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, InterruptedException {
    IAuthorizer authorizer;
    try {
      authorizer = LocalFileAuthorizer.getInstance();
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
      authorizer = LocalFileAuthorizer.getInstance();
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
      IOException, MetadataException, SQLException {

    QueryContext context = genQueryContext(queryId);
    QueryDataSet queryDataSet = executor.processQuery(physicalPlan, context);
    queryId2DataSet.put(queryId, queryDataSet);
    return queryDataSet;
  }

  protected QueryContext genQueryContext(long queryId) {
    return new QueryContext(queryId);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement, req.getSessionId());
    } catch (Exception e) {
      logger.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan, long sessionId) {
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    status = executePlan(plan);
    TSExecuteStatementResp resp = getTSExecuteStatementResp(status);
    long queryId = generateQueryId(false);
    resp.setQueryId(queryId);
    return resp;
  }

  private boolean executeNonQuery(PhysicalPlan plan) throws QueryProcessException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement, long sessionId) {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, sessionIdZoneIdMap.get(sessionId));
    } catch (QueryProcessException | SQLParserException e) {
      logger.info(ERROR_PARSING_SQL, e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    }

    if (physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is a query statement."));
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

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  static TSExecuteStatementResp getTSExecuteStatementResp(TSStatus status) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  private TSExecuteBatchStatementResp getTSBatchExecuteStatementResp(
      TSStatus status, List<Integer> result) {
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    resp.setResult(result);
    return resp;
  }

  private TSFetchResultsResp getTSFetchResultsResp(TSStatus status) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
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
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS);
      ZoneId zoneId = sessionIdZoneIdMap.get(sessionId);
      if (zoneId != null) {
        resp = new TSGetTimeZoneResp(tsStatus, zoneId.toString());
      }
    } catch (Exception e) {
      logger.error("meet error while generating time zone.", e);
      tsStatus = getStatus(TSStatusCode.GENERATE_TIME_ZONE_ERROR);
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
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      logger.error("meet error while setting time zone.", e);
      tsStatus = getStatus(TSStatusCode.SET_TIME_ZONE_ERROR);
    }
    return new TSStatus(tsStatus);
  }

  @Override
  public ServerProperties getProperties() {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    properties.setTimestampPrecision(
        IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    return properties;
  }

  @Override
  public TSExecuteInsertRowInBatchResp insertRowInBatch(TSInsertInBatchReq req) {
    TSExecuteInsertRowInBatchResp resp = new TSExecuteInsertRowInBatchResp();
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      resp.addToStatusList(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      return resp;
    }

    InsertPlan plan = new InsertPlan();
    for (int i = 0; i < req.deviceIds.size(); i++) {
      plan.setDeviceId(req.getDeviceIds().get(i));
      plan.setTime(req.getTimestamps().get(i));
      plan.setMeasurements(req.getMeasurementsList().get(i).toArray(new String[0]));
      plan.setValues(req.getValuesList().get(i).toArray(new String[0]));
      TSStatus status = checkAuthority(plan, req.getSessionId());
      if (status != null) {
        resp.addToStatusList(new TSStatus(status));
      } else {
        resp.addToStatusList(executePlan(plan));
      }
    }

    return resp;
  }

  @Override
  public TSExecuteBatchStatementResp testInsertBatch(TSBatchInsertionReq req) {
    logger.debug("Test insert batch request receive.");
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    resp.setStatus(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setResult(Collections.emptyList());
    return resp;
  }

  @Override
  public TSStatus testInsertRow(TSInsertReq req) {
    logger.debug("Test insert row request receive.");
    return getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSExecuteInsertRowInBatchResp testInsertRowInBatch(TSInsertInBatchReq req) {
    logger.debug("Test insert row in batch request receive.");

    TSExecuteInsertRowInBatchResp resp = new TSExecuteInsertRowInBatchResp();
    resp.addToStatusList(getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }

  @Override
  public TSStatus insert(TSInsertReq req) {
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    InsertPlan plan = new InsertPlan();
    plan.setDeviceId(req.getDeviceId());
    plan.setTime(req.getTimestamp());
    plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
    plan.setValues(req.getValues().toArray(new String[0]));

    TSStatus status = checkAuthority(plan, req.getSessionId());
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    DeletePlan plan = new DeletePlan();
    plan.setDeleteTime(req.getTimestamp());
    List<Path> paths = new ArrayList<>();
    for (String path : req.getPaths()) {
      paths.add(new Path(path));
    }
    plan.addPaths(paths);

    TSStatus status = checkAuthority(plan, req.getSessionId());
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSExecuteBatchStatementResp insertBatch(TSBatchInsertionReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR), null);
      }

      BatchInsertPlan batchInsertPlan = new BatchInsertPlan(req.deviceId, req.measurements);
      batchInsertPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      batchInsertPlan.setColumns(
          QueryDataSetUtils.readValuesFromBuffer(
              req.values, req.types, req.measurements.size(), req.size));
      batchInsertPlan.setRowCount(req.size);
      batchInsertPlan.setDataTypes(req.types);

      boolean isAllSuccessful = true;
      TSStatus status = checkAuthority(batchInsertPlan, req.getSessionId());
      if (status != null) {
        return new TSExecuteBatchStatementResp(status);
      }
      Integer[] results = executor.insertBatch(batchInsertPlan);

      for (Integer result : results) {
        if (result != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          isAllSuccessful = false;
          break;
        }
      }

      if (isAllSuccessful) {
        logger.debug("Insert one RowBatch successfully");
        return getTSBatchExecuteStatementResp(
            getStatus(TSStatusCode.SUCCESS_STATUS), Arrays.asList(results));
      } else {
        logger.debug("Insert one RowBatch failed!");
        return getTSBatchExecuteStatementResp(
            getStatus(TSStatusCode.INTERNAL_SERVER_ERROR), Arrays.asList(results));
      }
    } catch (Exception e) {
      logger.info("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSBatchExecuteStatementResp(
          getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()), null);
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    if (!checkLogin(sessionId)) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    SetStorageGroupPlan plan = new SetStorageGroupPlan(new Path(storageGroup));
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    if (!checkLogin(sessionId)) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    List<Path> storageGroupList = new ArrayList<>();
    for (String storageGroup : storageGroups) {
      storageGroupList.add(new Path(storageGroup));
    }
    DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    if (!checkLogin(req.getSessionId())) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    CreateTimeSeriesPlan plan =
        new CreateTimeSeriesPlan(
            new Path(req.getPath()),
            TSDataType.values()[req.getDataType()],
            TSEncoding.values()[req.getEncoding()],
            CompressionType.values()[req.compressor],
            new HashMap<>());
    TSStatus status = checkAuthority(plan, req.getSessionId());
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> paths) {
    if (!checkLogin(sessionId)) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    List<Path> pathList = new ArrayList<>();
    for (String path : paths) {
      pathList.add(new Path(path));
    }
    DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public long requestStatementId(long sessionId) {
    long statementId = statementIdGenerator.incrementAndGet();
    sessionId2StatementId.computeIfAbsent(sessionId, s -> new HashSet<>()).add(statementId);
    return statementId;
  }

  private TSStatus checkAuthority(PhysicalPlan plan, long sessionId) {
    List<Path> paths = plan.getPaths();
    try {
      if (!checkAuthorization(paths, plan, sessionIdUsernameMap.get(sessionId))) {
        return getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation " + plan.getOperatorType().toString());
      }
    } catch (AuthException e) {
      logger.error("meet error while checking authorization.", e);
      return getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    }
    return null;
  }

  protected TSStatus executePlan(PhysicalPlan plan) {
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (QueryProcessException e) {
      logger.debug("meet error while processing non-query. ", e);
      return new TSStatus(new TSStatusType(e.getErrorCode(), e.getMessage()));
    }

    return execRet
        ? getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
        : getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  private long generateQueryId(boolean isDataQuery) {
    return QueryResourceManager.getInstance().assignQueryId(isDataQuery);
  }
}
