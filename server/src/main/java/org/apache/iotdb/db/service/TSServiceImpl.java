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
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.*;

import static org.apache.iotdb.db.conf.IoTDBConstant.*;

/**
 * Thrift RPC implementation at server side.
 */

public class TSServiceImpl implements TSIService.Iface, ServerContext {

  private static final Logger logger = LoggerFactory.getLogger(TSServiceImpl.class);
  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private static final int MAX_SIZE = 200;
  private static final int DELETE_SIZE = 50;
  public static Vector<SqlArgument> sqlArgumentsList = new Vector<>();

  protected QueryProcessor processor;
  // Record the username for every rpc connection. Username.get() is null if
  // login is failed.
  protected ThreadLocal<String> username = new ThreadLocal<>();

  // The statementId is unique in one session for each statement.
  private ThreadLocal<Long> statementIdGenerator = new ThreadLocal<>();
  // The operationIdGenerator is unique in one session for each operation.
  private ThreadLocal<Long> operationIdGenerator = new ThreadLocal<>();
  // (statement -> Set(queryId))
  private ThreadLocal<Map<Long, Set<Long>>> statementId2QueryId = new ThreadLocal<>();
  // (queryId -> PhysicalPlan)
  private ThreadLocal<Map<Long, PhysicalPlan>> queryId2Plan = new ThreadLocal<>();
  // (queryId -> QueryDataSet)
  private ThreadLocal<Map<Long, QueryDataSet>> queryId2DataSet = new ThreadLocal<>();
  private ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private ThreadLocal<Map<Long, QueryContext>> contextMapLocal = new ThreadLocal<>();

  public TSServiceImpl() {
    processor = new QueryProcessor(new QueryProcessExecutor());
  }

  public static TSDataType getSeriesType(String path) throws QueryProcessException {
    switch (path.toLowerCase()) {
      // authorization queries
      case ROLE:
      case USER:
      case PRIVILEGE:
      case STORAGE_GROUP:
        return TSDataType.TEXT;
      case TTL:
        return TSDataType.INT64;
      default:
        // do nothing
    }

    if (path.contains("(") && !path.startsWith("(") && path.endsWith(")")) {
      // aggregation
      int leftBracketIndex = path.indexOf('(');
      String aggrType = path.substring(0, leftBracketIndex);
      String innerPath = path.substring(leftBracketIndex + 1, path.length() - 1);
      switch (aggrType.toLowerCase()) {
        case SQLConstant.MIN_TIME:
        case SQLConstant.MAX_TIME:
        case SQLConstant.COUNT:
          return TSDataType.INT64;
        case SQLConstant.LAST_VALUE:
        case SQLConstant.FIRST_VALUE:
        case SQLConstant.MIN_VALUE:
        case SQLConstant.MAX_VALUE:
          return getSeriesType(innerPath);
        case SQLConstant.AVG:
        case SQLConstant.SUM:
          return TSDataType.DOUBLE;
        default:
          throw new QueryProcessException(
              "aggregate does not support " + aggrType + " function.");
      }
    }
    return MManager.getInstance().getSeriesType(path);
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    logger.info("{}: receive open session request from username {}", IoTDBConstant.GLOBAL_DB_NAME,
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
    if (status) {
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS, "Login successfully");
      username.set(req.getUsername());
      zoneIds.set(config.getZoneID());
      initForOneSession();
    } else {
      tsStatus = getStatus(TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR);
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1);
    resp.setSessionHandle(
        new TS_SessionHandle(new TSHandleIdentifier(ByteBuffer.wrap(req.getUsername().getBytes()),
            ByteBuffer.wrap(req.getPassword().getBytes()), -1L)));
    logger.info("{}: Login status: {}. User : {}", IoTDBConstant.GLOBAL_DB_NAME,
        tsStatus.getStatusType().getMessage(), req.getUsername());

    return resp;
  }

  private void initForOneSession() {
    queryId2Plan.set(new HashMap<>());
    queryId2DataSet.set(new HashMap<>());
    operationIdGenerator.set(0L);
    statementIdGenerator.set(0L);
    contextMapLocal.set(new HashMap<>());
    statementId2QueryId.set(new HashMap<>());
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    logger.info("{}: receive close session", IoTDBConstant.GLOBAL_DB_NAME);
    TSStatus tsStatus;
    if (username.get() == null) {
      tsStatus = getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    } else {
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS);
      username.remove();
    }
    if (zoneIds.get() != null) {
      zoneIds.remove();
    }
    // clear the statementId counter
    if (statementIdGenerator.get() != null) {
      statementIdGenerator.remove();
    }
    // clear the queryId counter
    if (operationIdGenerator.get() != null) {
      operationIdGenerator.remove();
    }
    // clear all cached physical plans of the connection
    if (queryId2Plan.get() != null) {
      queryId2Plan.remove();
    }
    // clear all cached ResultSets of the connection
    if (queryId2DataSet.get() != null) {
      queryId2DataSet.remove();
    }
    // clear all cached query context of the connection
    if (contextMapLocal.get() != null) {
      try {
        for (QueryContext context : contextMapLocal.get().values()) {
          QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
        }
        contextMapLocal.remove();
      } catch (StorageEngineException e) {
        logger.error("Error in closeSession : ", e);
        return new TSStatus(
            getStatus(TSStatusCode.CLOSE_OPERATION_ERROR, "Error in closeOperation"));
      }
    }
    // clear the statementId to queryId map
    if (statementId2QueryId.get() != null) {
      statementId2QueryId.remove();
    }

    return new TSStatus(tsStatus);
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    return getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    logger.info("{}: receive close operation", IoTDBConstant.GLOBAL_DB_NAME);
    try {
      // statement close
      if (req.isSetStmtId()) {
        long stmtId = req.getStmtId();
        Set<Long> queryIdSet = statementId2QueryId.get().get(stmtId);
        if (queryIdSet != null) {
          for (long queryId : queryIdSet) {
            releaseQueryResource(queryId);
          }
          statementId2QueryId.get().remove(stmtId);
        }
      }
      // ResultSet close
      else {
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
  private void releaseQueryResource(long queryId) throws StorageEngineException {

    // remove the corresponding Physical Plan
    if (queryId2Plan.get() != null) {
      queryId2Plan.get().remove(queryId);
    }
    // remove the corresponding Dataset
    if (queryId2DataSet.get() != null) {
      queryId2DataSet.get().remove(queryId);
    }
    // remove the corresponding query context and query resource
    if (contextMapLocal.get() != null && contextMapLocal.get().containsKey(queryId)) {
      QueryResourceManager.getInstance()
          .endQueryForGivenJob(contextMapLocal.get().remove(queryId).getJobId());
    }
  }

  /**
   * convert from TSStatusCode to TSStatus according to status code and status message
   *
   * @param statusType status type
   */
  private TSStatus getStatus(TSStatusCode statusType) {
    TSStatusType statusCodeAndMessage = new TSStatusType(statusType.getStatusCode(), "");
    return new TSStatus(statusCodeAndMessage);
  }

  /**
   * convert from TSStatusCode to TSStatus, which has message appending with existed status message
   *
   * @param statusType status type
   * @param appendMessage appending message
   */
  private TSStatus getStatus(TSStatusCode statusType, String appendMessage) {
    TSStatusType statusCodeAndMessage = new TSStatusType(statusType.getStatusCode(), appendMessage);
    return new TSStatus(statusCodeAndMessage);
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSStatus status;
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      return new TSFetchMetadataResp(status);
    }
    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    try {
      switch (req.getType()) {
        case "SHOW_TIMESERIES":
          String path = req.getColumnPath();
          List<List<String>> timeseriesList = getTimeSeriesForPath(path);
          resp.setTimeseriesList(timeseriesList);
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "SHOW_STORAGE_GROUP":
          Set<String> storageGroups = new HashSet<>(getAllStorageGroups());
          resp.setStorageGroups(storageGroups);
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "METADATA_IN_JSON":
          String metadataInJson = getMetadataInString();
          resp.setMetadataInJson(metadataInJson);
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "SHOW_DEVICES":
          Set<String> devices = getAllDevices();
          resp.setDevices(devices);
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "SHOW_CHILD_PATHS":
          path = req.getColumnPath();
          Set<String> childPaths = getChildPaths(path);
          resp.setChildPaths(childPaths);
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
        case "COUNT_TIMESERIES":
          resp.setTimeseriesNum(getPaths(req.getColumnPath()).size());
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COUNT_NODES":
          resp.setNodesList(getNodesList(req.getColumnPath(), req.getNodeLevel()));
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COUNT_NODE_TIMESERIES":
          resp.setNodeTimeseriesNum(
              getNodeTimeseriesNum(getNodesList(req.getColumnPath(), req.getNodeLevel())));
          status = getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        default:
          status = getStatus(TSStatusCode.METADATA_ERROR, req.getType());
          break;
      }
    } catch (QueryProcessException | MetadataException | OutOfMemoryError | SQLException e) {
      logger
          .error(String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()), e);
      status = getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
      resp.setStatus(status);
      return resp;
    }
    resp.setStatus(status);
    return resp;
  }

  private Map<String, String> getNodeTimeseriesNum(List<String> nodes)
      throws MetadataException {
    Map<String, String> nodeColumnsNum = new HashMap<>();
    for (String columnPath : nodes) {
      nodeColumnsNum.put(columnPath, Integer.toString(getPaths(columnPath).size()));
    }
    return nodeColumnsNum;
  }

  private List<String> getNodesList(String schemaPattern, int level) throws SQLException {
    return MManager.getInstance().getNodesList(schemaPattern, level);
  }

  private List<String> getAllStorageGroups() {
    return MManager.getInstance().getAllStorageGroupNames();
  }

  private Set<String> getAllDevices() throws SQLException {
    return MManager.getInstance().getAllDevices();
  }

  private Set<String> getChildPaths(String path) throws PathException {
    return MManager.getInstance().getChildNodePathInNextLevel(path);
  }

  private List<List<String>> getTimeSeriesForPath(String path)
      throws PathException {
    return MManager.getInstance().getShowTimeseriesPath(path);
  }

  private String getMetadataInString() {
    return MManager.getInstance().getMetadataInString();
  }

  protected List<String> getPaths(String path) throws MetadataException {
    return MManager.getInstance().getPaths(path);
  }

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, execute it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   */
  private boolean execAdminCommand(String statement) throws StorageEngineException {
    if (!"root".equals(username.get())) {
      return false;
    }
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase();
    switch (statement) {
      case "flush":
        StorageEngine.getInstance().syncCloseAllProcessor();
        return true;
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

  @Override
  public TSExecuteBatchStatementResp executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<Integer> result = new ArrayList<>();
    try {
      if (!checkLogin()) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR), null);
      }
      List<String> statements = req.getStatements();

      boolean isAllSuccessful = true;
      StringBuilder batchErrorMessage = new StringBuilder();

      for (String statement : statements) {
        long t2 = System.currentTimeMillis();
        isAllSuccessful =
            isAllSuccessful && executeStatementInBatch(statement, batchErrorMessage, result);
        Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
      }
      if (isAllSuccessful) {
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS,
            "Execute batch statements successfully"), result);
      } else {
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR,
            batchErrorMessage.toString()), result);
      }
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    }
  }

  // execute one statement of a batch. Currently, query is not allowed in a batch statement and
  // on finding queries in a batch, such query will be ignored and an error will be generated
  private boolean executeStatementInBatch(String statement, StringBuilder batchErrorMessage,
      List<Integer> result) {
    try {
      PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      if (physicalPlan.isQuery()) {
        throw new QueryInBatchStatementException(statement);
      }
      TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan);
      if (resp.getStatus().getStatusType().getCode() == TSStatusCode.SUCCESS_STATUS
          .getStatusCode()) {
        result.add(Statement.SUCCESS_NO_INFO);
      } else {
        result.add(Statement.EXECUTE_FAILED);
        batchErrorMessage.append(resp.getStatus().getStatusType().getCode()).append("\n");
        return false;
      }
    } catch (SQLParserException e) {
      logger.error("Error occurred when executing {}, check metadata error: ", statement, e);
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(TSStatusCode.METADATA_ERROR.getStatusCode()).append("\n");
      return false;
    } catch (QueryProcessException e) {
      logger.info(
          "Error occurred when executing {}, meet error while parsing SQL to physical plan: {}",
          statement, e.getMessage());
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
      if (!checkLogin()) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }
      String statement = req.getStatement();

      if (execAdminCommand(statement)) {
        return getTSExecuteStatementResp(
            getStatus(TSStatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS"));
      }

      PhysicalPlan physicalPlan;
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      if (physicalPlan.isQuery()) {
        resp = executeQueryStatement(req.statementId, physicalPlan);
        long endTime = System.currentTimeMillis();
        sqlArgument = new SqlArgument(resp, physicalPlan, statement, startTime, endTime);
        sqlArgumentsList.add(sqlArgument);
        if (sqlArgumentsList.size() > MAX_SIZE) {
          sqlArgumentsList.subList(0, DELETE_SIZE).clear();
        }
        return resp;
      } else {
        return executeUpdateStatement(physicalPlan);
      }
    } catch (ParseCancellationException e) {
      logger.debug(e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    } catch (SQLParserException e) {
      logger.error("check metadata error: ", e);
      return getTSExecuteStatementResp(getStatus(TSStatusCode.METADATA_ERROR,
          "Check metadata error: " + e.getMessage()));
    } catch (QueryProcessException e) {
      logger.info("meet error while parsing SQL to physical plan: {}", e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR,
          "Statement format is not right: " + e.getMessage()));
    } catch (StorageEngineException e) {
      logger.info("meet error while parsing SQL to physical plan: {}", e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.READ_ONLY_SYSTEM_ERROR,
          e.getMessage()));
    }
  }

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByPlan, some
   * AuthorPlan
   */
  private TSExecuteStatementResp executeQueryStatement(long statementId, PhysicalPlan plan) {
    long t1 = System.currentTimeMillis();
    try {
      TSExecuteStatementResp resp; // column headers
      if (plan instanceof AuthorPlan) {
        resp = getAuthQueryColumnHeaders(plan);
      } else if (plan instanceof ShowPlan) {
        resp = getShowQueryColumnHeaders((ShowPlan) plan);
      } else {
        resp = getQueryColumnHeaders(plan);
      }
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        resp.setIgnoreTimeStamp(true);
      } // else default ignoreTimeStamp is false
      resp.setOperationType(plan.getOperatorType().toString());
      // generate the queryId for the operation
      long queryId = generateOperationId();
      // put it into the corresponding Set
      Set<Long> queryIdSet = statementId2QueryId.get()
          .computeIfAbsent(statementId, k -> new HashSet<>());
      queryIdSet.add(queryId);

      TSHandleIdentifier operationId = new TSHandleIdentifier(
          ByteBuffer.wrap(username.get().getBytes()), ByteBuffer.wrap("PASS".getBytes()),
          queryId);
      TSOperationHandle operationHandle = new TSOperationHandle(operationId, true);
      resp.setOperationHandle(operationHandle);

      queryId2Plan.get().put(queryId, plan);
      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, t1);
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
    }

    String statement = req.getStatement();
    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
    } catch (QueryProcessException | SQLParserException e) {
      logger.info("meet error while parsing SQL to physical plan: {}", e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    }

    if (!physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR,
          "Statement is not a query statement."));
    }
    return executeQueryStatement(req.statementId, physicalPlan);
  }

  private List<String> queryColumnsType(List<String> columns) throws QueryProcessException {
    List<String> columnTypes = new ArrayList<>();
    for (String column : columns) {
      columnTypes.add(getSeriesType(column).toString());
    }
    return columnTypes;
  }

  private TSExecuteStatementResp getShowQueryColumnHeaders(ShowPlan showPlan) throws Exception {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return executeShowTTL();
      case FLUSH_TASK_INFO:
        return executeShowFlushTaskInfo();
      case DYNAMIC_PARAMETER:
        return executeShowDynamicParameter();
      case VERSION:
        return executeShowVersion();
      default:
        logger.error("Unsupported show content type: {}", showPlan.getShowContentType());
        throw new Exception("Unsupported show content type:" + showPlan.getShowContentType());
    }
  }

  private TSExecuteStatementResp executeShowTTL() {
    TSExecuteStatementResp resp =
        getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setIgnoreTimeStamp(true);
    List<String> columns = new ArrayList<>();
    List<String> columnTypes = new ArrayList<>();
    columns.add(STORAGE_GROUP);
    columns.add(TTL);
    columnTypes.add(TSDataType.TEXT.toString());
    columnTypes.add(TSDataType.INT64.toString());
    resp.setColumns(columns);
    resp.setDataTypeList(columnTypes);
    return resp;
  }

  private TSExecuteStatementResp executeShowFlushTaskInfo() {
    TSExecuteStatementResp resp =
        getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setIgnoreTimeStamp(true);
    List<String> columns = new ArrayList<>();
    List<String> columnTypes = new ArrayList<>();
    columns.add(ITEM);
    columns.add(VALUE);
    columnTypes.add(TSDataType.TEXT.toString());
    columnTypes.add(TSDataType.TEXT.toString());
    resp.setColumns(columns);
    resp.setDataTypeList(columnTypes);
    return resp;
  }

  private TSExecuteStatementResp executeShowDynamicParameter() {
    TSExecuteStatementResp resp =
        getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setIgnoreTimeStamp(true);
    List<String> columns = new ArrayList<>();
    List<String> columnTypes = new ArrayList<>();
    columns.add(PARAMETER);
    columns.add(VALUE);
    columnTypes.add(TSDataType.TEXT.toString());
    columnTypes.add(TSDataType.TEXT.toString());
    resp.setColumns(columns);
    resp.setDataTypeList(columnTypes);
    return resp;
  }

  private TSExecuteStatementResp executeShowVersion() {
    TSExecuteStatementResp resp =
        getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setIgnoreTimeStamp(true);
    List<String> columns = new ArrayList<>();
    List<String> columnTypes = new ArrayList<>();
    columns.add("version        ");
    columnTypes.add(TSDataType.TEXT.toString());
    resp.setColumns(columns);
    resp.setDataTypeList(columnTypes);
    return resp;
  }

  private TSExecuteStatementResp getAuthQueryColumnHeaders(PhysicalPlan plan) {
    TSExecuteStatementResp resp = getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setIgnoreTimeStamp(true);
    AuthorPlan authorPlan = (AuthorPlan) plan;
    List<String> columnsName = new ArrayList<>();
    List<String> columnsType = new ArrayList<>();
    switch (authorPlan.getAuthorType()) {
      case LIST_ROLE:
      case LIST_USER_ROLES:
        columnsName.add(ROLE);
        columnsType.add(TSDataType.TEXT.toString());
        break;
      case LIST_USER:
      case LIST_ROLE_USERS:
        columnsName.add(USER);
        columnsType.add(TSDataType.TEXT.toString());
        break;
      case LIST_ROLE_PRIVILEGE:
        columnsName.add(PRIVILEGE);
        columnsType.add(TSDataType.TEXT.toString());
        break;
      case LIST_USER_PRIVILEGE:
        columnsName.add(ROLE);
        columnsName.add(PRIVILEGE);
        columnsType.add(TSDataType.TEXT.toString());
        columnsType.add(TSDataType.TEXT.toString());
        break;
      default:
        return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR,
            String.format("%s is not an auth query", authorPlan.getAuthorType())));
    }
    resp.setColumns(columnsName);
    resp.setDataTypeList(columnsType);
    return resp;
  }


  /**
   * get ResultSet schema
   */
  private TSExecuteStatementResp getQueryColumnHeaders(PhysicalPlan plan)
      throws AuthException, TException, QueryProcessException {
    List<Path> paths = plan.getPaths();
    List<String> respColumns = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(paths, plan)) {
      return getTSExecuteStatementResp(getStatus(TSStatusCode.NO_PERMISSION_ERROR));
    }

    TSExecuteStatementResp resp = getTSExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS));

    // group by device query
    if (((QueryPlan) plan).isGroupByDevice()) {
      // set columns in TSExecuteStatementResp. Note this is without deduplication.
      List<String> measurementColumns = ((QueryPlan) plan).getMeasurementColumnList();
      respColumns.add(SQLConstant.GROUPBY_DEVICE_COLUMN_NAME);
      respColumns.addAll(measurementColumns);
      resp.setColumns(respColumns);

      // get column types and do deduplication
      List<String> columnsType = new ArrayList<>();
      columnsType.add(TSDataType.TEXT.toString()); // the DEVICE column of GROUP_BY_DEVICE result
      List<TSDataType> deduplicatedColumnsType = new ArrayList<>();
      deduplicatedColumnsType.add(TSDataType.TEXT); // the DEVICE column of GROUP_BY_DEVICE result
      List<String> deduplicatedMeasurementColumns = new ArrayList<>();
      Set<String> tmpColumnSet = new HashSet<>();
      Map<String, TSDataType> checker = ((QueryPlan) plan).getDataTypeConsistencyChecker();
      for (String column : measurementColumns) {
        TSDataType dataType = checker.get(column);
        columnsType.add(dataType.toString());

        if (!tmpColumnSet.contains(column)) {
          // Note that this deduplication strategy is consistent with that of client IoTDBQueryResultSet.
          tmpColumnSet.add(column);
          deduplicatedMeasurementColumns.add(column);
          deduplicatedColumnsType.add(dataType);
        }
      }

      // set dataTypeList in TSExecuteStatementResp. Note this is without deduplication.
      resp.setDataTypeList(columnsType);

      // save deduplicated measurementColumn names and types in QueryPlan for the next stage to use.
      // i.e., used by DeviceIterateDataSet constructor in `fetchResults` stage.
      ((QueryPlan) plan).setMeasurementColumnList(deduplicatedMeasurementColumns);
      ((QueryPlan) plan).setDataTypes(deduplicatedColumnsType);

      // set these null since they are never used henceforth in GROUP_BY_DEVICE query processing.
      ((QueryPlan) plan).setPaths(null);
      ((QueryPlan) plan).setDataTypeConsistencyChecker(null);

    } else {
      // Restore column header of aggregate to func(column_name), only
      // support single aggregate function for now
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

      resp.setColumns(respColumns);
      resp.setDataTypeList(queryColumnsType(respColumns));
    }
    return resp;
  }


  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin()) {
        return getTSFetchResultsResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }

      if (!queryId2Plan.get().containsKey(req.queryId)) {
        return getTSFetchResultsResp(
            getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed statement"));
      }

      QueryDataSet queryDataSet;
      if (!queryId2DataSet.get().containsKey(req.queryId)) {
        queryDataSet = createNewDataSet(req);
      } else {
        queryDataSet = queryId2DataSet.get().get(req.queryId);
      }

      IAuthorizer authorizer;
      try {
        authorizer = LocalFileAuthorizer.getInstance();
      } catch (AuthException e) {
        throw new TException(e);
      }
      TSQueryDataSet result;
      if (config.isEnableWatermark() && authorizer.isUserUseWaterMark(username.get())) {
        WatermarkEncoder encoder;
        if (config.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
          encoder = new GroupedLSBWatermarkEncoder(config);
        } else {
          throw new UnSupportedDataTypeException(String.format(
              "Watermark method is not supported yet: %s", config.getWatermarkMethodName()));
        }
        result = QueryDataSetUtils
            .convertQueryDataSetByFetchSize(queryDataSet, req.fetchSize, encoder);
      } else {
        result = QueryDataSetUtils.convertQueryDataSetByFetchSize(queryDataSet, req.fetchSize);
      }
      boolean hasResultSet = (result.bufferForTime().limit() != 0);
      if (!hasResultSet && queryId2DataSet.get() != null) {
        queryId2DataSet.get().remove(req.queryId);
      }

      TSFetchResultsResp resp = getTSFetchResultsResp(getStatus(TSStatusCode.SUCCESS_STATUS,
          "FetchResult successfully. Has more result: " + hasResultSet));
      resp.setHasResultSet(hasResultSet);
      resp.setQueryDataSet(result);
      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSFetchResultsResp(getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private QueryDataSet createNewDataSet(TSFetchResultsReq req)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException, IOException {
    PhysicalPlan physicalPlan = queryId2Plan.get().get(req.queryId);

    QueryDataSet queryDataSet;
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());

    initContextMap();
    contextMapLocal.get().put(req.queryId, context);

    queryDataSet = processor.getExecutor().processQuery(physicalPlan, context);

    queryId2DataSet.get().put(req.queryId, queryDataSet);
    return queryDataSet;
  }

  private void initContextMap() {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      contextMap = new HashMap<>();
      contextMapLocal.set(contextMap);
    }
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin()) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement);
    } catch (Exception e) {
      logger.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage()));
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan) {
    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    status = executePlan(plan);
    TSExecuteStatementResp resp = getTSExecuteStatementResp(status);
    long queryId = generateOperationId();
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()), queryId);
    TSOperationHandle operationHandle;
    operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private boolean executeNonQuery(PhysicalPlan plan) throws QueryProcessException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return processor.getExecutor().processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement) {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
    } catch (QueryProcessException | SQLParserException e) {
      logger.info("meet error while parsing SQL to physical plan: {}", e.getMessage());
      return getTSExecuteStatementResp(getStatus(TSStatusCode.SQL_PARSE_ERROR, e.getMessage()));
    }

    if (physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR,
          "Statement is a query statement."));
    }

    return executeUpdateStatement(physicalPlan);
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  private boolean checkLogin() {
    return username.get() != null;
  }

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan) throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username.get(), paths, plan.getOperatorType(), targetUser);
  }

  private TSExecuteStatementResp getTSExecuteStatementResp(TSStatus status) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()), generateOperationId());
    TSOperationHandle operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private TSExecuteBatchStatementResp getTSBatchExecuteStatementResp(TSStatus status,
      List<Integer> result) {
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

  void handleClientExit() {
    closeSession(null);
  }

  @Override
  public TSGetTimeZoneResp getTimeZone() {
    TSStatus tsStatus;
    TSGetTimeZoneResp resp;
    try {
      tsStatus = getStatus(TSStatusCode.SUCCESS_STATUS);
      resp = new TSGetTimeZoneResp(tsStatus, zoneIds.get().toString());
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
      zoneIds.set(ZoneId.of(timeZoneID));
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
    properties
        .setTimestampPrecision(IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    return properties;
  }

  @Override
  public TSExecuteStatementResp insert(TSInsertionReq req) {
    // TODO need to refactor this when implementing PreparedStatement
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getTSExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR));
    }

    long stmtId = req.getStmtId();
    InsertPlan plan = (InsertPlan) queryId2Plan.get()
        .computeIfAbsent(stmtId, k -> new InsertPlan());

    // the old parameter will be used if new parameter is not set
    if (req.isSetDeviceId()) {
      plan.setDeviceId(req.getDeviceId());
    }
    if (req.isSetTimestamp()) {
      plan.setTime(req.getTimestamp());
    }
    if (req.isSetMeasurements()) {
      plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
    }
    if (req.isSetValues()) {
      plan.setValues(req.getValues().toArray(new String[0]));
    }

    try {
      return executeUpdateStatement(plan);
    } catch (Exception e) {
      logger.info("meet error while executing an insertion into {}", req.getDeviceId(), e);
      return getTSExecuteStatementResp(
          getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage()));
    }
  }

  @Override
  public TSExecuteInsertRowInBatchResp insertRowInBatch(TSInsertInBatchReq req) {
    TSExecuteInsertRowInBatchResp resp = new TSExecuteInsertRowInBatchResp();
    if (!checkLogin()) {
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
      TSStatus status = checkAuthority(plan);
      if (status != null) {
        resp.addToStatusList(new TSStatus(status));
      } else {
        resp.addToStatusList(executePlan(plan));
      }
    }

    return resp;
  }

  @Override
  public TSExecuteBatchStatementResp testInsertBatch(TSBatchInsertionReq req) throws TException {
    logger.debug("Test insert batch request receive.");
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    resp.setStatus(getStatus(TSStatusCode.SUCCESS_STATUS));
    resp.setResult(Collections.emptyList());
    return resp;
  }

  @Override
  public TSStatus testInsertRow(TSInsertReq req) throws TException {
    logger.debug("Test insert row request receive.");
    return getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSExecuteInsertRowInBatchResp testInsertRowInBatch(TSInsertInBatchReq req)
      throws TException {
    logger.debug("Test insert row in batch request receive.");

    TSExecuteInsertRowInBatchResp resp = new TSExecuteInsertRowInBatchResp();
    resp.addToStatusList(getStatus(TSStatusCode.SUCCESS_STATUS));
    return resp;
  }


  @Override
  public TSStatus insertRow(TSInsertReq req) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    InsertPlan plan = new InsertPlan();
    plan.setDeviceId(req.getDeviceId());
    plan.setTime(req.getTimestamp());
    plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
    plan.setValues(req.getValues().toArray(new String[0]));

    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    if (!checkLogin()) {
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

    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSExecuteBatchStatementResp insertBatch(TSBatchInsertionReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin()) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.NOT_LOGIN_ERROR), null);
      }

      BatchInsertPlan batchInsertPlan = new BatchInsertPlan(req.deviceId, req.measurements);
      batchInsertPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      batchInsertPlan.setColumns(QueryDataSetUtils
          .readValuesFromBuffer(req.values, req.types, req.measurements.size(), req.size));
      batchInsertPlan.setRowCount(req.size);
      batchInsertPlan.setTimeBuffer(req.timestamps);
      batchInsertPlan.setValueBuffer(req.values);
      batchInsertPlan.setDataTypes(req.types);

      boolean isAllSuccessful = true;
      TSStatus status = checkAuthority(batchInsertPlan);
      if (status != null) {
        return new TSExecuteBatchStatementResp(status);
      }
      Integer[] results = processor.getExecutor().insertBatch(batchInsertPlan);

      for (Integer result : results) {
        if (result != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          isAllSuccessful = false;
          break;
        }
      }

      if (isAllSuccessful) {
        logger.debug("Insert one RowBatch successfully");
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.SUCCESS_STATUS),
            Arrays.asList(results));
      } else {
        logger.debug("Insert one RowBatch failed!");
        return getTSBatchExecuteStatementResp(getStatus(TSStatusCode.INTERNAL_SERVER_ERROR),
            Arrays.asList(results));
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
  public TSStatus setStorageGroup(String storageGroup) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    SetStorageGroupPlan plan = new SetStorageGroupPlan(new Path(storageGroup));
    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteStorageGroups(List<String> storageGroups) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    List<Path> storageGroupList = new ArrayList<>();
    for (String storageGroup : storageGroups) {
      storageGroupList.add(new Path(storageGroup));
    }
    DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan(new Path(req.getPath()),
        TSDataType.values()[req.getDataType()], TSEncoding.values()[req.getEncoding()],
        CompressionType.values()[req.compressor], new HashMap<>());
    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public TSStatus deleteTimeseries(List<String> paths) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }
    List<Path> pathList = new ArrayList<>();
    for (String path : paths) {
      pathList.add(new Path(path));
    }
    DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
    TSStatus status = checkAuthority(plan);
    if (status != null) {
      return new TSStatus(status);
    }
    return new TSStatus(executePlan(plan));
  }

  @Override
  public long requestStatementId() {
    long statementId = statementIdGenerator.get();
    statementIdGenerator.set(statementId + 1);
    return statementId;
  }

  private TSStatus checkAuthority(PhysicalPlan plan) {
    List<Path> paths = plan.getPaths();
    try {
      if (!checkAuthorization(paths, plan)) {
        return getStatus(TSStatusCode.NO_PERMISSION_ERROR, plan.getOperatorType().toString());
      }
    } catch (AuthException e) {
      logger.error("meet error while checking authorization.", e);
      return getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    }
    return null;
  }

  private TSStatus executePlan(PhysicalPlan plan) {
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (QueryProcessException e) {
      logger.debug("meet error while processing non-query. ", e);
      return new TSStatus(new TSStatusType(e.getErrorCode(), e.getMessage()));
    }

    return execRet ? getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
        : getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  private long generateOperationId() {
    long queryId = operationIdGenerator.get();
    operationIdGenerator.set(queryId + 1);
    return queryId;
  }
}

