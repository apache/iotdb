/**
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

import static org.apache.iotdb.db.conf.IoTDBConstant.PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.USER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.QueryInBatchStmtException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.qp.IllegalASTFormatException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.Metadata;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSHandleIdentifier;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSOperationHandle;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
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
  private static final String ERROR_NOT_LOGIN = "Not login";
  public static List<SqlArgument> sqlArgumentsList = new ArrayList<SqlArgument>();

  protected QueryProcessor processor;
  // Record the username for every rpc connection. Username.get() is null if
  // login is failed.
  protected ThreadLocal<String> username = new ThreadLocal<>();
  private ThreadLocal<HashMap<String, PhysicalPlan>> queryStatus = new ThreadLocal<>();
  private ThreadLocal<HashMap<String, QueryDataSet>> queryRet = new ThreadLocal<>();
  private ThreadLocal<ZoneId> zoneIds = new ThreadLocal<>();
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private ThreadLocal<Map<Long, QueryContext>> contextMapLocal = new ThreadLocal<>();

  private AtomicLong globalStmtId = new AtomicLong(0L);
  // (statementId) -> (statement)
  // TODO: remove unclosed statements
  private Map<Long, PhysicalPlan> idStmtMap = new ConcurrentHashMap<>();

  public TSServiceImpl() throws IOException {
    processor = new QueryProcessor(new QueryProcessExecutor());
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
      logger.error("meet error while logging in.", e);
      status = false;
    }
    TS_Status tsStatus;
    if (status) {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      tsStatus.setErrorMessage("login successfully.");
      username.set(req.getUsername());
      zoneIds.set(config.getZoneID());
      initForOneSession();
    } else {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("login failed. Username or password is wrong.");
    }
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus,
        TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1);
    resp.setSessionHandle(
        new TS_SessionHandle(new TSHandleIdentifier(ByteBuffer.wrap(req.getUsername().getBytes()),
            ByteBuffer.wrap(req.getPassword().getBytes()))));
    logger.info("{}: Login status: {}. User : {}", IoTDBConstant.GLOBAL_DB_NAME,
        tsStatus.getErrorMessage(),
        req.getUsername());

    return resp;
  }

  private void initForOneSession() {
    queryStatus.set(new HashMap<>());
    queryRet.set(new HashMap<>());
  }

  @Override
  public TSCloseSessionResp closeSession(TSCloseSessionReq req) {
    logger.info("{}: receive close session", IoTDBConstant.GLOBAL_DB_NAME);
    TS_Status tsStatus;
    if (username.get() == null) {
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage("Has not logged in");
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    } else {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      username.remove();
      if (zoneIds.get() != null) {
        zoneIds.remove();
      }
    }
    return new TSCloseSessionResp(tsStatus);
  }

  @Override
  public TSCancelOperationResp cancelOperation(TSCancelOperationReq req) {
    return new TSCancelOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSCloseOperationResp closeOperation(TSCloseOperationReq req) {
    logger.info("{}: receive close operation", IoTDBConstant.GLOBAL_DB_NAME);
    try {

      if (req != null && req.isSetStmtId()) {
        long stmtId = req.getStmtId();
        idStmtMap.remove(stmtId);
      }

      releaseQueryResource(req);

      clearAllStatusForCurrentRequest();
    } catch (Exception e) {
      logger.error("Error in closeOperation : ", e);
    }
    return new TSCloseOperationResp(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
  }

  private void releaseQueryResource(TSCloseOperationReq req) throws StorageEngineException {
    Map<Long, QueryContext> contextMap = contextMapLocal.get();
    if (contextMap == null) {
      return;
    }
    if (req == null || req.queryId == -1) {
      // end query for all the query tokens created by current thread
      for (QueryContext context : contextMap.values()) {
        QueryResourceManager.getInstance().endQueryForGivenJob(context.getJobId());
      }
      contextMapLocal.set(new HashMap<>());
    } else {
      QueryResourceManager.getInstance()
          .endQueryForGivenJob(contextMap.remove(req.queryId).getJobId());
    }
  }

  private void clearAllStatusForCurrentRequest() {
    if (this.queryRet.get() != null) {
      this.queryRet.get().clear();
    }
    if (this.queryStatus.get() != null) {
      this.queryStatus.get().clear();
    }
  }

  private TS_Status getErrorStatus(String message) {
    TS_Status status = new TS_Status(TS_StatusCode.ERROR_STATUS);
    status.setErrorMessage(message);
    return status;
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TS_Status status;
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      status = getErrorStatus(ERROR_NOT_LOGIN);
      return new TSFetchMetadataResp(status);
    }
    TSFetchMetadataResp resp = new TSFetchMetadataResp();
    try {
      switch (req.getType()) {
        case "SHOW_TIMESERIES":
          String path = req.getColumnPath();
          List<List<String>> showTimeseriesList = getTimeSeriesForPath(path);
          resp.setShowTimeseriesList(showTimeseriesList);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "SHOW_STORAGE_GROUP":
          Set<String> storageGroups = getAllStorageGroups();
          resp.setShowStorageGroups(storageGroups);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "METADATA_IN_JSON":
          String metadataInJson = getMetadataInString();
          resp.setMetadataInJson(metadataInJson);
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "DELTA_OBEJECT":
          Metadata metadata = getMetadata();
          String column = req.getColumnPath();
          Map<String, List<String>> deviceMap = metadata.getDeviceMap();
          if (deviceMap == null || !deviceMap.containsKey(column)) {
            resp.setColumnsList(new ArrayList<>());
          } else {
            resp.setColumnsList(deviceMap.get(column));
          }
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesType(req.getColumnPath()).toString());
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(getPaths(req.getColumnPath()));
          status = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
          break;
        default:
          status = new TS_Status(TS_StatusCode.ERROR_STATUS);
          status
              .setErrorMessage(
                  String.format("Unsupported fetch metadata operation %s", req.getType()));
          break;
      }
    } catch (PathErrorException | MetadataErrorException | OutOfMemoryError e) {
      logger
          .error(String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()),
              e);
      Thread.currentThread().interrupt();
      status = getErrorStatus(
          String.format("Failed to fetch metadata because: %s", e));
      resp.setStatus(status);
      return resp;
    }
    resp.setStatus(status);
    return resp;
  }

  private Set<String> getAllStorageGroups() throws PathErrorException {
    return MManager.getInstance().getAllStorageGroup();
  }

  private List<List<String>> getTimeSeriesForPath(String path)
      throws PathErrorException {
    return MManager.getInstance().getShowTimeseriesPath(path);
  }

  private String getMetadataInString() {
    return MManager.getInstance().getMetadataInString();
  }

  protected Metadata getMetadata()
      throws PathErrorException {
    return MManager.getInstance().getMetadata();
  }

  protected TSDataType getSeriesType(String path)
      throws PathErrorException {
    switch (path.toLowerCase()) {
      // authorization queries
      case "role":
      case "user":
      case "privilege":
        return TSDataType.TEXT;
      default:
        // do nothing
    }

    if (path.contains("(") && !path.startsWith("(") && path.endsWith(")")) {
      // aggregation
      int leftBracketIndex = path.indexOf('(');
      String aggrType = path.substring(0, leftBracketIndex);
      String innerPath = path.substring(leftBracketIndex + 1, path.length() - 1);
      switch (aggrType.toLowerCase()) {
        case StatisticConstant.MIN_TIME:
        case StatisticConstant.MAX_TIME:
        case StatisticConstant.COUNT:
          return TSDataType.INT64;
        case StatisticConstant.LAST:
        case StatisticConstant.FIRST:
        case StatisticConstant.MIN_VALUE:
        case StatisticConstant.MAX_VALUE:
          return getSeriesType(innerPath);
        case StatisticConstant.MEAN:
        case StatisticConstant.SUM:
          return TSDataType.DOUBLE;
        default:
          throw new PathErrorException(
              "aggregate does not support " + aggrType + " function.");
      }
    }
    return MManager.getInstance().getSeriesType(path);
  }

  protected List<String> getPaths(String path)
      throws MetadataErrorException {
    return MManager.getInstance().getPaths(path);
  }

  /**
   * Judge whether the statement is ADMIN COMMAND and if true, execute it.
   *
   * @param statement command
   * @return true if the statement is ADMIN COMMAND
   * @throws IOException exception
   */
  private boolean execAdminCommand(String statement) {
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
        // TODO change to merge!!!
        throw new UnsupportedOperationException("merge not implemented");
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
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN, null);
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
        return getTSBathExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS,
            "Execute batch statements successfully", result);
      } else {
        return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            batchErrorMessage.toString(),
            result);
      }
    } catch (Exception e) {
      logger.error("{}: error occurs when executing statements", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSBathExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage(), null);
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH, t1);
    }
  }

  // execute one statement of a batch. Currently, query is not allowed in a batch statement and
  // on finding queries in a batch, such query will be ignored and an error will be generated
  private boolean executeStatementInBatch(String statement, StringBuilder batchErrorMessage,
      List<Integer> result) {
    try {
      PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      if (physicalPlan.isQuery()) {
        throw new QueryInBatchStmtException("Query statement not allowed in batch: " + statement);
      }
      TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan);
      if (resp.getStatus().getStatusCode().equals(TS_StatusCode.SUCCESS_STATUS)) {
        result.add(Statement.SUCCESS_NO_INFO);
      } else {
        result.add(Statement.EXECUTE_FAILED);
        batchErrorMessage.append(resp.getStatus().getErrorMessage()).append("\n");
        return false;
      }
    } catch (Exception e) {
      String errMessage = String.format(
          "Fail to generate physcial plan and execute for statement "
              + "%s beacuse %s",
          statement, e.getMessage());
      logger.warn("Error occurred when executing {}", statement, e);
      result.add(Statement.EXECUTE_FAILED);
      batchErrorMessage.append(errMessage).append("\n");
      return false;
    }
    return true;
  }


  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
	long starttime = System.currentTimeMillis();
	TSExecuteStatementResp resp;
	SqlArgument sqlArgument;
    try {
      if (!checkLogin()) {
        logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }
      String statement = req.getStatement();

      if (execAdminCommand(statement)) {
        return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "ADMIN_COMMAND_SUCCESS");
      }

      if (execShowFlushInfo(statement)) {
        String msg = String.format(
            "There are %d flush tasks, %d flush tasks are in execution and %d flush tasks are waiting for execution.",
            FlushTaskPoolManager.getInstance().getTotalTasks(),
            FlushTaskPoolManager.getInstance().getWorkingTasksNumber(),
            FlushTaskPoolManager.getInstance().getWaitingTasksNumber());
        return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_WITH_INFO_STATUS, msg);
      }

      if (execShowDynamicParameters(statement)) {
        String msg = String.format(
            "Memtable size threshold: %dB, Memtable number: %d, Tsfile size threshold: %dB, Compression ratio: %f,"
                + " Storage group number: %d, Timeseries number: %d, Maximal timeseries number among storage groups: %d",
            IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold(),
            IoTDBDescriptor.getInstance().getConfig().getMaxMemtableNumber(),
            IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold(),
            CompressionRatio.getInstance().getRatio(),
            MManager.getInstance().getAllStorageGroup().size(),
            IoTDBConfigDynamicAdapter.getInstance().getTotalTimeseries(),
            MManager.getInstance().getMaximalSeriesNumberAmongStorageGroups());
        return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_WITH_INFO_STATUS, msg);
      }

      if (execSetConsistencyLevel(statement)) {
        return getTSExecuteStatementResp(TS_StatusCode.SUCCESS_WITH_INFO_STATUS,
            "Execute set consistency level successfully");
      }

      PhysicalPlan physicalPlan;
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
      if (physicalPlan.isQuery()) {
      	resp = executeQueryStatement(statement, physicalPlan);
      	long endtime = System.currentTimeMillis();
      	sqlArgument = new SqlArgument(resp,physicalPlan,statement,starttime,endtime);
      	sqlArgument.setStatement(statement);
      	sqlArgumentsList.add(sqlArgument);
      	if(sqlArgumentsList.size()>200) {
  			for (int i = 0; i < 50; i++) {
  				sqlArgumentsList.remove(sqlArgumentsList.size()-1);
  			}
      	}
        return resp;
      } else {
        return executeUpdateStatement(physicalPlan);
      }
    } catch (IllegalASTFormatException e) {
      logger.debug("meet error while parsing SQL to physical plan: ", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Statement format is not right:" + e.getMessage());
    } catch (Exception e) {
      logger.info("meet error while executing statement: {}", req.getStatement(), e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  /**
   * Show flush info
   */
  private boolean execShowFlushInfo(String statement) {
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase().trim();
    if (Pattern.matches(IoTDBConstant.SHOW_FLUSH_TASK_INFO, statement)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Show dynamic parameters
   */
  private boolean execShowDynamicParameters(String statement) {
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase().trim();
    if (Pattern.matches(IoTDBConstant.SHOW_DYNAMIC_PARAMETERS, statement)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set consistency level
   */
  private boolean execSetConsistencyLevel(String statement) throws SQLException {
    if (statement == null) {
      return false;
    }
    statement = statement.toLowerCase().trim();
    if (Pattern.matches(IoTDBConstant.SET_READ_CONSISTENCY_LEVEL_PATTERN, statement)) {
      throw new SQLException(
          "IoTDB Stand-alone version does not support setting read-insert consistency level");
    } else {
      return false;
    }
  }

  private TSExecuteStatementResp executeQueryStatement(String statement, PhysicalPlan plan) {
    long t1 = System.currentTimeMillis();
    try {
      TSExecuteStatementResp resp;
      List<String> columns = new ArrayList<>();
      if (!(plan instanceof AuthorPlan)) {
        resp = executeDataQuery(plan, columns);
      } else {
        resp = executeAuthQuery(plan, columns);
      }
      resp.setColumns(columns);
      resp.setDataTypeList(queryColumnsType(columns));
      resp.setOperationType(plan.getOperatorType().toString());
      TSHandleIdentifier operationId = new TSHandleIdentifier(
          ByteBuffer.wrap(username.get().getBytes()), ByteBuffer.wrap("PASS".getBytes()));
      TSOperationHandle operationHandle = new TSOperationHandle(operationId, true);
      resp.setOperationHandle(operationHandle);

      recordANewQuery(statement, plan);
      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, t1);
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
    }

    String statement = req.getStatement();
    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
    } catch (QueryProcessorException | ArgsErrorException | MetadataErrorException e) {
      logger.error("meet error while parsing SQL to physical plan!", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    if (!physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Statement is not a query statement.");
    }
    return executeQueryStatement(statement, physicalPlan);
  }

  private List<String> queryColumnsType(List<String> columns) throws PathErrorException {
    List<String> columnTypes = new ArrayList<>();
    for (String column : columns) {
      columnTypes.add(getSeriesType(column).toString());
    }
    return columnTypes;
  }

  private TSExecuteStatementResp executeAuthQuery(PhysicalPlan plan, List<String> columns) {
    TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
    resp.setIgnoreTimeStamp(true);
    AuthorPlan authorPlan = (AuthorPlan) plan;
    switch (authorPlan.getAuthorType()) {
      case LIST_ROLE:
        columns.add(ROLE);
        break;
      case LIST_USER:
        columns.add(USER);
        break;
      case LIST_ROLE_USERS:
        columns.add(USER);
        break;
      case LIST_USER_ROLES:
        columns.add(ROLE);
        break;
      case LIST_ROLE_PRIVILEGE:
        columns.add(PRIVILEGE);
        break;
      case LIST_USER_PRIVILEGE:
        columns.add(ROLE);
        columns.add(PRIVILEGE);
        break;
      default:
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, String.format("%s is not an "
            + "auth query", authorPlan.getAuthorType()));
    }
    return resp;
  }

  private TSExecuteStatementResp executeDataQuery(PhysicalPlan plan, List<String> columns)
      throws AuthException, TException {
    List<Path> paths = plan.getPaths();

    // check seriesPath exists
    if (paths.isEmpty()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, "Timeseries does not exist.");
    }

    // check file level set
    try {
      checkFileLevelSet(paths);
    } catch (PathErrorException e) {
      logger.error("meet error while checking file level.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    // check permissions
    if (!checkAuthorization(paths, plan)) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "No permissions for this query.");
    }

    TSExecuteStatementResp resp = getTSExecuteStatementResp(TS_StatusCode.SUCCESS_STATUS, "");
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (Path p : paths) {
          columns.add(p.getFullPath());
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
          columns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
        }
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }
    return resp;
  }

  private void checkFileLevelSet(List<Path> paths) throws PathErrorException {
    MManager.getInstance().checkFileLevel(paths);
  }

  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin()) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Not login.");
      }

      String statement = req.getStatement();
      if (!queryStatus.get().containsKey(statement)) {
        return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, "Has not executed statement");
      }

      QueryDataSet queryDataSet;
      if (!queryRet.get().containsKey(statement)) {
        queryDataSet = createNewDataSet(statement, req);
      } else {
        queryDataSet = queryRet.get().get(statement);
      }

      int fetchSize = req.getFetch_size();
      TSQueryDataSet result = QueryDataSetUtils
          .convertQueryDataSetByFetchSize(queryDataSet, fetchSize);

      boolean hasResultSet = !result.getRecords().isEmpty();
      if (!hasResultSet && queryRet.get() != null) {
        queryRet.get().remove(statement);
      }

      TSFetchResultsResp resp = getTSFetchResultsResp(TS_StatusCode.SUCCESS_STATUS,
          "FetchResult successfully. Has more result: " + hasResultSet);
      resp.setHasResultSet(hasResultSet);
      resp.setQueryDataSet(result);
      return resp;
    } catch (Exception e) {
      logger.error("{}: Internal server error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSFetchResultsResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private QueryDataSet createNewDataSet(String statement, TSFetchResultsReq req)
      throws PathErrorException, QueryFilterOptimizationException, StorageEngineException,
      ProcessorException, IOException {
    PhysicalPlan physicalPlan = queryStatus.get().get(statement);

    QueryDataSet queryDataSet;
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignJobId());

    initContextMap();
    contextMapLocal.get().put(req.queryId, context);

    queryDataSet = processor.getExecutor().processQuery(physicalPlan,
        context);

    queryRet.get().put(statement, queryDataSet);
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
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
      }
      String statement = req.getStatement();
      return executeUpdateStatement(statement);
    } catch (Exception e) {
      logger.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan) {
    List<Path> paths = plan.getPaths();
    try {
      if (!checkAuthorization(paths, plan)) {
        return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      logger.error("meet error while checking authorization.", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Uninitialized authorizer " + e.getMessage());
    }
    // TODO
    // In current version, we only return OK/ERROR
    // Do we need to add extra information of executive condition
    boolean execRet;
    try {
      execRet = executeNonQuery(plan);
    } catch (ProcessorException e) {
      logger.debug("meet error while processing non-query. ", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    TS_StatusCode statusCode = execRet ? TS_StatusCode.SUCCESS_STATUS : TS_StatusCode.ERROR_STATUS;
    String msg = execRet ? "Execute successfully" : "Execute statement error.";
    TSExecuteStatementResp resp = getTSExecuteStatementResp(statusCode, msg);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle;
    operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private boolean executeNonQuery(PhysicalPlan plan) throws ProcessorException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new ProcessorException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return processor.getExecutor().processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement) {

    PhysicalPlan physicalPlan;
    try {
      physicalPlan = processor.parseSQLToPhysicalPlan(statement, zoneIds.get());
    } catch (QueryProcessorException | ArgsErrorException | MetadataErrorException e) {
      logger.error("meet error while parsing SQL to physical plan!", e);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }

    if (physicalPlan.isQuery()) {
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS,
          "Statement is a query statement.");
    }

    return executeUpdateStatement(physicalPlan);
  }

  private void recordANewQuery(String statement, PhysicalPlan physicalPlan) {
    queryStatus.get().put(statement, physicalPlan);
    // refresh current queryRet for statement
    queryRet.get().remove(statement);
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

  private TSExecuteStatementResp getTSExecuteStatementResp(TS_StatusCode code, String msg) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    TSHandleIdentifier operationId = new TSHandleIdentifier(
        ByteBuffer.wrap(username.get().getBytes()),
        ByteBuffer.wrap("PASS".getBytes()));
    TSOperationHandle operationHandle = new TSOperationHandle(operationId, false);
    resp.setOperationHandle(operationHandle);
    return resp;
  }

  private TSExecuteBatchStatementResp getTSBathExecuteStatementResp(TS_StatusCode code,
      String msg,
      List<Integer> result) {
    TSExecuteBatchStatementResp resp = new TSExecuteBatchStatementResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    resp.setResult(result);
    return resp;
  }

  private TSFetchResultsResp getTSFetchResultsResp(TS_StatusCode code, String msg) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TS_Status tsStatus = new TS_Status(code);
    tsStatus.setErrorMessage(msg);
    resp.setStatus(tsStatus);
    return resp;
  }

  void handleClientExit() throws TException {
    closeOperation(null);
    closeSession(null);
  }

  @Override
  public TSGetTimeZoneResp getTimeZone() {
    TS_Status tsStatus;
    TSGetTimeZoneResp resp;
    try {
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
      resp = new TSGetTimeZoneResp(tsStatus, zoneIds.get().toString());
    } catch (Exception e) {
      logger.error("meet error while generating time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
      resp = new TSGetTimeZoneResp(tsStatus, "Unknown time zone");
    }
    return resp;
  }

  @Override
  public TSSetTimeZoneResp setTimeZone(TSSetTimeZoneReq req) {
    TS_Status tsStatus;
    try {
      String timeZoneID = req.getTimeZone();
      zoneIds.set(ZoneId.of(timeZoneID));
      tsStatus = new TS_Status(TS_StatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      logger.error("meet error while setting time zone.", e);
      tsStatus = new TS_Status(TS_StatusCode.ERROR_STATUS);
      tsStatus.setErrorMessage(e.getMessage());
    }
    return new TSSetTimeZoneResp(tsStatus);
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
  public TSExecuteStatementResp executeInsertion(TSInsertionReq req) {
    if (!checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, ERROR_NOT_LOGIN);
    }

    long stmtId = req.getStmtId();
    InsertPlan plan = (InsertPlan) idStmtMap.computeIfAbsent(stmtId, k -> new InsertPlan());

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
      return getTSExecuteStatementResp(TS_StatusCode.ERROR_STATUS, e.getMessage());
    }
  }

  @Override
  public long requestStatementId() {
    return globalStmtId.incrementAndGet();
  }
}

