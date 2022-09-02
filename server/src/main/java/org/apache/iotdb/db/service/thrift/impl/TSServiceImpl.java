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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.engine.selectinto.InsertTabletPlansIterator;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.template.TemplateQueryType;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SelectIntoPlan;
import org.apache.iotdb.db.qp.physical.crud.UDFPlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.tracing.TracingConstant;
import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.DirectNonAlignDataSet;
import org.apache.iotdb.db.query.pool.QueryTaskManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.StaticResps;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.metrics.enums.Operation;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RedirectException;
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
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.service.basic.ServiceProvider.AUDIT_LOGGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CONFIG;
import static org.apache.iotdb.db.service.basic.ServiceProvider.CURRENT_RPC_VERSION;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_FREQUENCY_RECORDER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.QUERY_TIME_MANAGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.SLOW_SQL_LOGGER;
import static org.apache.iotdb.db.service.basic.ServiceProvider.TRACING_MANAGER;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onIoTDBException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNonQueryException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

/** Thrift RPC implementation at server side. */
public class TSServiceImpl implements IClientRPCServiceWithHandler {

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private class QueryTask implements Callable<TSExecuteStatementResp> {

    private final PhysicalPlan plan;
    private final long queryStartTime;
    private final long sessionId;
    private final String statement;
    private final long statementId;
    private final long timeout;
    private final int fetchSize;
    private final boolean isJdbcQuery;
    private final boolean enableRedirectQuery;

    /**
     * Execute query statement, return TSExecuteStatementResp with dataset.
     *
     * @param plan must be a plan for Query: QueryPlan, ShowPlan, and some AuthorPlan
     */
    public QueryTask(
        PhysicalPlan plan,
        long queryStartTime,
        long sessionId,
        String statement,
        long statementId,
        long timeout,
        int fetchSize,
        boolean isJdbcQuery,
        boolean enableRedirectQuery) {
      this.plan = plan;
      this.queryStartTime = queryStartTime;
      this.sessionId = sessionId;
      this.statement = statement;
      this.statementId = statementId;
      this.timeout = timeout;
      this.fetchSize = fetchSize;
      this.isJdbcQuery = isJdbcQuery;
      this.enableRedirectQuery = enableRedirectQuery;
    }

    @Override
    public TSExecuteStatementResp call() throws Exception {
      String username = SESSION_MANAGER.getUsername(sessionId);
      plan.setLoginUserName(username);

      QUERY_FREQUENCY_RECORDER.incrementAndGet();
      AUDIT_LOGGER.debug("Session {} execute Query: {}", sessionId, statement);

      final long queryId = SESSION_MANAGER.requestQueryId(statementId, true);
      QueryContext context =
          serviceProvider.genQueryContext(
              queryId, plan.isDebug(), queryStartTime, statement, timeout);

      TSExecuteStatementResp resp;
      try {
        if (plan instanceof QueryPlan) {
          QueryPlan queryPlan = (QueryPlan) plan;
          queryPlan.setEnableRedirect(enableRedirectQuery);
          resp = executeQueryPlan(queryPlan, context, isJdbcQuery, fetchSize, username);
        } else {
          resp = executeShowOrAuthorPlan(plan, context, fetchSize, username);
        }
        resp.setQueryId(queryId);
        resp.setOperationType(plan.getOperatorType().toString());
      } catch (Exception e) {
        SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
        throw e;
      } finally {
        addOperationLatency(Operation.EXECUTE_QUERY, queryStartTime);
        long costTime = System.currentTimeMillis() - queryStartTime;
        if (costTime >= CONFIG.getSlowQueryThreshold()) {
          SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
        }
      }
      return resp;
    }
  }

  private class FetchResultsTask implements Callable<TSFetchResultsResp> {

    private final long sessionId;
    private final long queryId;
    private final int fetchSize;
    private final boolean isAlign;

    public FetchResultsTask(long sessionId, long queryId, int fetchSize, boolean isAlign) {
      this.sessionId = sessionId;
      this.queryId = queryId;
      this.fetchSize = fetchSize;
      this.isAlign = isAlign;
    }

    @Override
    public TSFetchResultsResp call() throws Exception {
      QueryDataSet queryDataSet = SESSION_MANAGER.getDataset(queryId);
      TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
      try {
        if (isAlign) {
          TSQueryDataSet result =
              fillRpcReturnData(fetchSize, queryDataSet, SESSION_MANAGER.getUsername(sessionId));
          boolean hasResultSet = result.bufferForTime().limit() != 0;
          if (!hasResultSet) {
            SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          resp.setHasResultSet(hasResultSet);
          resp.setQueryDataSet(result);
          resp.setIsAlign(true);
        } else {
          TSQueryNonAlignDataSet nonAlignResult =
              fillRpcNonAlignReturnData(
                  fetchSize, queryDataSet, SESSION_MANAGER.getUsername(sessionId));
          boolean hasResultSet = false;
          for (ByteBuffer timeBuffer : nonAlignResult.getTimeList()) {
            if (timeBuffer.limit() != 0) {
              hasResultSet = true;
              break;
            }
          }
          if (!hasResultSet) {
            SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
          }
          resp.setHasResultSet(hasResultSet);
          resp.setNonAlignQueryDataSet(nonAlignResult);
          resp.setIsAlign(false);
        }
        QUERY_TIME_MANAGER.unRegisterQuery(queryId, false);
        return resp;
      } catch (Exception e) {
        SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
        throw e;
      }
    }
  }

  // main logger
  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);

  private static final String INFO_INTERRUPT_ERROR =
      "Current Thread interrupted when dealing with request {}";

  protected final ServiceProvider serviceProvider;

  public TSServiceImpl() {
    super();
    serviceProvider = IoTDB.serviceProvider;
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    IoTDBConstant.ClientVersion clientVersion = parseClientVersion(req);
    BasicOpenSessionResp openSessionResp =
        SESSION_MANAGER.openSession(
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
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSFetchMetadataResp resp = new TSFetchMetadataResp();

    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return resp.setStatus(getNotLoggedInStatus());
    }

    TSStatus status;
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          resp.setMetadataInJson(IoTDB.schemaProcessor.getMetadataInString());
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesTypeByPath(new PartialPath(req.getColumnPath())).toString());
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(
              getPaths(new PartialPath(req.getColumnPath())).stream()
                  .map(PartialPath::getFullPath)
                  .collect(Collectors.toList()));
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        default:
          status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, req.getType());
          break;
      }
    } catch (MetadataException e) {
      LOGGER.error(
          String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()), e);
      status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    } catch (Exception e) {
      status =
          onNPEOrUnexpectedException(
              e, OperationType.FETCH_METADATA, TSStatusCode.INTERNAL_SERVER_ERROR);
    }
    return resp.setStatus(status);
  }

  protected List<MeasurementPath> getPaths(PartialPath path) throws MetadataException {
    return IoTDB.schemaProcessor.getMeasurementPaths(path);
  }

  protected TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return IoTDB.schemaProcessor.getSeriesType(path);
  }

  private boolean executeInsertRowsPlan(InsertRowsPlan insertRowsPlan, List<TSStatus> result) {
    long t1 = System.currentTimeMillis();
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);
    addOperationLatency(Operation.EXECUTE_ROWS_PLAN_IN_BATCH, t1);
    int startIndex = result.size();
    if (startIndex > 0) {
      startIndex = startIndex - 1;
    }
    for (int i = 0; i < insertRowsPlan.getRowCount(); i++) {
      result.add(RpcUtils.SUCCESS_STATUS);
    }
    if (tsStatus.subStatus != null) {
      for (Entry<Integer, TSStatus> entry : insertRowsPlan.getResults().entrySet()) {
        result.set(startIndex + entry.getKey(), entry.getValue());
      }
    }
    return tsStatus.getCode() == RpcUtils.SUCCESS_STATUS.getCode();
  }

  private boolean executeMultiTimeSeriesPlan(
      CreateMultiTimeSeriesPlan multiPlan, List<TSStatus> result) {
    long t1 = System.currentTimeMillis();
    TSStatus tsStatus = executeNonQueryPlan(multiPlan);
    addOperationLatency(Operation.EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH, t1);

    int startIndex = result.size();
    if (startIndex > 0) {
      startIndex = startIndex - 1;
    }
    for (int k = 0; k < multiPlan.getPaths().size(); k++) {
      result.add(RpcUtils.SUCCESS_STATUS);
    }
    if (tsStatus.subStatus != null) {
      for (Entry<Integer, TSStatus> entry : multiPlan.getResults().entrySet()) {
        result.set(startIndex + entry.getKey(), entry.getValue());
      }
    }
    return tsStatus.getCode() == RpcUtils.SUCCESS_STATUS.getCode();
  }

  private void initMultiTimeSeriesPlan(CreateMultiTimeSeriesPlan multiPlan) {
    if (multiPlan.getPaths() == null) {
      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> tsDataTypes = new ArrayList<>();
      List<TSEncoding> tsEncodings = new ArrayList<>();
      List<CompressionType> tsCompressionTypes = new ArrayList<>();
      List<Map<String, String>> tagsList = new ArrayList<>();
      List<Map<String, String>> attributesList = new ArrayList<>();
      List<String> aliasList = new ArrayList<>();
      multiPlan.setPaths(paths);
      multiPlan.setDataTypes(tsDataTypes);
      multiPlan.setEncodings(tsEncodings);
      multiPlan.setCompressors(tsCompressionTypes);
      multiPlan.setTags(tagsList);
      multiPlan.setAttributes(attributesList);
      multiPlan.setAlias(aliasList);
    }
  }

  private void setMultiTimeSeriesPlan(
      CreateMultiTimeSeriesPlan multiPlan, CreateTimeSeriesPlan createTimeSeriesPlan) {
    PartialPath path = createTimeSeriesPlan.getPath();
    TSDataType type = createTimeSeriesPlan.getDataType();
    TSEncoding encoding = createTimeSeriesPlan.getEncoding();
    CompressionType compressor = createTimeSeriesPlan.getCompressor();
    Map<String, String> tags = createTimeSeriesPlan.getTags();
    Map<String, String> attributes = createTimeSeriesPlan.getAttributes();
    String alias = createTimeSeriesPlan.getAlias();

    multiPlan.getPaths().add(path);
    multiPlan.getDataTypes().add(type);
    multiPlan.getEncodings().add(encoding);
    multiPlan.getCompressors().add(compressor);
    multiPlan.getTags().add(tags);
    multiPlan.getAttributes().add(attributes);
    multiPlan.getAlias().add(alias);
  }

  private boolean executeBatchList(List executeList, List<TSStatus> result) {
    boolean isAllSuccessful = true;
    for (int j = 0; j < executeList.size(); j++) {
      Object planObject = executeList.get(j);
      if (InsertRowsPlan.class.isInstance(planObject)) {
        if (!executeInsertRowsPlan((InsertRowsPlan) planObject, result)) {
          isAllSuccessful = false;
        }
      } else if (CreateMultiTimeSeriesPlan.class.isInstance(planObject)) {
        if (!executeMultiTimeSeriesPlan((CreateMultiTimeSeriesPlan) planObject, result)) {
          isAllSuccessful = false;
        }
      }
    }
    return isAllSuccessful;
  }

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<TSStatus> result = new ArrayList<>();
    boolean isAllSuccessful = true;
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    InsertRowsPlan insertRowsPlan;
    int index = 0;
    List<Object> executeList = new ArrayList<>();
    OperatorType lastOperatorType = null;
    CreateMultiTimeSeriesPlan multiPlan;
    for (int i = 0; i < req.getStatements().size(); i++) {
      String statement = req.getStatements().get(i);
      try {
        PhysicalPlan physicalPlan =
            serviceProvider
                .getPlanner()
                .parseSQLToPhysicalPlan(
                    statement,
                    SESSION_MANAGER.getZoneId(req.sessionId),
                    SESSION_MANAGER.getClientVersion(req.sessionId));
        if (physicalPlan.isQuery() || physicalPlan.isSelectInto()) {
          throw new QueryInBatchStatementException(statement);
        }

        if (physicalPlan.getOperatorType().equals(OperatorType.INSERT)) {
          if (OperatorType.INSERT == lastOperatorType) {
            insertRowsPlan = (InsertRowsPlan) executeList.get(executeList.size() - 1);
          } else {
            insertRowsPlan = new InsertRowsPlan();
            executeList.add(insertRowsPlan);
            index = 0;
          }

          TSStatus status = SESSION_MANAGER.checkAuthority(physicalPlan, req.getSessionId());
          if (status != null) {
            insertRowsPlan.getResults().put(index, status);
            isAllSuccessful = false;
          }

          lastOperatorType = OperatorType.INSERT;
          insertRowsPlan.addOneInsertRowPlan((InsertRowPlan) physicalPlan, index);
          index++;

          if (i == req.getStatements().size() - 1) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
          }
        } else if (physicalPlan.getOperatorType().equals(OperatorType.CREATE_TIMESERIES)) {
          if (OperatorType.CREATE_TIMESERIES == lastOperatorType) {
            multiPlan = (CreateMultiTimeSeriesPlan) executeList.get(executeList.size() - 1);
          } else {
            multiPlan = new CreateMultiTimeSeriesPlan();
            executeList.add(multiPlan);
          }
          TSStatus status = SESSION_MANAGER.checkAuthority(physicalPlan, req.getSessionId());
          if (status != null) {
            multiPlan.getResults().put(i, status);
            isAllSuccessful = false;
          }

          lastOperatorType = OperatorType.CREATE_TIMESERIES;
          initMultiTimeSeriesPlan(multiPlan);

          CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) physicalPlan;
          setMultiTimeSeriesPlan(multiPlan, createTimeSeriesPlan);
          if (i == req.getStatements().size() - 1) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
          }
        } else {
          lastOperatorType = physicalPlan.getOperatorType();
          if (!executeList.isEmpty()) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
            executeList.clear();
          }
          long t2 = System.currentTimeMillis();
          TSExecuteStatementResp resp = executeNonQueryStatement(physicalPlan, req.getSessionId());
          addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
          result.add(resp.status);
          if (resp.getStatus().code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            isAllSuccessful = false;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error occurred when executing executeBatchStatement: ", e);
        TSStatus status =
            onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_BATCH_STATEMENT);
        if (status.getCode() != TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()) {
          isAllSuccessful = false;
        }
        result.add(status);
      }
    }
    addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    return isAllSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully")
        : RpcUtils.getStatus(result);
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    String statement = req.getStatement();
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  statement,
                  SESSION_MANAGER.getZoneId(req.getSessionId()),
                  SESSION_MANAGER.getClientVersion(req.sessionId));

      if (physicalPlan.isQuery()) {
        return submitQueryTask(physicalPlan, startTime, req);
      } else {
        return executeUpdateStatement(
            statement,
            req.statementId,
            physicalPlan,
            req.fetchSize,
            req.timeout,
            req.getSessionId());
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_STATEMENT));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "\"" + statement + "\". " + OperationType.EXECUTE_STATEMENT));
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      String statement = req.getStatement();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  statement,
                  SESSION_MANAGER.getZoneId(req.sessionId),
                  SESSION_MANAGER.getClientVersion(req.sessionId));

      if (physicalPlan.isQuery()) {
        return submitQueryTask(physicalPlan, startTime, req);
      } else {
        return RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(
              e, "\"" + req.getStatement() + "\". " + OperationType.EXECUTE_QUERY_STATEMENT));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(
              e, "\"" + req.getStatement() + "\". " + OperationType.EXECUTE_QUERY_STATEMENT));
    }
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .rawDataQueryReqToPhysicalPlan(
                  req,
                  SESSION_MANAGER.getZoneId(req.sessionId),
                  SESSION_MANAGER.getClientVersion(req.sessionId));

      if (physicalPlan.isQuery()) {
        Future<TSExecuteStatementResp> resp =
            QueryTaskManager.getInstance()
                .submit(
                    new QueryTask(
                        physicalPlan,
                        startTime,
                        req.sessionId,
                        "",
                        req.statementId,
                        CONFIG.getQueryTimeoutThreshold(),
                        req.fetchSize,
                        req.isJdbcQuery(),
                        req.enableRedirectQuery));
        return resp.get();
      } else {
        return RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, OperationType.EXECUTE_RAW_DATA_QUERY));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, OperationType.EXECUTE_RAW_DATA_QUERY));
    }
  }

  @Override
  public TSExecuteStatementResp executeLastDataQuery(TSLastDataQueryReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .lastDataQueryReqToPhysicalPlan(
                  req,
                  SESSION_MANAGER.getZoneId(req.sessionId),
                  SESSION_MANAGER.getClientVersion(req.sessionId));

      if (physicalPlan.isQuery()) {
        Future<TSExecuteStatementResp> resp =
            QueryTaskManager.getInstance()
                .submit(
                    new QueryTask(
                        physicalPlan,
                        startTime,
                        req.sessionId,
                        "",
                        req.statementId,
                        CONFIG.getQueryTimeoutThreshold(),
                        req.fetchSize,
                        req.isJdbcQuery(),
                        req.enableRedirectQuery));
        return resp.get();
      } else {
        return RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, OperationType.EXECUTE_LAST_DATA_QUERY));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, OperationType.EXECUTE_LAST_DATA_QUERY));
    }
  }

  private TSExecuteStatementResp submitQueryTask(
      PhysicalPlan physicalPlan, long startTime, TSExecuteStatementReq req) throws Exception {
    TSStatus status = SESSION_MANAGER.checkAuthority(physicalPlan, req.getSessionId());
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }
    QueryTask queryTask =
        new QueryTask(
            physicalPlan,
            startTime,
            req.sessionId,
            req.statement,
            req.statementId,
            req.timeout,
            req.fetchSize,
            req.jdbcQuery,
            req.enableRedirectQuery);
    TSExecuteStatementResp resp;
    if (physicalPlan instanceof ShowQueryProcesslistPlan) {
      resp = queryTask.call();
    } else {
      resp = QueryTaskManager.getInstance().submit(queryTask).get();
    }
    return resp;
  }

  private TSExecuteStatementResp executeQueryPlan(
      QueryPlan plan, QueryContext context, boolean isJdbcQuery, int fetchSize, String username)
      throws TException, MetadataException, QueryProcessException, StorageEngineException,
          SQLException, IOException, InterruptedException, QueryFilterOptimizationException,
          AuthException {
    // check permissions
    List<? extends PartialPath> authPaths = plan.getAuthPaths();
    if (authPaths != null
        && !authPaths.isEmpty()
        && !SESSION_MANAGER.checkAuthorization(plan, username)) {
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(
              TSStatusCode.NO_PERMISSION_ERROR,
              "No permissions for this operation, please add privilege "
                  + OperatorType.values()[
                      AuthorityChecker.translateToPermissionId(plan.getOperatorType())]));
    }

    long queryId = context.getQueryId();
    if (plan.isEnableTracing()) {
      context.setEnableTracing(true);
      TRACING_MANAGER.setStartTime(queryId, context.getStartTime(), context.getStatement());
      TRACING_MANAGER.registerActivity(
          queryId, TracingConstant.ACTIVITY_PARSE_SQL, System.currentTimeMillis());
      TRACING_MANAGER.setSeriesPathNum(queryId, plan.getPaths().size());
    }

    TSExecuteStatementResp resp = null;
    // execute it before createDataSet since it may change the content of query plan
    if (!(plan instanceof UDFPlan)) {
      resp = plan.getTSExecuteStatementResp(isJdbcQuery);
    }
    // create and cache dataset
    QueryDataSet newDataSet = serviceProvider.createQueryDataSet(context, plan, fetchSize);

    if (plan.isEnableTracing()) {
      TRACING_MANAGER.registerActivity(
          queryId, TracingConstant.ACTIVITY_CREATE_DATASET, System.currentTimeMillis());
    }

    if (newDataSet.getEndPoint() != null && plan.isEnableRedirect()) {
      QueryDataSet.EndPoint endPoint = newDataSet.getEndPoint();
      return redirectQueryToAnotherNode(resp, context, endPoint.getIp(), endPoint.getPort());
    }

    if (plan instanceof UDFPlan || plan.isGroupByLevel()) {
      resp = plan.getTSExecuteStatementResp(isJdbcQuery);
    }

    if (newDataSet instanceof DirectNonAlignDataSet) {
      resp.setNonAlignQueryDataSet(fillRpcNonAlignReturnData(fetchSize, newDataSet, username));
    } else {
      try {
        TSQueryDataSet tsQueryDataSet = fillRpcReturnData(fetchSize, newDataSet, username);
        resp.setQueryDataSet(tsQueryDataSet);
      } catch (RedirectException e) {
        if (plan.isEnableRedirect()) {
          TEndPoint endPoint = e.getEndPoint();
          return redirectQueryToAnotherNode(resp, context, endPoint.ip, endPoint.port);
        } else {
          LOGGER.error(
              "execute {} error, if session does not support redirect, should not throw redirection exception.",
              context.getStatement(),
              e);
        }
      }
    }
    QUERY_TIME_MANAGER.unRegisterQuery(context.getQueryId(), false);

    if (plan.isEnableTracing()) {
      TRACING_MANAGER.registerActivity(
          queryId, TracingConstant.ACTIVITY_REQUEST_COMPLETE, System.currentTimeMillis());
      TSTracingInfo tsTracingInfo = fillRpcReturnTracingInfo(queryId);
      resp.setTracingInfo(tsTracingInfo);
    }
    return resp;
  }

  private TSExecuteStatementResp executeShowOrAuthorPlan(
      PhysicalPlan plan, QueryContext context, int fetchSize, String username)
      throws QueryProcessException, TException, StorageEngineException, SQLException, IOException,
          InterruptedException, QueryFilterOptimizationException, MetadataException, AuthException {
    // create and cache dataset
    QueryDataSet newDataSet = serviceProvider.createQueryDataSet(context, plan, fetchSize);
    TSExecuteStatementResp resp = getListDataSetResp(plan, newDataSet);

    resp.setQueryDataSet(fillRpcReturnData(fetchSize, newDataSet, username));
    QUERY_TIME_MANAGER.unRegisterQuery(context.getQueryId(), false);
    return resp;
  }

  /**
   * Construct TSExecuteStatementResp and the header of list result set.
   *
   * @param plan maybe ShowPlan or AuthorPlan
   */
  private TSExecuteStatementResp getListDataSetResp(PhysicalPlan plan, QueryDataSet dataSet) {
    TSExecuteStatementResp resp =
        StaticResps.getNoTimeExecuteResp(
            dataSet.getPaths().stream().map(Path::getFullPath).collect(Collectors.toList()),
            dataSet.getDataTypes().stream().map(Enum::toString).collect(Collectors.toList()));
    if (plan instanceof ShowQueryProcesslistPlan) {
      resp.setIgnoreTimeStamp(false);
    }
    return resp;
  }

  /** Redirect query */
  private TSExecuteStatementResp redirectQueryToAnotherNode(
      TSExecuteStatementResp resp, QueryContext context, String ip, int port) {
    LOGGER.debug(
        "need to redirect {} {} to node {}:{}",
        context.getStatement(),
        context.getQueryId(),
        ip,
        port);
    TSStatus status = new TSStatus();
    status.setRedirectNode(new TEndPoint(ip, port));
    status.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
    resp.setStatus(status);
    resp.setQueryId(context.getQueryId());
    return resp;
  }

  private TSExecuteStatementResp executeSelectIntoStatement(
      String statement,
      long statementId,
      PhysicalPlan physicalPlan,
      int fetchSize,
      long timeout,
      long sessionId)
      throws IoTDBException, TException, SQLException, IOException, InterruptedException,
          QueryFilterOptimizationException {
    TSStatus status = SESSION_MANAGER.checkAuthority(physicalPlan, sessionId);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    final long startTime = System.currentTimeMillis();
    final long queryId = SESSION_MANAGER.requestQueryId(statementId, true);
    QueryContext context =
        serviceProvider.genQueryContext(
            queryId, physicalPlan.isDebug(), startTime, statement, timeout);
    final SelectIntoPlan selectIntoPlan = (SelectIntoPlan) physicalPlan;
    final QueryPlan queryPlan = selectIntoPlan.getQueryPlan();

    QUERY_FREQUENCY_RECORDER.incrementAndGet();
    AUDIT_LOGGER.debug(
        "Session {} execute select into: {}", SESSION_MANAGER.getCurrSessionId(), statement);
    if (queryPlan.isEnableTracing()) {
      TRACING_MANAGER.setSeriesPathNum(queryId, queryPlan.getPaths().size());
    }

    try {
      InsertTabletPlansIterator insertTabletPlansIterator =
          new InsertTabletPlansIterator(
              queryPlan,
              serviceProvider.createQueryDataSet(context, queryPlan, fetchSize),
              selectIntoPlan.getFromPath(),
              selectIntoPlan.getIntoPaths(),
              selectIntoPlan.isIntoPathsAligned());
      while (insertTabletPlansIterator.hasNext()) {
        List<InsertTabletPlan> insertTabletPlans = insertTabletPlansIterator.next();
        if (insertTabletPlans.isEmpty()) {
          continue;
        }
        TSStatus executionStatus = insertTabletsInternally(insertTabletPlans, sessionId);
        if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && executionStatus.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
          return RpcUtils.getTSExecuteStatementResp(executionStatus).setQueryId(queryId);
        }
      }

      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS).setQueryId(queryId);
    } finally {
      SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
      addOperationLatency(Operation.EXECUTE_SELECT_INTO, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
    }
  }

  private TSStatus insertTabletsInternally(
      List<InsertTabletPlan> insertTabletPlans, long sessionId) {
    InsertMultiTabletsPlan insertMultiTabletsPlan = new InsertMultiTabletsPlan();
    for (int i = 0; i < insertTabletPlans.size(); i++) {
      InsertTabletPlan insertTabletPlan = insertTabletPlans.get(i);
      TSStatus status = SESSION_MANAGER.checkAuthority(insertTabletPlan, sessionId);

      if (status != null) {
        // not authorized
        insertMultiTabletsPlan.getResults().put(i, status);
      }
    }
    insertMultiTabletsPlan.setInsertTabletPlanList(insertTabletPlans);

    return executeNonQueryPlan(insertMultiTabletsPlan);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }

      if (!SESSION_MANAGER.hasDataset(req.queryId)) {
        return RpcUtils.getTSFetchResultsResp(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed query"));
      }

      Future<TSFetchResultsResp> resp =
          QueryTaskManager.getInstance()
              .submit(new FetchResultsTask(req.sessionId, req.queryId, req.fetchSize, req.isAlign));
      return resp.get();
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSFetchResultsResp(onQueryException(e, OperationType.FETCH_RESULTS));
    } catch (Exception e) {
      return RpcUtils.getTSFetchResultsResp(onQueryException(e, OperationType.FETCH_RESULTS));
    }
  }

  private TSQueryDataSet fillRpcReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, InterruptedException, QueryProcessException {
    WatermarkEncoder encoder = getWatermarkEncoder(userName);
    return queryDataSet instanceof DirectAlignByTimeDataSet
        ? ((DirectAlignByTimeDataSet) queryDataSet).fillBuffer(fetchSize, encoder)
        : QueryDataSetUtils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize, encoder);
  }

  private TSQueryNonAlignDataSet fillRpcNonAlignReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, QueryProcessException, InterruptedException {
    WatermarkEncoder encoder = getWatermarkEncoder(userName);
    return ((DirectNonAlignDataSet) queryDataSet).fillBuffer(fetchSize, encoder);
  }

  private TSTracingInfo fillRpcReturnTracingInfo(long queryId) {
    return TRACING_MANAGER.fillRpcReturnTracingInfo(queryId);
  }

  private WatermarkEncoder getWatermarkEncoder(String userName) throws TException, AuthException {
    IAuthorizer authorizer = AuthorizerManager.getInstance();

    WatermarkEncoder encoder = null;
    if (CONFIG.isEnableWatermark() && authorizer.isUserUseWaterMark(userName)) {
      if (CONFIG.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
        encoder = new GroupedLSBWatermarkEncoder(CONFIG);
      } else {
        throw new UnSupportedDataTypeException(
            String.format(
                "Watermark method is not supported yet: %s", CONFIG.getWatermarkMethodName()));
      }
    }
    return encoder;
  }

  /** update statement can be: 1. select-into statement 2. non-query statement */
  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }

    try {
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  req.statement,
                  SESSION_MANAGER.getZoneId(req.sessionId),
                  SESSION_MANAGER.getClientVersion(req.sessionId));
      return physicalPlan.isQuery()
          ? RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is a query statement.")
          : executeUpdateStatement(
              req.statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              req.getSessionId());
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(
              e, "\"" + req.statement + "\". " + OperationType.EXECUTE_UPDATE_STATEMENT));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(
              e, "\"" + req.statement + "\". " + OperationType.EXECUTE_UPDATE_STATEMENT));
    }
  }

  /** update statement can be: 1. select-into statement 2. non-query statement */
  private TSExecuteStatementResp executeUpdateStatement(
      String statement,
      long statementId,
      PhysicalPlan plan,
      int fetchSize,
      long timeout,
      long sessionId)
      throws TException, SQLException, IoTDBException, IOException, InterruptedException,
          QueryFilterOptimizationException {
    return plan.isSelectInto()
        ? executeSelectIntoStatement(statement, statementId, plan, fetchSize, timeout, sessionId)
        : executeNonQueryStatement(plan, sessionId);
  }

  private TSExecuteStatementResp executeNonQueryStatement(PhysicalPlan plan, long sessionId) {
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, sessionId);
    return status != null
        ? new TSExecuteStatementResp(status)
        : RpcUtils.getTSExecuteStatementResp(executeNonQueryPlan(plan))
            .setQueryId(SESSION_MANAGER.requestQueryId(false));
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
    properties.setIsReadOnly(CommonDescriptor.getInstance().getConfig().isReadOnly());
    properties.setThriftMaxFrameSize(
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize());
    return properties;
  }

  @Override
  public TSStatus insertRecords(TSInsertRecordsReq req) {
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
    boolean allCheckSuccess = true;

    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      try {
        // check whether measurement is legal according to syntax convention
        PathUtils.isLegalSingleMeasurements(req.getMeasurementsList().get(i));
        InsertRowPlan plan =
            new InsertRowPlan(
                new PartialPath(req.getPrefixPaths().get(i)),
                req.getTimestamps().get(i),
                req.getMeasurementsList().get(i).toArray(new String[0]),
                req.valuesList.get(i),
                req.isAligned);
        TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
        if (status != null) {
          insertRowsPlan.getResults().put(i, status);
          allCheckSuccess = false;
        }
        insertRowsPlan.addOneInsertRowPlan(plan, i);
      } catch (IoTDBException e) {
        allCheckSuccess = false;
        insertRowsPlan
            .getResults()
            .put(i, onIoTDBException(e, OperationType.INSERT_RECORDS, e.getErrorCode()));
      } catch (Exception e) {
        allCheckSuccess = false;
        insertRowsPlan
            .getResults()
            .put(
                i,
                onNPEOrUnexpectedException(
                    e, OperationType.INSERT_RECORDS, TSStatusCode.INTERNAL_SERVER_ERROR));
      }
    }
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);

    return judgeFinalTsStatus(
        allCheckSuccess, tsStatus, insertRowsPlan.getResults(), req.prefixPaths.size());
  }

  private TSStatus judgeFinalTsStatus(
      boolean allCheckSuccess,
      TSStatus executeTsStatus,
      Map<Integer, TSStatus> checkTsStatus,
      int totalRowCount) {

    if (allCheckSuccess) {
      return executeTsStatus;
    }

    if (executeTsStatus.subStatus == null) {
      TSStatus[] tmpSubTsStatus = new TSStatus[totalRowCount];
      Arrays.fill(tmpSubTsStatus, RpcUtils.SUCCESS_STATUS);
      executeTsStatus.subStatus = Arrays.asList(tmpSubTsStatus);
    }
    for (Entry<Integer, TSStatus> entry : checkTsStatus.entrySet()) {
      executeTsStatus.subStatus.set(entry.getKey(), entry.getValue());
    }
    return RpcUtils.getStatus(executeTsStatus.subStatus);
  }

  @Override
  public TSStatus insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
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

    List<TSStatus> statusList = new ArrayList<>();
    try {
      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());
      InsertRowsOfOneDevicePlan plan =
          new InsertRowsOfOneDevicePlan(
              new PartialPath(req.getPrefixPath()),
              req.getTimestamps(),
              req.getMeasurementsList(),
              req.getValuesList(),
              req.isAligned);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      statusList.add(status != null ? status : executeNonQueryPlan(plan));
    } catch (IoTDBException e) {
      statusList.add(
          onIoTDBException(e, OperationType.INSERT_RECORDS_OF_ONE_DEVICE, e.getErrorCode()));
    } catch (Exception e) {
      statusList.add(
          onNPEOrUnexpectedException(
              e, OperationType.INSERT_RECORDS_OF_ONE_DEVICE, TSStatusCode.INTERNAL_SERVER_ERROR));
    }

    TSStatus resp = RpcUtils.getStatus(statusList);
    for (TSStatus status : resp.subStatus) {
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return resp;
      }
    }

    resp.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    return resp;
  }

  @Override
  public TSStatus insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq req) {
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

    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.timestamps.size(); i++) {
      InsertRowPlan plan = new InsertRowPlan();
      try {
        // check whether measurement is legal according to syntax convention
        PathUtils.isLegalSingleMeasurements(req.getMeasurementsList().get(i));
        plan.setDevicePath(new PartialPath(req.getPrefixPath()));
        plan.setTime(req.getTimestamps().get(i));
        addMeasurementAndValue(plan, req.getMeasurementsList().get(i), req.getValuesList().get(i));
        plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
        plan.setNeedInferType(true);
        plan.setAligned(req.isAligned);
        TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());

        if (status != null) {
          insertRowsPlan.getResults().put(i, status);
          allCheckSuccess = false;
        }
        insertRowsPlan.addOneInsertRowPlan(plan, i);
      } catch (IoTDBException e) {
        insertRowsPlan
            .getResults()
            .put(
                i,
                onIoTDBException(
                    e, OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE, e.getErrorCode()));
        allCheckSuccess = false;
      } catch (Exception e) {
        insertRowsPlan
            .getResults()
            .put(
                i,
                onNPEOrUnexpectedException(
                    e,
                    OperationType.INSERT_STRING_RECORDS_OF_ONE_DEVICE,
                    TSStatusCode.INTERNAL_SERVER_ERROR));
        allCheckSuccess = false;
      }
    }
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);

    return judgeFinalTsStatus(
        allCheckSuccess, tsStatus, insertRowsPlan.getResults(), req.timestamps.size());
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
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

    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowPlan plan = new InsertRowPlan();
      try {
        // check whether measurement is legal according to syntax convention
        PathUtils.isLegalSingleMeasurements(req.getMeasurementsList().get(i));
        plan.setDevicePath(new PartialPath(req.getPrefixPaths().get(i)));
        plan.setTime(req.getTimestamps().get(i));
        addMeasurementAndValue(plan, req.getMeasurementsList().get(i), req.getValuesList().get(i));
        plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
        plan.setNeedInferType(true);
        plan.setAligned(req.isAligned);
        TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());

        if (status != null) {
          insertRowsPlan.getResults().put(i, status);
          allCheckSuccess = false;
        }
        insertRowsPlan.addOneInsertRowPlan(plan, i);
      } catch (IoTDBException e) {
        insertRowsPlan
            .getResults()
            .put(i, onIoTDBException(e, OperationType.INSERT_STRING_RECORDS, e.getErrorCode()));
        allCheckSuccess = false;
      } catch (Exception e) {
        insertRowsPlan
            .getResults()
            .put(
                i,
                onNPEOrUnexpectedException(
                    e, OperationType.INSERT_STRING_RECORDS, TSStatusCode.INTERNAL_SERVER_ERROR));
        allCheckSuccess = false;
      }
    }
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);

    return judgeFinalTsStatus(
        allCheckSuccess, tsStatus, insertRowsPlan.getResults(), req.prefixPaths.size());
  }

  private void addMeasurementAndValue(
      InsertRowPlan insertRowPlan, List<String> measurements, List<String> values) {
    List<String> newMeasurements = new ArrayList<>(measurements.size());
    List<Object> newValues = new ArrayList<>(values.size());

    for (int i = 0; i < measurements.size(); ++i) {
      String value = values.get(i);
      if (value.isEmpty()) {
        continue;
      }
      newMeasurements.add(measurements.get(i));
      newValues.add(value);
    }

    insertRowPlan.setValues(newValues.toArray(new Object[0]));
    insertRowPlan.setMeasurements(newMeasurements.toArray(new String[0]));
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
  public TSStatus insertRecord(TSInsertRecordReq req) {
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

      InsertRowPlan plan =
          new InsertRowPlan(
              new PartialPath(req.getPrefixPath()),
              req.getTimestamp(),
              req.getMeasurements().toArray(new String[0]),
              req.values,
              req.isAligned);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_RECORD, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) {
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

      InsertRowPlan plan = new InsertRowPlan();
      plan.setDevicePath(new PartialPath(req.getPrefixPath()));
      plan.setTime(req.getTimestamp());
      plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
      plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
      plan.setValues(req.getValues().toArray(new Object[0]));
      plan.setNeedInferType(true);
      plan.setAligned(req.isAligned);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_STRING_RECORD, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_STRING_RECORD, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      DeletePlan plan = new DeletePlan();
      plan.setDeleteStartTime(req.getStartTime());
      plan.setDeleteEndTime(req.getEndTime());
      List<PartialPath> paths = new ArrayList<>();
      for (String path : req.getPaths()) {
        paths.add(new PartialPath(path));
      }
      plan.addPaths(paths);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());

      return status != null ? new TSStatus(status) : new TSStatus(executeNonQueryPlan(plan));
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_DATA, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_DATA, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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

      InsertTabletPlan insertTabletPlan =
          new InsertTabletPlan(new PartialPath(req.getPrefixPath()), req.measurements);
      insertTabletPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      insertTabletPlan.setColumns(
          QueryDataSetUtils.readTabletValuesFromBuffer(
              req.values, req.types, req.types.size(), req.size));
      insertTabletPlan.setBitMaps(
          QueryDataSetUtils.readBitMapsFromBuffer(req.values, req.types.size(), req.size));
      insertTabletPlan.setRowCount(req.size);
      insertTabletPlan.setDataTypes(req.types);
      insertTabletPlan.setAligned(req.isAligned);
      TSStatus status = SESSION_MANAGER.checkAuthority(insertTabletPlan, req.getSessionId());

      return status != null ? status : executeNonQueryPlan(insertTabletPlan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_TABLET, e.getErrorCode());
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

      return insertTabletsInternally(req);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_TABLETS, e.getErrorCode());
    } catch (NullPointerException e) {
      LOGGER.error("{}: error occurs when insertTablets", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLETS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  private InsertTabletPlan constructInsertTabletPlan(TSInsertTabletsReq req, int i)
      throws MetadataException {
    // check whether measurement is legal according to syntax convention
    PathUtils.isLegalSingleMeasurementLists(req.getMeasurementsList());
    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(req.prefixPaths.get(i)), req.measurementsList.get(i));
    insertTabletPlan.setTimes(
        QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i)));
    insertTabletPlan.setColumns(
        QueryDataSetUtils.readTabletValuesFromBuffer(
            req.valuesList.get(i),
            req.typesList.get(i),
            req.measurementsList.get(i).size(),
            req.sizeList.get(i)));
    insertTabletPlan.setBitMaps(
        QueryDataSetUtils.readBitMapsFromBuffer(
            req.valuesList.get(i), req.measurementsList.get(i).size(), req.sizeList.get(i)));
    insertTabletPlan.setRowCount(req.sizeList.get(i));
    insertTabletPlan.setDataTypes(req.typesList.get(i));
    insertTabletPlan.setAligned(req.isAligned);
    return insertTabletPlan;
  }

  /** construct one InsertMultiTabletsPlan and process it */
  public TSStatus insertTabletsInternally(TSInsertTabletsReq req) throws MetadataException {
    List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
    InsertMultiTabletsPlan insertMultiTabletsPlan = new InsertMultiTabletsPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertTabletPlan insertTabletPlan = constructInsertTabletPlan(req, i);
      TSStatus status = SESSION_MANAGER.checkAuthority(insertTabletPlan, req.getSessionId());
      if (status != null) {
        // not authorized
        insertMultiTabletsPlan.getResults().put(i, status);
      }
      insertTabletPlanList.add(insertTabletPlan);
    }

    insertMultiTabletsPlan.setInsertTabletPlanList(insertTabletPlanList);
    return executeNonQueryPlan(insertMultiTabletsPlan);
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      if (!SESSION_MANAGER.checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      SetStorageGroupPlan plan = new SetStorageGroupPlan(new PartialPath(storageGroup));
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, sessionId);

      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.SET_STORAGE_GROUP, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.SET_STORAGE_GROUP, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    try {
      if (!SESSION_MANAGER.checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> storageGroupList = new ArrayList<>();
      for (String storageGroup : storageGroups) {
        storageGroupList.add(new PartialPath(storageGroup));
      }
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, sessionId);
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_STORAGE_GROUPS, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_STORAGE_GROUPS, TSStatusCode.EXECUTE_STATEMENT_ERROR);
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
      CreateTimeSeriesPlan plan =
          new CreateTimeSeriesPlan(
              new PartialPath(req.path),
              TSDataType.values()[req.dataType],
              TSEncoding.values()[req.encoding],
              CompressionType.values()[req.compressor],
              req.props,
              req.tags,
              req.attributes,
              req.measurementAlias);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_TIMESERIES, e.getErrorCode());
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

      // check whether measurement is legal according to syntax convention

      PathUtils.isLegalSingleMeasurements(req.getMeasurements());

      PathUtils.isLegalSingleMeasurements(req.getMeasurementAlias());

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create aligned timeseries {}.{}",
            SESSION_MANAGER.getCurrSessionId(),
            req.getPrefixPath(),
            req.getMeasurements());
      }

      List<TSDataType> dataTypes = new ArrayList<>();
      for (int dataType : req.dataTypes) {
        dataTypes.add(TSDataType.values()[dataType]);
      }
      List<TSEncoding> encodings = new ArrayList<>();
      for (int encoding : req.encodings) {
        encodings.add(TSEncoding.values()[encoding]);
      }
      List<CompressionType> compressors = new ArrayList<>();
      for (int compressor : req.compressors) {
        compressors.add(CompressionType.values()[compressor]);
      }

      CreateAlignedTimeSeriesPlan plan =
          new CreateAlignedTimeSeriesPlan(
              new PartialPath(req.prefixPath),
              req.measurements,
              dataTypes,
              encodings,
              compressors,
              req.measurementAlias,
              req.tagsList,
              req.attributesList);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_ALIGNED_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_ALIGNED_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
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

      // measurementAlias is also a nodeName

      PathUtils.isLegalSingleMeasurements(req.measurementAliasList);

      CreateMultiTimeSeriesPlan multiPlan = new CreateMultiTimeSeriesPlan();
      List<PartialPath> paths = new ArrayList<>(req.paths.size());
      List<TSDataType> dataTypes = new ArrayList<>(req.dataTypes.size());
      List<TSEncoding> encodings = new ArrayList<>(req.dataTypes.size());
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

      // for authority check
      CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan();
      for (int i = 0; i < req.paths.size(); i++) {
        plan.setPath(new PartialPath(req.paths.get(i)));
        TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
        if (status != null) {
          // not authorized
          multiPlan.getResults().put(i, status);
        }

        paths.add(new PartialPath(req.paths.get(i)));
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
      }
      for (int i = 0; i < req.dataTypes.size(); i++) {
        dataTypes.add(TSDataType.values()[req.dataTypes.get(i)]);
        encodings.add(TSEncoding.values()[req.encodings.get(i)]);
      }

      multiPlan.setPaths(paths);
      multiPlan.setDataTypes(dataTypes);
      multiPlan.setEncodings(encodings);
      multiPlan.setCompressors(compressors);
      multiPlan.setAlias(alias);
      multiPlan.setProps(props);
      multiPlan.setTags(tags);
      multiPlan.setAttributes(attributes);
      multiPlan.setIndexes(new ArrayList<>());

      return executeNonQueryPlan(multiPlan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_MULTI_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      LOGGER.error("creating multi timeseries fails", e);
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_MULTI_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> paths) {
    try {
      if (!SESSION_MANAGER.checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> pathList = new ArrayList<>();
      for (String path : paths) {
        pathList.add(new PartialPath(path));
      }
      DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, sessionId);
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.DELETE_TIMESERIES, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.DELETE_TIMESERIES, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public long requestStatementId(long sessionId) {
    return SESSION_MANAGER.requestStatementId(sessionId);
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) throws TException {
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

      CreateTemplatePlan plan;
      // Construct plan from serialized request
      ByteBuffer buffer = ByteBuffer.wrap(req.getSerializedTemplate());
      plan = CreateTemplatePlan.deserializeFromReq(buffer);
      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalMeasurementLists(plan.getMeasurements());
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());

      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.CREATE_SCHEMA_TEMPLATE, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_SCHEMA_TEMPLATE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus appendSchemaTemplate(TSAppendSchemaTemplateReq req) {
    try {
      // check whether measurement is legal according to syntax convention
      PathUtils.isLegalMeasurements(req.getMeasurements());
    } catch (IoTDBException e) {
      onIoTDBException(e, OperationType.EXECUTE_NON_QUERY_PLAN, e.getErrorCode());
    }

    int size = req.getMeasurementsSize();
    String[] measurements = new String[size];
    TSDataType[] dataTypes = new TSDataType[size];
    TSEncoding[] encodings = new TSEncoding[size];
    CompressionType[] compressionTypes = new CompressionType[size];

    for (int i = 0; i < req.getDataTypesSize(); i++) {
      measurements[i] = req.getMeasurements().get(i);
      dataTypes[i] = TSDataType.values()[req.getDataTypes().get(i)];
      encodings[i] = TSEncoding.values()[req.getEncodings().get(i)];
      compressionTypes[i] = CompressionType.values()[req.getCompressors().get(i)];
    }

    AppendTemplatePlan plan =
        new AppendTemplatePlan(
            req.getName(), req.isAligned, measurements, dataTypes, encodings, compressionTypes);
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    PruneTemplatePlan plan =
        new PruneTemplatePlan(req.getName(), Collections.singletonList(req.getPath()));
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
    try {
      TSQueryTemplateResp resp = new TSQueryTemplateResp();
      String path;
      switch (TemplateQueryType.values()[req.getQueryType()]) {
        case COUNT_MEASUREMENTS:
          resp.setQueryType(TemplateQueryType.COUNT_MEASUREMENTS.ordinal());
          resp.setCount(IoTDB.schemaProcessor.countMeasurementsInTemplate(req.name));
          break;
        case IS_MEASUREMENT:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.IS_MEASUREMENT.ordinal());
          resp.setResult(IoTDB.schemaProcessor.isMeasurementInTemplate(req.name, path));
          break;
        case PATH_EXIST:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.PATH_EXIST.ordinal());
          resp.setResult(IoTDB.schemaProcessor.isPathExistsInTemplate(req.name, path));
          break;
        case SHOW_MEASUREMENTS:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.SHOW_MEASUREMENTS.ordinal());
          resp.setMeasurements(IoTDB.schemaProcessor.getMeasurementsInTemplate(req.name, path));
          break;
        case SHOW_TEMPLATES:
          resp.setQueryType(TemplateQueryType.SHOW_TEMPLATES.ordinal());
          resp.setMeasurements(new ArrayList<>(IoTDB.schemaProcessor.getAllTemplates()));
          break;
        case SHOW_SET_TEMPLATES:
          path = req.getName();
          resp.setQueryType(TemplateQueryType.SHOW_SET_TEMPLATES.ordinal());
          resp.setMeasurements(new ArrayList<>(IoTDB.schemaProcessor.getPathsSetTemplate(path)));
          break;
        case SHOW_USING_TEMPLATES:
          path = req.getName();
          resp.setQueryType(TemplateQueryType.SHOW_USING_TEMPLATES.ordinal());
          resp.setMeasurements(new ArrayList<>(IoTDB.schemaProcessor.getPathsUsingTemplate(path)));
          break;
      }
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully"));
      return resp;
    } catch (MetadataException e) {
      LOGGER.error("fail to query schema template because: " + e);
    }
    return null;
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} set device template {}.{}",
          SESSION_MANAGER.getCurrSessionId(),
          req.getTemplateName(),
          req.getPrefixPath());
    }

    try {
      SetTemplatePlan plan = new SetTemplatePlan(req.templateName, req.prefixPath);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    }
  }

  @Override
  public TSStatus unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} unset schema template {}.{}",
          SESSION_MANAGER.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTemplateName());
    }

    try {
      UnsetTemplatePlan plan = new UnsetTemplatePlan(req.prefixPath, req.templateName);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    }
  }

  @Override
  public TSStatus dropSchemaTemplate(TSDropSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} drop schema template {}.",
          SESSION_MANAGER.getCurrSessionId(),
          req.getTemplateName());
    }

    DropTemplatePlan plan = new DropTemplatePlan(req.templateName);
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus handshake(TSyncIdentityInfo info) throws TException {
    return SyncService.getInstance().handshake(info);
  }

  @Override
  public TSStatus sendPipeData(ByteBuffer buff) throws TException {
    return SyncService.getInstance().transportPipeData(buff);
  }

  @Override
  public TSStatus sendFile(TSyncTransportMetaInfo metaInfo, ByteBuffer buff) throws TException {
    return SyncService.getInstance().transportFile(metaInfo, buff);
  }

  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      return serviceProvider.executeNonQuery(plan)
          ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
          : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } catch (Exception e) {
      return onNonQueryException(e, OperationType.EXECUTE_NON_QUERY_PLAN);
    }
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
}
