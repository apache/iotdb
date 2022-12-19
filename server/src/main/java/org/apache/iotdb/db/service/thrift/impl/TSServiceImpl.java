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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Operation;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.AuthorizerManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
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
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.control.tracing.TracingConstant;
import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.DirectNonAlignDataSet;
import org.apache.iotdb.db.query.pool.QueryTaskManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.StaticResps;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
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
    private final IClientSession session;
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
        IClientSession session,
        String statement,
        long statementId,
        long timeout,
        int fetchSize,
        boolean isJdbcQuery,
        boolean enableRedirectQuery) {
      this.plan = plan;
      this.queryStartTime = queryStartTime;
      this.session = session;
      this.statement = statement;
      this.statementId = statementId;
      this.timeout = timeout;
      this.fetchSize = fetchSize;
      this.isJdbcQuery = isJdbcQuery;
      this.enableRedirectQuery = enableRedirectQuery;
    }

    @Override
    public TSExecuteStatementResp call() throws Exception {
      return null;
    }
  }

  private class FetchResultsTask implements Callable<TSFetchResultsResp> {

    private final IClientSession session;
    private final long queryId;
    private final int fetchSize;
    private final boolean isAlign;

    public FetchResultsTask(IClientSession session, long queryId, int fetchSize, boolean isAlign) {
      this.session = session;
      this.queryId = queryId;
      this.fetchSize = fetchSize;
      this.isAlign = isAlign;
    }

    @Override
    public TSFetchResultsResp call() throws Exception {
      return null;
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
  public TSExecuteStatementResp executeQueryStatementV2(TSExecuteStatementReq req)
      throws TException {
    return null;
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatementV2(TSExecuteStatementReq req)
      throws TException {
    return null;
  }

  @Override
  public TSExecuteStatementResp executeStatementV2(TSExecuteStatementReq req) throws TException {
    return null;
  }

  @Override
  public TSExecuteStatementResp executeRawDataQueryV2(TSRawDataQueryReq req) throws TException {
    return null;
  }

  @Override
  public TSExecuteStatementResp executeLastDataQueryV2(TSLastDataQueryReq req) throws TException {
    return null;
  }

  @Override
  public TSFetchResultsResp fetchResultsV2(TSFetchResultsReq req) throws TException {
    return null;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    // TODO implement
    return RpcUtils.getStatus(TSStatusCode.QUERY_NOT_ALLOWED, "Cancellation is not implemented");
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSFetchMetadataResp resp = new TSFetchMetadataResp();

    if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
      return resp.setStatus(getNotLoggedInStatus());
    }

    TSStatus status;
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          resp.setMetadataInJson("{}");
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

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    return null;
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    String statement = req.getStatement();
    try {
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  statement,
                  SESSION_MANAGER.getCurrSession().getZoneId(),
                  SESSION_MANAGER.getCurrSession().getClientVersion());

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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      String statement = req.getStatement();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  statement,
                  SESSION_MANAGER.getCurrSession().getZoneId(),
                  SESSION_MANAGER.getCurrSession().getClientVersion());

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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .rawDataQueryReqToPhysicalPlan(
                  req,
                  SESSION_MANAGER.getCurrSession().getZoneId(),
                  SESSION_MANAGER.getCurrSession().getClientVersion());

      if (physicalPlan.isQuery()) {
        Future<TSExecuteStatementResp> resp =
            QueryTaskManager.getInstance()
                .submit(
                    new QueryTask(
                        physicalPlan,
                        startTime,
                        SESSION_MANAGER.getCurrSession(),
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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      long startTime = System.currentTimeMillis();
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .lastDataQueryReqToPhysicalPlan(
                  req,
                  SESSION_MANAGER.getCurrSession().getZoneId(),
                  SESSION_MANAGER.getCurrSession().getClientVersion());

      if (physicalPlan.isQuery()) {
        Future<TSExecuteStatementResp> resp =
            QueryTaskManager.getInstance()
                .submit(
                    new QueryTask(
                        physicalPlan,
                        startTime,
                        SESSION_MANAGER.getCurrSession(),
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
    TSStatus status =
        SESSION_MANAGER.checkAuthority(physicalPlan, SESSION_MANAGER.getCurrSession());
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }
    QueryTask queryTask =
        new QueryTask(
            physicalPlan,
            startTime,
            SESSION_MANAGER.getCurrSession(),
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
              TSStatusCode.NO_PERMISSION,
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
    status.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
    resp.setStatus(status);
    resp.setQueryId(context.getQueryId());
    return resp;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }

      Future<TSFetchResultsResp> resp =
          QueryTaskManager.getInstance()
              .submit(
                  new FetchResultsTask(
                      SESSION_MANAGER.getCurrSession(), req.queryId, req.fetchSize, req.isAlign));
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
    if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }

    try {
      PhysicalPlan physicalPlan =
          serviceProvider
              .getPlanner()
              .parseSQLToPhysicalPlan(
                  req.statement,
                  SESSION_MANAGER.getCurrSession().getZoneId(),
                  SESSION_MANAGER.getCurrSession().getClientVersion());
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
      throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  private TSExecuteStatementResp executeNonQueryStatement(PhysicalPlan plan) {
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
    return status != null
        ? new TSExecuteStatementResp(status)
        : RpcUtils.getTSExecuteStatementResp(executeNonQueryPlan(plan))
            .setQueryId(SESSION_MANAGER.requestQueryId());
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
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    return null;
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
    return null;
  }

  @Override
  public TSStatus insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq req) {
    return null;
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
    return null;
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
    return null;
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) {
    return null;
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    return null;
  }

  @Override
  public TSStatus insertTablet(TSInsertTabletReq req) {
    return null;
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
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

  public TSStatus insertTabletsInternally(TSInsertTabletsReq req) throws MetadataException {
    return null;
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      SetStorageGroupPlan plan = new SetStorageGroupPlan(new PartialPath(storageGroup));
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());

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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> storageGroupList = new ArrayList<>();
      for (String storageGroup : storageGroups) {
        storageGroupList.add(new PartialPath(storageGroup));
      }
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create timeseries {}", SESSION_MANAGER.getCurrSession(), req.getPath());
      }

      // measurementAlias is also a nodeName
      PathUtils.checkIsLegalSingleMeasurementsAndUpdate(
          Collections.singletonList(req.getMeasurementAlias()));
      CreateTimeSeriesPlan plan =
          new CreateTimeSeriesPlan(
              new PartialPath(req.path),
              TSDataType.values()[req.dataType],
              TSEncoding.values()[req.encoding],
              CompressionType.deserialize((byte) req.compressor),
              req.props,
              req.tags,
              req.attributes,
              req.measurementAlias);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      // check whether measurement is legal according to syntax convention

      PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurements());

      PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.getMeasurementAlias());

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create aligned timeseries {}.{}",
            SESSION_MANAGER.getCurrSession(),
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
        compressors.add(CompressionType.deserialize((byte) compressor));
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
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create {} timeseries, the first is {}",
            SESSION_MANAGER.getCurrSession(),
            req.getPaths().size(),
            req.getPaths().get(0));
      }

      // measurementAlias is also a nodeName

      PathUtils.checkIsLegalSingleMeasurementsAndUpdate(req.measurementAliasList);

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
        TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
        if (status != null) {
          // not authorized
          multiPlan.getResults().put(i, status);
        }

        paths.add(new PartialPath(req.paths.get(i)));
        compressors.add(CompressionType.deserialize(req.compressors.get(i).byteValue()));
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
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> pathList = new ArrayList<>();
      for (String path : paths) {
        pathList.add(new PartialPath(path));
      }
      DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
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
    return SESSION_MANAGER.requestStatementId(SESSION_MANAGER.getCurrSession());
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) throws TException {
    try {
      if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create schema template {}",
            SESSION_MANAGER.getCurrSession(),
            req.getName());
      }

      CreateTemplatePlan plan;
      // Construct plan from serialized request
      ByteBuffer buffer = ByteBuffer.wrap(req.getSerializedTemplate());
      plan = CreateTemplatePlan.deserializeFromReq(buffer);
      // check whether measurement is legal according to syntax convention
      PathUtils.checkIsLegalMeasurementListsAndUpdate(plan.getMeasurements());
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());

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
      PathUtils.checkIsLegalMeasurementsAndUpdate(req.getMeasurements());
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
      compressionTypes[i] = CompressionType.deserialize(req.getCompressors().get(i).byteValue());
    }

    AppendTemplatePlan plan =
        new AppendTemplatePlan(
            req.getName(), req.isAligned, measurements, dataTypes, encodings, compressionTypes);
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) {
    PruneTemplatePlan plan =
        new PruneTemplatePlan(req.getName(), Collections.singletonList(req.getPath()));
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) {
    return null;
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} set device template {}.{}",
          SESSION_MANAGER.getCurrSession(),
          req.getTemplateName(),
          req.getPrefixPath());
    }

    try {
      SetTemplatePlan plan = new SetTemplatePlan(req.templateName, req.prefixPath);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    }
  }

  @Override
  public TSStatus unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} unset schema template {}.{}",
          SESSION_MANAGER.getCurrSession(),
          req.getPrefixPath(),
          req.getTemplateName());
    }

    try {
      UnsetTemplatePlan plan = new UnsetTemplatePlan(req.prefixPath, req.templateName);
      TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (IllegalPathException e) {
      return onIoTDBException(e, OperationType.EXECUTE_STATEMENT, e.getErrorCode());
    }
  }

  @Override
  public TSStatus dropSchemaTemplate(TSDropSchemaTemplateReq req) throws TException {
    if (!SESSION_MANAGER.checkLogin(SESSION_MANAGER.getCurrSession())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} drop schema template {}.",
          SESSION_MANAGER.getCurrSession(),
          req.getTemplateName());
    }

    DropTemplatePlan plan = new DropTemplatePlan(req.templateName);
    TSStatus status = SESSION_MANAGER.checkAuthority(plan, SESSION_MANAGER.getCurrSession());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus handshake(TSyncIdentityInfo info) throws TException {
    return SyncService.getInstance()
        .handshake(info, SESSION_MANAGER.getCurrSession().getClientAddress(), null, null);
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
    return null;
  }

  @Override
  public TSConnectionInfoResp fetchAllConnectionsInfo() {
    throw new UnsupportedOperationException();
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
        TSStatusCode.NOT_LOGIN,
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  /** Add stat of operation into metrics */
  private void addOperationLatency(Operation operation, long startTime) {
    MetricService.getInstance()
        .histogram(
            System.currentTimeMillis() - startTime,
            Metric.OPERATION.toString(),
            MetricLevel.IMPORTANT,
            "name",
            operation.getName());
  }
}
