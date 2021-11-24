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

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.selectinto.InsertTabletPlansIterator;
import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.TemplateQueryType;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.MeasurementInfo;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SelectIntoPlan;
import org.apache.iotdb.db.qp.physical.crud.UDFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.tracing.TracingConstant;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.DirectNonAlignDataSet;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
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
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

import com.google.common.primitives.Bytes;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onIoTDBException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNPEOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNonQueryException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.tryCatchQueryException;

/** Thrift RPC implementation at server side. */
public class TSServiceImpl extends BasicServiceProvider implements TSIService.Iface {

  // main logger
  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);

  private static final String INFO_INTERRUPT_ERROR =
      "Current Thread interrupted when dealing with request {}";

  private static final int MAX_SIZE = CONFIG.getQueryCacheSizeInMetric();
  private static final int DELETE_SIZE = 20;

  private static final List<SqlArgument> sqlArgumentList = new ArrayList<>(MAX_SIZE);

  private long startTime = -1L;

  public TSServiceImpl() throws QueryProcessException {
    super();
  }

  public static List<SqlArgument> getSqlArgumentList() {
    return sqlArgumentList;
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    BasicOpenSessionResp openSessionResp =
        openSession(req.username, req.password, req.zoneId, req.client_protocol);
    TSStatus tsStatus = RpcUtils.getStatus(openSessionResp.getCode(), openSessionResp.getMessage());
    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
    return resp.setSessionId(openSessionResp.getSessionId());
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    return new TSStatus(
        !closeSession(req.sessionId)
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
    return closeOperation(
        req.sessionId, req.queryId, req.statementId, req.isSetStatementId(), req.isSetQueryId());
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSFetchMetadataResp resp = new TSFetchMetadataResp();

    if (!checkLogin(req.getSessionId())) {
      return resp.setStatus(getNotLoggedInStatus());
    }

    TSStatus status;
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          resp.setMetadataInJson(IoTDB.metaManager.getMetadataInString());
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

  private String getMetadataInString() {
    return IoTDB.metaManager.getMetadataInString();
  }

  protected List<MeasurementPath> getPaths(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getMeasurementPaths(path);
  }

  protected TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getSeriesType(path);
  }

  private boolean executeInsertRowsPlan(InsertRowsPlan insertRowsPlan, List<TSStatus> result) {
    long t1 = System.currentTimeMillis();
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ROWS_PLAN_IN_BATCH, t1);
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
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH, t1);

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
    if (!checkLogin(req.getSessionId())) {
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
            processor.parseSQLToPhysicalPlan(statement, sessionManager.getZoneId(req.sessionId));
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

          TSStatus status = checkAuthority(physicalPlan, req.getSessionId());
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
          TSStatus status = checkAuthority(physicalPlan, req.getSessionId());
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
          Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
          result.add(resp.status);
          if (resp.getStatus().code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            isAllSuccessful = false;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error occurred when executing executeBatchStatement: ", e);
        TSStatus status = tryCatchQueryException(e);
        if (status != null) {
          result.add(status);
          isAllSuccessful = false;
        } else {
          result.add(
              onNPEOrUnexpectedException(
                  e,
                  "\"" + statement + "\". " + OperationType.EXECUTE_BATCH_STATEMENT,
                  TSStatusCode.INTERNAL_SERVER_ERROR));
        }
      }
    }
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    return isAllSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully")
        : RpcUtils.getStatus(result);
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    String statement = req.getStatement();
    startTime = System.currentTimeMillis();

    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(statement, sessionManager.getZoneId(req.getSessionId()));

      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              req.getSessionId(),
              req.isEnableRedirectQuery(),
              req.isJdbcQuery())
          : executeUpdateStatement(
              statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              req.getSessionId());
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
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      String statement = req.getStatement();
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(statement, sessionManager.getZoneId(req.sessionId));

      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              req.getSessionId(),
              req.isEnableRedirectQuery(),
              req.isJdbcQuery())
          : RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
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
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      PhysicalPlan physicalPlan =
          processor.rawDataQueryReqToPhysicalPlan(req, sessionManager.getZoneId(req.sessionId));
      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              "",
              req.statementId,
              physicalPlan,
              req.fetchSize,
              CONFIG.getQueryTimeoutThreshold(),
              req.sessionId,
              req.isEnableRedirectQuery(),
              req.isJdbcQuery())
          : RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
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
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
      }

      PhysicalPlan physicalPlan =
          processor.lastDataQueryReqToPhysicalPlan(req, sessionManager.getZoneId(req.sessionId));
      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              "",
              req.statementId,
              physicalPlan,
              req.fetchSize,
              CONFIG.getQueryTimeoutThreshold(),
              req.sessionId,
              req.isEnableRedirectQuery(),
              req.isJdbcQuery())
          : RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
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

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByTimePlan, UDFPlan,
   *     some AuthorPlan
   */
  @SuppressWarnings({"squid:S3776", "squid:S1141"}) // Suppress high Cognitive Complexity warning
  private TSExecuteStatementResp internalExecuteQueryStatement(
      String statement,
      long statementId,
      PhysicalPlan plan,
      int fetchSize,
      long timeout,
      long sessionId,
      boolean enableRedirect,
      boolean isJdbcQuery)
      throws QueryProcessException, SQLException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          TException, AuthException {
    queryFrequencyRecorder.incrementAndGet();
    AUDIT_LOGGER.debug(
        "Session {} execute Query: {}", sessionManager.getCurrSessionId(), statement);

    final long queryStartTime = System.currentTimeMillis();
    final long queryId = sessionManager.requestQueryId(statementId, true);
    QueryContext context =
        genQueryContext(queryId, plan.isDebug(), queryStartTime, statement, timeout);

    if (plan instanceof QueryPlan && ((QueryPlan) plan).isEnableTracing()) {
      context.setEnableTracing(true);
      tracingManager.setStartTime(queryId, this.startTime);
      tracingManager.registerActivity(
          queryId,
          String.format(TracingConstant.ACTIVITY_START_EXECUTE, statement),
          this.startTime);
      tracingManager.registerActivity(queryId, TracingConstant.ACTIVITY_PARSE_SQL, queryStartTime);
      if (!(plan instanceof AlignByDevicePlan)) {
        tracingManager.setSeriesPathNum(queryId, plan.getPaths().size());
      }
    }

    try {
      String username = sessionManager.getUsername(sessionId);
      plan.setLoginUserName(username);

      TSExecuteStatementResp resp = null;
      // execute it before createDataSet since it may change the content of query plan
      if (plan instanceof QueryPlan && !(plan instanceof UDFPlan)) {
        resp = getQueryColumnHeaders(plan, username, isJdbcQuery);
      }
      if (plan instanceof QueryPlan) {
        ((QueryPlan) plan).setEnableRedirect(enableRedirect);
      }
      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(context, plan, fetchSize);
      if (plan instanceof QueryPlan && ((QueryPlan) plan).isEnableTracing()) {
        tracingManager.registerActivity(
            queryId, TracingConstant.ACTIVITY_CREATE_DATASET, System.currentTimeMillis());
      }

      if (newDataSet.getEndPoint() != null && enableRedirect) {
        // redirect query
        LOGGER.debug(
            "need to redirect {} {} to node {}", statement, queryId, newDataSet.getEndPoint());
        TSStatus status = new TSStatus();
        status.setRedirectNode(
            new EndPoint(newDataSet.getEndPoint().getIp(), newDataSet.getEndPoint().getPort()));
        status.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
        resp.setStatus(status);
        resp.setQueryId(queryId);
        return resp;
      }

      if (plan instanceof ShowPlan || plan instanceof AuthorPlan) {
        resp = getListDataSetHeaders(newDataSet);
      } else if (plan instanceof UDFPlan
          || (plan instanceof QueryPlan && ((QueryPlan) plan).isGroupByLevel())) {
        resp = getQueryColumnHeaders(plan, username, isJdbcQuery);
      }

      resp.setOperationType(plan.getOperatorType().toString());
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        resp.setIgnoreTimeStamp(true);
      } else if (plan instanceof ShowQueryProcesslistPlan) {
        resp.setIgnoreTimeStamp(false);
      }

      if (newDataSet instanceof DirectNonAlignDataSet) {
        resp.setNonAlignQueryDataSet(fillRpcNonAlignReturnData(fetchSize, newDataSet, username));
      } else {
        try {
          TSQueryDataSet tsQueryDataSet = fillRpcReturnData(fetchSize, newDataSet, username);
          resp.setQueryDataSet(tsQueryDataSet);
        } catch (RedirectException e) {
          LOGGER.debug("need to redirect {} {} to {}", statement, queryId, e.getEndPoint());
          if (enableRedirect) {
            // redirect query
            TSStatus status = new TSStatus();
            status.setRedirectNode(e.getEndPoint());
            status.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
            resp.setStatus(status);
            resp.setQueryId(queryId);
            return resp;
          } else {
            LOGGER.error(
                "execute {} error, if session does not support redirect,"
                    + " should not throw redirection exception.",
                statement,
                e);
          }
        }
      }

      resp.setQueryId(queryId);

      if (plan instanceof AlignByDevicePlan && ((QueryPlan) plan).isEnableTracing()) {
        tracingManager.setSeriesPathNum(queryId, ((AlignByDeviceDataSet) newDataSet).getPathsNum());
      }

      if (CONFIG.isEnableMetricService()) {
        long endTime = System.currentTimeMillis();
        SqlArgument sqlArgument = new SqlArgument(resp, plan, statement, queryStartTime, endTime);
        synchronized (sqlArgumentList) {
          sqlArgumentList.add(sqlArgument);
          if (sqlArgumentList.size() >= MAX_SIZE) {
            sqlArgumentList.subList(0, DELETE_SIZE).clear();
          }
        }
      }
      queryTimeManager.unRegisterQuery(queryId, false);

      if (plan instanceof QueryPlan && ((QueryPlan) plan).isEnableTracing()) {
        tracingManager.registerActivity(
            queryId, TracingConstant.ACTIVITY_REQUEST_COMPLETE, System.currentTimeMillis());

        TSTracingInfo tsTracingInfo = fillRpcReturnTracingInfo(queryId);
        resp.setTracingInfo(tsTracingInfo);
      }

      return resp;
    } catch (Exception e) {
      sessionManager.releaseQueryResourceNoExceptions(queryId);
      throw e;
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, queryStartTime);
      long costTime = System.currentTimeMillis() - queryStartTime;
      if (costTime >= CONFIG.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
    }
  }

  private TSExecuteStatementResp getListDataSetHeaders(QueryDataSet dataSet) {
    return StaticResps.getNoTimeExecuteResp(
        dataSet.getPaths().stream().map(Path::getFullPath).collect(Collectors.toList()),
        dataSet.getDataTypes().stream().map(Enum::toString).collect(Collectors.toList()));
  }

  /** get ResultSet schema */
  private TSExecuteStatementResp getQueryColumnHeaders(
      PhysicalPlan physicalPlan, String username, boolean isJdbcQuery)
      throws AuthException, TException, MetadataException {

    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(physicalPlan.getAuthPaths(), physicalPlan, username)) {
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(
              TSStatusCode.NO_PERMISSION_ERROR,
              "No permissions for this operation " + physicalPlan.getOperatorType()));
    }

    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);

    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, respColumns, columnsTypes);
    } else if (plan instanceof LastQueryPlan) {
      // Last Query should return different respond instead of the static one
      // because the query dataset and query id is different although the header of last query is
      // same.
      return StaticResps.LAST_RESP.deepCopy();
    } else if (plan.isGroupByLevel()) {
      for (Map.Entry<String, AggregateResult> groupPathResult :
          ((AggregationPlan) plan).getGroupPathsResultMap().entrySet()) {
        respColumns.add(groupPathResult.getKey());
        columnsTypes.add(groupPathResult.getValue().getResultDataType().toString());
      }
    } else {
      List<String> respSgColumns = new ArrayList<>();
      BitSet aliasMap = new BitSet();
      getWideQueryHeaders(plan, respColumns, columnsTypes, respSgColumns, isJdbcQuery, aliasMap);
      resp.setColumnNameIndexMap(plan.getPathToIndex());
      resp.setSgColumns(respSgColumns);
      List<Byte> byteList = new ArrayList<>();
      byteList.addAll(Bytes.asList(aliasMap.toByteArray()));
      resp.setAliasColumns(byteList);
    }
    resp.setColumns(respColumns);
    resp.setDataTypeList(columnsTypes);
    return resp;
  }

  // wide means not align by device
  private void getWideQueryHeaders(
      QueryPlan plan,
      List<String> respColumns,
      List<String> columnTypes,
      List<String> respSgColumns,
      Boolean isJdbcQuery,
      BitSet aliasList)
      throws TException, MetadataException {
    List<ResultColumn> resultColumns = plan.getResultColumns();
    List<MeasurementPath> paths = plan.getPaths();
    List<TSDataType> seriesTypes = new ArrayList<>();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (int i = 0; i < resultColumns.size(); ++i) {
          if (isJdbcQuery) {
            String sgName =
                IoTDB.metaManager.getBelongedStorageGroup(plan.getPaths().get(i)).getFullPath();
            respSgColumns.add(sgName);
            if (resultColumns.get(i).getAlias() == null) {
              respColumns.add(
                  resultColumns.get(i).getResultColumnName().substring(sgName.length() + 1));
            } else {
              aliasList.set(i);
              respColumns.add(resultColumns.get(i).getResultColumnName());
            }
          } else {
            respColumns.add(resultColumns.get(i).getResultColumnName());
          }
          seriesTypes.add(paths.get(i).getSeriesType());
        }
        break;
      case AGGREGATION:
      case GROUP_BY_TIME:
      case GROUP_BY_FILL:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (ResultColumn resultColumn : resultColumns) {
          respColumns.add(resultColumn.getResultColumnName());
        }
        seriesTypes = SchemaUtils.getSeriesTypesByPaths(paths, aggregations);
        break;
      case UDTF:
        seriesTypes = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
          respColumns.add(resultColumns.get(i).getResultColumnName());
          seriesTypes.add(resultColumns.get(i).getDataType());
        }
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
    Map<String, MeasurementInfo> measurementInfoMap = plan.getMeasurementInfoMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    for (String measurement : measurements) {
      MeasurementInfo measurementInfo = measurementInfoMap.get(measurement);
      TSDataType type = TSDataType.TEXT;
      switch (measurementInfo.getMeasurementType()) {
        case Exist:
          type = measurementInfo.getColumnDataType();
          break;
        case NonExist:
        case Constant:
          type = TSDataType.TEXT;
      }
      String measurementAlias = measurementInfo.getMeasurementAlias();
      respColumns.add(measurementAlias != null ? measurementAlias : measurement);
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

  private TSExecuteStatementResp executeSelectIntoStatement(
      String statement,
      long statementId,
      PhysicalPlan physicalPlan,
      int fetchSize,
      long timeout,
      long sessionId)
      throws IoTDBException, TException, SQLException, IOException, InterruptedException,
          QueryFilterOptimizationException {
    TSStatus status = checkAuthority(physicalPlan, sessionId);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    final SelectIntoPlan selectIntoPlan = (SelectIntoPlan) physicalPlan;
    final QueryPlan queryPlan = selectIntoPlan.getQueryPlan();

    if (queryPlan instanceof UDTFPlan
        && queryPlan
            .getResultColumns()
            .get(0)
            .getExpression()
            .isTimeSeriesGeneratingFunctionExpression()
    //        && ((FunctionExpression) queryPlan.getResultColumns().get(0).getExpression())
    //            .getFunctionName()
    //            .equalsIgnoreCase("en")
    ) {
      return executeSelectIntoStatementXianyi(
          (UDTFPlan) queryPlan, this, statement, statementId, timeout, fetchSize, sessionId);
    }

    final long startTime = System.currentTimeMillis();
    final long queryId = sessionManager.requestQueryId(statementId, true);
    QueryContext context =
        genQueryContext(queryId, physicalPlan.isDebug(), startTime, statement, timeout);

    try {
      InsertTabletPlansIterator insertTabletPlansIterator =
          new InsertTabletPlansIterator(
              queryPlan,
              createQueryDataSet(context, queryPlan, fetchSize),
              selectIntoPlan.getFromPath(),
              selectIntoPlan.getIntoPaths());
      while (insertTabletPlansIterator.hasNext()) {
        TSStatus executionStatus =
            insertTabletsInternally(insertTabletPlansIterator.next(), sessionId);
        if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && executionStatus.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
          return RpcUtils.getTSExecuteStatementResp(executionStatus).setQueryId(queryId);
        }
      }

      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS).setQueryId(queryId);
    } finally {
      sessionManager.releaseQueryResourceNoExceptions(queryId);
    }
  }

  private TSExecuteStatementResp executeSelectIntoStatementXianyi(
      UDTFPlan udtfPlan,
      TSServiceImpl tsService,
      String statement,
      long statementId,
      long timeout,
      int fetchSize,
      long sessionId) {
    ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() << 1);
    List<ForkJoinTask<Void>> futures = new ArrayList<>();
    for (String subStatement : split(udtfPlan, statement)) {
      futures.add(
          forkJoinPool.submit(
              new InsertTabletPlanTask(
                  tsService, statementId, timeout, fetchSize, sessionId, subStatement)));
    }
    for (ForkJoinTask<Void> v : futures) {
      v.join();
    }
    forkJoinPool.shutdown();
    return RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
  }

  private List<String> split(UDTFPlan udtfPlan, String statement) {
    List<String> statements = new ArrayList<>();

    String prefix = statement.split("where")[0] + " where ";

    return statements;
  }

  private class InsertTabletPlanTask extends RecursiveTask<Void> {

    private final TSServiceImpl tsService;
    private final long statementId;
    private final long timeout;
    private final int fetchSize;
    private final long sessionId;
    private final String statement;

    InsertTabletPlanTask(
        TSServiceImpl tsService,
        long statementId,
        long timeout,
        int fetchSize,
        long sessionId,
        String statement) {
      this.tsService = tsService;
      this.statementId = statementId;
      this.timeout = timeout;
      this.fetchSize = fetchSize;
      this.sessionId = sessionId;
      this.statement = statement;
    }

    @Override
    protected Void compute() {
      try {
        PhysicalPlan physicalPlan =
            processor.parseSQLToPhysicalPlan(statement, sessionManager.getZoneId(sessionId));
        tsService.executeSelectIntoStatement(
            statement, statementId, physicalPlan, fetchSize, timeout, sessionId);
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  private TSStatus insertTabletsInternally(
      List<InsertTabletPlan> insertTabletPlans, long sessionId) {
    InsertMultiTabletPlan insertMultiTabletPlan = new InsertMultiTabletPlan();
    for (int i = 0; i < insertTabletPlans.size(); i++) {
      InsertTabletPlan insertTabletPlan = insertTabletPlans.get(i);
      TSStatus status = checkAuthority(insertTabletPlan, sessionId);

      if (status != null) {
        // not authorized
        insertMultiTabletPlan.getResults().put(i, status);
      }
    }
    insertMultiTabletPlan.setInsertTabletPlanList(insertTabletPlans);

    return executeNonQueryPlan(insertMultiTabletPlan);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSFetchResultsResp(getNotLoggedInStatus());
      }

      if (!sessionManager.hasDataset(req.queryId)) {
        return RpcUtils.getTSFetchResultsResp(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed query"));
      }

      genQueryContext(req.queryId, false, System.currentTimeMillis(), req.statement, req.timeout);

      QueryDataSet queryDataSet = sessionManager.getDataset(req.queryId);
      if (req.isAlign) {
        TSQueryDataSet result =
            fillRpcReturnData(
                req.fetchSize, queryDataSet, sessionManager.getUsername(req.sessionId));
        boolean hasResultSet = result.bufferForTime().limit() != 0;
        if (!hasResultSet) {
          sessionManager.releaseQueryResourceNoExceptions(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setQueryDataSet(result);
        resp.setIsAlign(true);

        queryTimeManager.unRegisterQuery(req.queryId, false);
        return resp;
      } else {
        TSQueryNonAlignDataSet nonAlignResult =
            fillRpcNonAlignReturnData(
                req.fetchSize, queryDataSet, sessionManager.getUsername(req.sessionId));
        boolean hasResultSet = false;
        for (ByteBuffer timeBuffer : nonAlignResult.getTimeList()) {
          if (timeBuffer.limit() != 0) {
            hasResultSet = true;
            break;
          }
        }
        if (!hasResultSet) {
          sessionManager.removeDataset(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setNonAlignQueryDataSet(nonAlignResult);
        resp.setIsAlign(false);

        queryTimeManager.unRegisterQuery(req.queryId, false);
        return resp;
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSFetchResultsResp(
          onNPEOrUnexpectedException(
              e, OperationType.FETCH_RESULTS, TSStatusCode.INTERNAL_SERVER_ERROR));
    } catch (Exception e) {
      sessionManager.releaseQueryResourceNoExceptions(req.queryId);
      return RpcUtils.getTSFetchResultsResp(
          onNPEOrUnexpectedException(
              e, OperationType.FETCH_RESULTS, TSStatusCode.INTERNAL_SERVER_ERROR));
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
    return tracingManager.fillRpcReturnTracingInfo(queryId);
  }

  private WatermarkEncoder getWatermarkEncoder(String userName) throws TException, AuthException {
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }

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
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(getNotLoggedInStatus());
    }

    try {
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(req.statement, sessionManager.getZoneId(req.sessionId));
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
    TSStatus status = checkAuthority(plan, sessionId);
    return status != null
        ? new TSExecuteStatementResp(status)
        : RpcUtils.getTSExecuteStatementResp(executeNonQueryPlan(plan))
            .setQueryId(sessionManager.requestQueryId(false));
  }

  protected void handleClientExit() {
    Long sessionId = sessionManager.getCurrSessionId();
    if (sessionId != null) {
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
  }

  @Override
  public TSGetTimeZoneResp getTimeZone(long sessionId) {
    try {
      ZoneId zoneId = sessionManager.getZoneId(sessionId);
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
      sessionManager.setTimezone(req.sessionId, req.timeZone);
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
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    if (!checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, first device {}, first time {}",
          sessionManager.getCurrSessionId(),
          req.prefixPaths.get(0),
          req.getTimestamps().get(0));
    }
    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      try {
        InsertRowPlan plan =
            new InsertRowPlan(
                new PartialPath(req.getPrefixPaths().get(i)),
                req.getTimestamps().get(i),
                req.getMeasurementsList().get(i).toArray(new String[0]),
                req.valuesList.get(i),
                req.isAligned);
        TSStatus status = checkAuthority(plan, req.getSessionId());
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
    if (!checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, device {}, first time {}",
          sessionManager.getCurrSessionId(),
          req.prefixPath,
          req.getTimestamps().get(0));
    }

    List<TSStatus> statusList = new ArrayList<>();
    try {
      InsertRowsOfOneDevicePlan plan =
          new InsertRowsOfOneDevicePlan(
              new PartialPath(req.getPrefixPath()),
              req.getTimestamps().toArray(new Long[0]),
              req.getMeasurementsList(),
              req.getValuesList().toArray(new ByteBuffer[0]),
              req.isAligned);
      TSStatus status = checkAuthority(plan, req.getSessionId());
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
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
    if (!checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, first device {}, first time {}",
          sessionManager.getCurrSessionId(),
          req.prefixPaths.get(0),
          req.getTimestamps().get(0));
    }

    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowPlan plan = new InsertRowPlan();
      try {
        plan.setDeviceId(new PartialPath(req.getPrefixPaths().get(i)));
        plan.setTime(req.getTimestamps().get(i));
        addMeasurementAndValue(plan, req.getMeasurementsList().get(i), req.getValuesList().get(i));
        plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
        plan.setNeedInferType(true);
        plan.setAligned(req.isAligned);
        TSStatus status = checkAuthority(plan, req.getSessionId());

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
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          sessionManager.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTimestamp());

      InsertRowPlan plan =
          new InsertRowPlan(
              new PartialPath(req.getPrefixPath()),
              req.getTimestamp(),
              req.getMeasurements().toArray(new String[0]),
              req.values,
              req.isAligned);
      TSStatus status = checkAuthority(plan, req.getSessionId());
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
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          sessionManager.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTimestamp());

      InsertRowPlan plan = new InsertRowPlan();
      plan.setDeviceId(new PartialPath(req.getPrefixPath()));
      plan.setTime(req.getTimestamp());
      plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
      plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
      plan.setValues(req.getValues().toArray(new Object[0]));
      plan.setNeedInferType(true);
      plan.setAligned(req.isAligned);
      TSStatus status = checkAuthority(plan, req.getSessionId());
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
      if (!checkLogin(req.getSessionId())) {
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
      TSStatus status = checkAuthority(plan, req.getSessionId());

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
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      InsertTabletPlan insertTabletPlan =
          new InsertTabletPlan(new PartialPath(req.getPrefixPath()), req.measurements);
      insertTabletPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      insertTabletPlan.setColumns(
          QueryDataSetUtils.readValuesFromBuffer(
              req.values, req.types, req.types.size(), req.size));
      insertTabletPlan.setBitMaps(
          QueryDataSetUtils.readBitMapsFromBuffer(req.values, req.types.size(), req.size));
      insertTabletPlan.setRowCount(req.size);
      insertTabletPlan.setDataTypes(req.types);
      insertTabletPlan.setAligned(req.isAligned);
      TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());

      return status != null ? status : executeNonQueryPlan(insertTabletPlan);
    } catch (IoTDBException e) {
      return onIoTDBException(e, OperationType.INSERT_TABLET, e.getErrorCode());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.INSERT_TABLET, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
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
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  private InsertTabletPlan constructInsertTabletPlan(TSInsertTabletsReq req, int i)
      throws IllegalPathException {
    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(req.prefixPaths.get(i)), req.measurementsList.get(i));
    insertTabletPlan.setTimes(
        QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i)));
    insertTabletPlan.setColumns(
        QueryDataSetUtils.readValuesFromBuffer(
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

  /** construct one InsertMultiTabletPlan and process it */
  public TSStatus insertTabletsInternally(TSInsertTabletsReq req) throws IllegalPathException {
    List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
    InsertMultiTabletPlan insertMultiTabletPlan = new InsertMultiTabletPlan();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertTabletPlan insertTabletPlan = constructInsertTabletPlan(req, i);
      TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());
      if (status != null) {
        // not authorized
        insertMultiTabletPlan.getResults().put(i, status);
      }
      insertTabletPlanList.add(insertTabletPlan);
    }

    insertMultiTabletPlan.setInsertTabletPlanList(insertTabletPlanList);
    return executeNonQueryPlan(insertMultiTabletPlan);
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      if (!checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      SetStorageGroupPlan plan = new SetStorageGroupPlan(new PartialPath(storageGroup));
      TSStatus status = checkAuthority(plan, sessionId);

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
      if (!checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> storageGroupList = new ArrayList<>();
      for (String storageGroup : storageGroups) {
        storageGroupList.add(new PartialPath(storageGroup));
      }
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);
      TSStatus status = checkAuthority(plan, sessionId);
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
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create timeseries {}", sessionManager.getCurrSessionId(), req.getPath());
      }

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
      TSStatus status = checkAuthority(plan, req.getSessionId());
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
      if (!checkLogin(req.getSessionId())) {
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
            sessionManager.getCurrSessionId(),
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
              req.measurementAlias);
      TSStatus status = checkAuthority(plan, req.getSessionId());
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
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create {} timeseries, the first is {}",
            sessionManager.getCurrSessionId(),
            req.getPaths().size(),
            req.getPaths().get(0));
      }

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
        TSStatus status = checkAuthority(plan, req.getSessionId());
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
      if (!checkLogin(sessionId)) {
        return getNotLoggedInStatus();
      }

      List<PartialPath> pathList = new ArrayList<>();
      for (String path : paths) {
        pathList.add(new PartialPath(path));
      }
      DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);
      TSStatus status = checkAuthority(plan, sessionId);
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
    return sessionManager.requestStatementId(sessionId);
  }

  @Override
  public TSStatus createSchemaTemplate(TSCreateSchemaTemplateReq req) throws TException {
    try {
      if (!checkLogin(req.getSessionId())) {
        return getNotLoggedInStatus();
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create device template {}.{}.{}.{}.{}.{}",
            sessionManager.getCurrSessionId(),
            req.getName(),
            req.getSchemaNames(),
            req.getMeasurements(),
            req.getDataTypes(),
            req.getEncodings(),
            req.getCompressors());
      }

      CreateTemplatePlan plan;
      if (req.getMeasurements().size() == 0) {
        // Construct plan from serialized request
        ByteBuffer buffer = ByteBuffer.wrap(req.getSerializedTemplate());
        plan = CreateTemplatePlan.deserializeFromReq(buffer);
      } else {
        int size = req.getMeasurementsSize();
        String[][] measurements = new String[size][];
        TSDataType[][] dataTypes = new TSDataType[size][];
        TSEncoding[][] encodings = new TSEncoding[size][];
        CompressionType[][] compressionTypes = new CompressionType[size][];

        for (int i = 0; i < size; i++) {
          int alignedSize = req.getMeasurements().get(i).size();
          measurements[i] = new String[alignedSize];
          dataTypes[i] = new TSDataType[alignedSize];
          encodings[i] = new TSEncoding[alignedSize];
          compressionTypes[i] = new CompressionType[alignedSize];
          for (int j = 0; j < alignedSize; j++) {
            measurements[i][j] = req.getMeasurements().get(i).get(j);
            dataTypes[i][j] = TSDataType.values()[req.getDataTypes().get(i).get(j)];
            encodings[i][j] = TSEncoding.values()[req.getEncodings().get(i).get(j)];
            compressionTypes[i][j] = CompressionType.values()[req.getCompressors().get(i).get(j)];
          }
        }

        plan =
            new CreateTemplatePlan(
                req.getName(), measurements, dataTypes, encodings, compressionTypes);
      }
      TSStatus status = checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, OperationType.CREATE_SCHEMA_TEMPLATE, TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus appendSchemaTemplate(TSAppendSchemaTemplateReq req) throws TException {
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
    TSStatus status = checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus pruneSchemaTemplate(TSPruneSchemaTemplateReq req) throws TException {
    PruneTemplatePlan plan =
        new PruneTemplatePlan(req.getName(), Collections.singletonList(req.getPath()));
    TSStatus status = checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req) throws TException {
    try {
      TSQueryTemplateResp resp = new TSQueryTemplateResp();
      String path;
      switch (TemplateQueryType.values()[req.getQueryType()]) {
        case COUNT_MEASUREMENTS:
          resp.setQueryType(TemplateQueryType.COUNT_MEASUREMENTS.ordinal());
          resp.setCount(IoTDB.metaManager.countMeasurementsInTemplate(req.name));
          break;
        case IS_MEASUREMENT:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.IS_MEASUREMENT.ordinal());
          resp.setResult(IoTDB.metaManager.isMeasurementInTemplate(req.name, path));
          break;
        case PATH_EXIST:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.PATH_EXIST.ordinal());
          resp.setResult(IoTDB.metaManager.isPathExistsInTemplate(req.name, path));
          break;
        case SHOW_MEASUREMENTS:
          path = req.getMeasurement();
          resp.setQueryType(TemplateQueryType.SHOW_MEASUREMENTS.ordinal());
          resp.setMeasurements(IoTDB.metaManager.getMeasurementsInTemplate(req.name, path));
          break;
      }
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully"));
      return resp;
    } catch (MetadataException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public TSStatus setSchemaTemplate(TSSetSchemaTemplateReq req) throws TException {
    if (!checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} set device template {}.{}",
          sessionManager.getCurrSessionId(),
          req.getTemplateName(),
          req.getPrefixPath());
    }

    SetTemplatePlan plan = new SetTemplatePlan(req.templateName, req.prefixPath);
    TSStatus status = checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  @Override
  public TSStatus unsetSchemaTemplate(TSUnsetSchemaTemplateReq req) throws TException {
    if (!checkLogin(req.getSessionId())) {
      return getNotLoggedInStatus();
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session-{} unset device template {}.{}",
          sessionManager.getCurrSessionId(),
          req.getPrefixPath(),
          req.getTemplateName());
    }

    UnsetTemplatePlan plan = new UnsetTemplatePlan(req.prefixPath, req.templateName);
    TSStatus status = checkAuthority(plan, req.getSessionId());
    return status != null ? status : executeNonQueryPlan(plan);
  }

  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      return executeNonQuery(plan)
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
}
