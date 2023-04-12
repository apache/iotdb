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
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.control.clientsession.InternalClientSession;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFetchMoreDataReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchMoreDataResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchTimeseriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchTimeseriesResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchWindowBatchReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchWindowBatchResp;
import org.apache.iotdb.mpp.rpc.thrift.TRecordModelMetricsReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TimeZone;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class MLNodeRPCServiceImpl implements IMLNodeRPCServiceWithHandler {

  public static final String ML_METRICS_PATH_PREFIX = "root.__system.ml.exp";

  private static final Logger LOGGER = LoggerFactory.getLogger(MLNodeRPCServiceImpl.class);

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private final IPartitionFetcher PARTITION_FETCHER;

  private final ISchemaFetcher SCHEMA_FETCHER;

  private final IClientSession session;

  public MLNodeRPCServiceImpl() {
    super();
    PARTITION_FETCHER = ClusterPartitionFetcher.getInstance();
    SCHEMA_FETCHER = ClusterSchemaFetcher.getInstance();
    session = new InternalClientSession("MLNodeService");
    SESSION_MANAGER.registerSession(session);
    SESSION_MANAGER.supplySession(
        session, "MLNode", TimeZone.getDefault().getID(), ClientVersion.V_1_0);
  }

  @Override
  public TFetchTimeseriesResp fetchTimeseries(TFetchTimeseriesReq req) throws TException {
    boolean finished = false;
    TFetchTimeseriesResp resp = new TFetchTimeseriesResp();

    try {
      QueryStatement s =
          (QueryStatement) StatementGenerator.createStatement(req, session.getZoneId());

      long queryId =
          SESSION_MANAGER.requestQueryId(session, SESSION_MANAGER.requestStatementId(session));
      ExecutionResult result =
          COORDINATOR.execute(
              s,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER,
              req.getTimeout());

      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && result.status.code != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        resp.setStatus(result.status);
        return resp;
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);

      try (SetThreadName threadName = new SetThreadName(result.queryId.getId())) {

        DatasetHeader header = queryExecution.getDatasetHeader();
        resp.setStatus(result.status);
        resp.setColumnNameList(header.getRespColumns());
        resp.setColumnTypeList(header.getRespDataTypeList());
        resp.setColumnNameIndexMap(header.getColumnNameIndexMap());
        resp.setQueryId(queryId);

        Pair<List<ByteBuffer>, Boolean> pair =
            QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, req.fetchSize);
        resp.setTsDataset(pair.left);
        finished = pair.right;
        resp.setHasMoreData(!finished);
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      resp.setStatus(onQueryException(e, OperationType.EXECUTE_STATEMENT));
      return resp;
    } finally {
      if (finished) {
        COORDINATOR.cleanupQueryExecution(resp.queryId);
      }
    }
  }

  @Override
  public TFetchMoreDataResp fetchMoreData(TFetchMoreDataReq req) throws TException {
    TFetchMoreDataResp resp = new TFetchMoreDataResp();
    boolean finished = false;
    try {
      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(req.queryId);
      resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

      if (queryExecution == null) {
        resp.setHasMoreData(false);
        return resp;
      }

      try (SetThreadName queryName = new SetThreadName(queryExecution.getQueryId())) {
        Pair<List<ByteBuffer>, Boolean> pair =
            QueryDataSetUtils.convertQueryResultByFetchSize(queryExecution, req.fetchSize);
        List<ByteBuffer> result = pair.left;
        finished = pair.right;
        resp.setTsDataset(result);
        resp.setHasMoreData(!finished);
        return resp;
      }
    } catch (Exception e) {
      finished = true;
      resp.setStatus(onQueryException(e, OperationType.FETCH_RESULTS));
      return resp;
    } finally {
      if (finished) {
        COORDINATOR.cleanupQueryExecution(req.queryId);
      }
    }
  }

  @Override
  public TSStatus recordModelMetrics(TRecordModelMetricsReq req) throws TException {
    try {
      InsertRowStatement insertRowStatement = StatementGenerator.createStatement(req);

      long queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result =
          COORDINATOR.execute(
              insertRowStatement,
              queryId,
              SESSION_MANAGER.getSessionInfo(session),
              "",
              PARTITION_FETCHER,
              SCHEMA_FETCHER);
      return result.status;
    } catch (Exception e) {
      return onQueryException(e, OperationType.INSERT_RECORD);
    }
  }

  @Override
  public TFetchWindowBatchResp fetchWindowBatch(TFetchWindowBatchReq req) throws TException {
    throw new TException(new UnsupportedOperationException().getCause());
  }

  @Override
  public void handleExit() {
    SESSION_MANAGER.closeSession(session, COORDINATOR::cleanupQueryExecution);
  }
}
