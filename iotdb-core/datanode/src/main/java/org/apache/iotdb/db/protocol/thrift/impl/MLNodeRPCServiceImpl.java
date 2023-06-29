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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant.ClientVersion;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TimeZone;

public class MLNodeRPCServiceImpl implements IMLNodeRPCServiceWithHandler {

  public static final String ML_METRICS_PATH_PREFIX = "root.__system.ml.exp";

  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final IClientSession session;

  public MLNodeRPCServiceImpl() {
    super();
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
    session = new InternalClientSession("MLNodeService");
    SESSION_MANAGER.registerSession(session);
    SESSION_MANAGER.supplySession(
        session, "MLNode", TimeZone.getDefault().getID(), ClientVersion.V_1_0);
  }

  @Override
  public TFetchTimeseriesResp fetchTimeseries(TFetchTimeseriesReq req) throws TException {
    boolean finished = false;
    TFetchTimeseriesResp resp = new TFetchTimeseriesResp();
    Throwable t = null;
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
              partitionFetcher,
              schemaFetcher,
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
      t = e;
      resp.setStatus(ErrorHandlingUtils.onQueryException(e, OperationType.EXECUTE_STATEMENT));
      return resp;
    } catch (Error error) {
      t = error;
      throw error;
    } finally {
      if (finished) {
        COORDINATOR.cleanupQueryExecution(resp.queryId, t);
      }
    }
  }

  @Override
  public TFetchMoreDataResp fetchMoreData(TFetchMoreDataReq req) throws TException {
    TFetchMoreDataResp resp = new TFetchMoreDataResp();
    boolean finished = false;
    Throwable t = null;
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
      t = e;
      resp.setStatus(ErrorHandlingUtils.onQueryException(e, OperationType.FETCH_RESULTS));
      return resp;
    } catch (Error error) {
      t = error;
      throw error;
    } finally {
      if (finished) {
        COORDINATOR.cleanupQueryExecution(req.queryId, t);
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
              partitionFetcher,
              schemaFetcher);
      return result.status;
    } catch (Exception e) {
      return ErrorHandlingUtils.onQueryException(e, OperationType.INSERT_RECORD);
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
