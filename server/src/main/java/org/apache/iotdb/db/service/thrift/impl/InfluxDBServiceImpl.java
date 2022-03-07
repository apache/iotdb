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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.sql.InfluxDBLogicalGenerator;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.db.utils.InfluxDBUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxDBService;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSCreateDatabaseReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSQueryReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSQueryRsp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSStatus;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSWritePointsReq;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBServiceImpl implements InfluxDBService.Iface {

  private final ServiceProvider serviceProvider;

  private final InfluxDBMetaManager metaManager;

  public InfluxDBServiceImpl() {
    serviceProvider = IoTDB.serviceProvider;
    metaManager = InfluxDBMetaManager.getInstance();
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    BasicOpenSessionResp basicOpenSessionResp =
        serviceProvider.openSession(
            req.username, req.password, req.zoneId, TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    return new TSOpenSessionResp()
        .setStatus(
            RpcUtils.getInfluxDBStatus(
                basicOpenSessionResp.getCode(), basicOpenSessionResp.getMessage()))
        .setSessionId(basicOpenSessionResp.getSessionId());
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    return new TSStatus(
        !serviceProvider.closeSession(req.sessionId)
            ? RpcUtils.getInfluxDBStatus(TSStatusCode.NOT_LOGIN_ERROR)
            : RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSStatus writePoints(TSWritePointsReq req) {
    if (!serviceProvider.checkLogin(req.sessionId)) {
      return getNotLoggedInStatus();
    }

    List<TSStatus> tsStatusList = new ArrayList<>();
    int executeCode = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    for (Point point :
        InfluxLineParser.parserRecordsToPointsWithPrecision(req.lineProtocol, req.precision)) {
      IoTDBPoint iotdbPoint = new IoTDBPoint(req.database, point, metaManager);
      try {
        InsertRowPlan plan = iotdbPoint.convertToInsertRowPlan();
        TSStatus tsStatus = executeNonQueryPlan(plan, req.sessionId);
        if (executeCode == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && tsStatus.getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
          executeCode = tsStatus.getCode();
        }
        tsStatusList.add(tsStatus);
      } catch (StorageGroupNotSetException
          | StorageEngineException
          | IllegalPathException
          | IoTDBConnectionException
          | QueryProcessException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
    return new TSStatus().setCode(executeCode).setSubStatus(tsStatusList);
  }

  @Override
  public TSStatus createDatabase(TSCreateDatabaseReq req) throws TException {
    if (!serviceProvider.checkLogin(req.sessionId)) {
      return getNotLoggedInStatus();
    }
    try {
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new PartialPath("root." + req.getDatabase()));
      return executeNonQueryPlan(setStorageGroupPlan, req.getSessionId());
    } catch (IllegalPathException
        | QueryProcessException
        | StorageGroupNotSetException
        | StorageEngineException e) {
      if (e instanceof QueryProcessException && e.getErrorCode() == 300) {
        return RpcUtils.getInfluxDBStatus(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), "Execute successfully");
      }
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public TSQueryRsp query(TSQueryReq req) throws TException {
    Operator operator = InfluxDBLogicalGenerator.generate(req.command);
    InfluxDBUtils.checkInfluxDBQueryOperator(operator);
    return queryInfluxDB(req.database, (InfluxQueryOperator) operator, req.sessionId);
  }

  private TSQueryRsp queryInfluxDB(
      String database, InfluxQueryOperator queryOperator, long sessionId) {
    String measurement = queryOperator.getFromComponent().getPrefixPaths().get(0).getFullPath();
    // The list of fields under the current measurement and the order of the specified rules
    Map<String, Integer> fieldOrders = new HashMap<>();
    Map<Integer, String> fieldOrdersReversed = new HashMap<>();
    updateFieldOrders(database, measurement, fieldOrders, fieldOrdersReversed);
    QueryResult queryResult;
    // contain filter condition or have common query the result of by traversal.
    if (queryOperator.getWhereComponent() != null
        || queryOperator.getSelectComponent().isHasCommonQuery()) {
      // step1 : generate query results
      queryResult = InfluxDBUtils.queryExpr(queryOperator.getWhereComponent().getFilterOperator());
      // step2 : select filter
      InfluxDBUtils.ProcessSelectComponent(queryResult, queryOperator.getSelectComponent());
    }
    // don't contain filter condition and only have function use iotdb function.
    else {
      queryResult =
          InfluxDBUtils.queryFuncWithoutFilter(
              queryOperator.getSelectComponent(), database, measurement, serviceProvider);
    }
    return null;
  }

  private void updateFieldOrders(
      String database,
      String measurement,
      Map<String, Integer> fieldOrders,
      Map<Integer, String> fieldOrdersReversed) {
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String showTimeseriesSql = "show timeseries root." + database + '.' + measurement + "**";
      PhysicalPlan physicalPlan =
          serviceProvider.getPlanner().parseSQLToPhysicalPlan(showTimeseriesSql);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              showTimeseriesSql,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, physicalPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      int fieldNums = 0;
      Map<String, Integer> tagOrders =
          InfluxDBMetaManager.database2Measurement2TagOrders.get(database).get(measurement);
      int tagOrderNums = tagOrders.size();
      while (queryDataSet.hasNext()) {
        List<Field> fields = queryDataSet.next().getFields();
        String filed = InfluxDBUtils.getFieldByPath(fields.get(0).getStringValue());
        if (!fieldOrders.containsKey(filed)) {
          // The corresponding order of fields is 1 + tagNum (the first is timestamp, then all tags,
          // and finally all fields)
          fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
          fieldOrdersReversed.put(tagOrderNums + fieldNums + 1, filed);
          fieldNums++;
        }
      }
    } catch (QueryProcessException
        | TException
        | StorageEngineException
        | SQLException
        | IOException
        | InterruptedException
        | QueryFilterOptimizationException
        | MetadataException e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
  }

  public void handleClientExit() {
    Long sessionId = ServiceProvider.SESSION_MANAGER.getCurrSessionId();
    if (sessionId != null) {
      closeSession(new TSCloseSessionReq(sessionId));
    }
  }

  private TSStatus getNotLoggedInStatus() {
    return RpcUtils.getInfluxDBStatus(
        TSStatusCode.NOT_LOGIN_ERROR.getStatusCode(),
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  private TSStatus executeNonQueryPlan(PhysicalPlan plan, long sessionId)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    org.apache.iotdb.service.rpc.thrift.TSStatus status =
        serviceProvider.checkAuthority(plan, sessionId);
    if (status == null) {
      status =
          serviceProvider.executeNonQuery(plan)
              ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
              : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(status);
  }
}
