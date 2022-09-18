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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.handler.AbstractQueryHandler;
import org.apache.iotdb.db.protocol.influxdb.handler.QueryHandler;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.AbstractInfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.sql.InfluxDBLogicalGenerator;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxCloseSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxCreateDatabaseReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionResp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryResultRsp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxTSStatus;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxWritePointsReq;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBServiceImpl implements IInfluxDBServiceWithHandler {

  private final SessionManager SESSION_MANAGER = SessionManager.getInstance();

  private final AbstractInfluxDBMetaManager metaManager;

  private final AbstractQueryHandler queryHandler;

  public InfluxDBServiceImpl() {
    metaManager = InfluxDBMetaManager.getInstance();
    queryHandler = new QueryHandler();
  }

  @Override
  public InfluxOpenSessionResp openSession(InfluxOpenSessionReq req) throws TException {
    BasicOpenSessionResp basicOpenSessionResp =
        SESSION_MANAGER.openSession(
            req.username, req.password, req.zoneId, TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    return new InfluxOpenSessionResp()
        .setStatus(
            RpcUtils.getInfluxDBStatus(
                basicOpenSessionResp.getCode(), basicOpenSessionResp.getMessage()))
        .setSessionId(basicOpenSessionResp.getSessionId());
  }

  @Override
  public InfluxTSStatus closeSession(InfluxCloseSessionReq req) {
    return new InfluxTSStatus(
        !SESSION_MANAGER.closeSession(req.sessionId)
            ? RpcUtils.getInfluxDBStatus(TSStatusCode.NOT_LOGIN_ERROR)
            : RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public InfluxTSStatus writePoints(InfluxWritePointsReq req) {
    if (!SESSION_MANAGER.checkLogin(req.sessionId)) {
      return getNotLoggedInStatus();
    }

    List<InfluxTSStatus> tsStatusList = new ArrayList<>();
    int executeCode = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    for (Point point :
        InfluxLineParser.parserRecordsToPointsWithPrecision(req.lineProtocol, req.precision)) {
      IoTDBPoint iotdbPoint = new IoTDBPoint(req.database, point, metaManager, req.sessionId);

      try {
        InsertRowPlan plan = iotdbPoint.convertToInsertRowPlan();
        InfluxTSStatus tsStatus = executeNonQueryPlan(plan, req.sessionId);
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
    return new InfluxTSStatus().setCode(executeCode).setSubStatus(tsStatusList);
  }

  @Override
  public InfluxTSStatus createDatabase(InfluxCreateDatabaseReq req) {
    if (!SESSION_MANAGER.checkLogin(req.sessionId)) {
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
      if (e instanceof QueryProcessException
          && e.getErrorCode() == TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode()) {
        return RpcUtils.getInfluxDBStatus(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), "Execute successfully");
      }
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public InfluxQueryResultRsp query(InfluxQueryReq req) throws TException {
    Operator operator = InfluxDBLogicalGenerator.generate(req.command);
    queryHandler.checkInfluxDBQueryOperator(operator);
    return queryHandler.queryInfluxDB(req.database, (InfluxQueryOperator) operator, req.sessionId);
  }

  @Override
  public void handleClientExit() {
    Long sessionId = ServiceProvider.SESSION_MANAGER.getCurrSessionId();
    if (sessionId != null) {
      closeSession(new InfluxCloseSessionReq(sessionId));
    }
  }

  private InfluxTSStatus getNotLoggedInStatus() {
    return RpcUtils.getInfluxDBStatus(
        TSStatusCode.NOT_LOGIN_ERROR.getStatusCode(),
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  private InfluxTSStatus executeNonQueryPlan(PhysicalPlan plan, long sessionId)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    org.apache.iotdb.common.rpc.thrift.TSStatus status =
        SESSION_MANAGER.checkAuthority(plan, sessionId);
    if (status == null) {
      status =
          IoTDB.serviceProvider.executeNonQuery(plan)
              ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
              : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(status);
  }
}
