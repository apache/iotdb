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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.MetaManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBServiceImpl implements InfluxDBService.Iface {

  private final BasicServiceProvider basicServiceProvider;

  private final MetaManager metaManager;

  public InfluxDBServiceImpl() throws QueryProcessException {
    basicServiceProvider = new BasicServiceProvider();
    metaManager = MetaManager.getInstance();
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    BasicOpenSessionResp basicOpenSessionResp =
        basicServiceProvider.openSession(
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
        !basicServiceProvider.closeSession(req.sessionId)
            ? RpcUtils.getInfluxDBStatus(TSStatusCode.NOT_LOGIN_ERROR)
            : RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSStatus writePoints(TSWritePointsReq req) {
    if (!basicServiceProvider.checkLogin(req.sessionId)) {
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
    if (!basicServiceProvider.checkLogin(req.sessionId)) {
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

  public void handleClientExit() {
    Long sessionId = BasicServiceProvider.sessionManager.getCurrSessionId();
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
        basicServiceProvider.checkAuthority(plan, sessionId);
    if (status == null) {
      status =
          basicServiceProvider.executeNonQuery(plan)
              ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
              : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(status);
  }
}
