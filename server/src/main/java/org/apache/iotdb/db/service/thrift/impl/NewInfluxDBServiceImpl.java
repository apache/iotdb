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
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.handler.AbstractQueryHandler;
import org.apache.iotdb.db.protocol.influxdb.handler.QueryHandlerFactory;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.IInfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManagerFactory;
import org.apache.iotdb.db.protocol.influxdb.operator.InfluxQueryOperator;
import org.apache.iotdb.db.protocol.influxdb.sql.InfluxDBLogicalGenerator;
import org.apache.iotdb.db.protocol.influxdb.util.InfluxReqAndRespUtils;
import org.apache.iotdb.db.qp.logical.Operator;
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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.List;

public class NewInfluxDBServiceImpl implements IInfluxDBServiceWithHandler {

  private static final ClientRPCServiceImpl clientRPCService = new ClientRPCServiceImpl();

  private final IInfluxDBMetaManager metaManager;

  private final AbstractQueryHandler queryHandler;

  public NewInfluxDBServiceImpl() {
    metaManager = InfluxDBMetaManagerFactory.getInstance();
    metaManager.recover();
    queryHandler = QueryHandlerFactory.getInstance();
  }

  public static ClientRPCServiceImpl getClientRPCService() {
    return clientRPCService;
  }

  @Override
  public InfluxOpenSessionResp openSession(InfluxOpenSessionReq req) throws TException {
    TSOpenSessionReq tsOpenSessionReq = InfluxReqAndRespUtils.convertOpenSessionReq(req);
    TSOpenSessionResp tsOpenSessionResp = clientRPCService.openSession(tsOpenSessionReq);
    return InfluxReqAndRespUtils.convertOpenSessionResp(tsOpenSessionResp);
  }

  @Override
  public InfluxTSStatus closeSession(InfluxCloseSessionReq req) {
    TSCloseSessionReq tsCloseSessionReq = InfluxReqAndRespUtils.convertCloseSessionReq(req);
    TSStatus tsStatus = clientRPCService.closeSession(tsCloseSessionReq);
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus);
  }

  @Override
  public InfluxTSStatus writePoints(InfluxWritePointsReq req) {
    List<InfluxTSStatus> tsStatusList = new ArrayList<>();
    int executeCode = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    for (Point point :
        InfluxLineParser.parserRecordsToPointsWithPrecision(req.lineProtocol, req.precision)) {
      IoTDBPoint iotdbPoint = new IoTDBPoint(req.database, point, metaManager, req.sessionId);
      try {
        TSInsertRecordReq insertRecordReq = iotdbPoint.convertToTSInsertRecordReq(req.sessionId);
        TSStatus tsStatus = clientRPCService.insertRecord(insertRecordReq);
        tsStatusList.add(DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus));
      } catch (IoTDBConnectionException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
    return new InfluxTSStatus().setCode(executeCode).setSubStatus(tsStatusList);
  }

  @Override
  public InfluxTSStatus createDatabase(InfluxCreateDatabaseReq req) {
    TSStatus tsStatus =
        clientRPCService.setStorageGroup(req.sessionId, "root." + req.getDatabase());
    if (tsStatus.getCode() == TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode()) {
      tsStatus.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      tsStatus.setMessage("Execute successfully");
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus);
  }

  @Override
  public InfluxQueryResultRsp query(InfluxQueryReq req) throws TException {
    Operator operator = InfluxDBLogicalGenerator.generate(req.command);
    queryHandler.checkInfluxDBQueryOperator(operator);
    return queryHandler.queryInfluxDB(req.database, (InfluxQueryOperator) operator, req.sessionId);
  }

  public static TSExecuteStatementResp executeStatement(String sql, long sessionId) {
    TSExecuteStatementReq tsExecuteStatementReq = new TSExecuteStatementReq();
    tsExecuteStatementReq.setStatement(sql);
    tsExecuteStatementReq.setSessionId(sessionId);
    tsExecuteStatementReq.setStatementId(
        NewInfluxDBServiceImpl.getClientRPCService().requestStatementId(sessionId));
    tsExecuteStatementReq.setFetchSize(InfluxConstant.DEFAULT_FETCH_SIZE);
    TSExecuteStatementResp executeStatementResp =
        NewInfluxDBServiceImpl.getClientRPCService().executeStatement(tsExecuteStatementReq);
    return executeStatementResp;
  }

  @Override
  public void handleClientExit() {
    clientRPCService.handleClientExit();
  }
}
