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
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.handler.AbstractQueryHandler;
import org.apache.iotdb.db.protocol.influxdb.handler.QueryHandlerFactory;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.IInfluxDBMetaManager;
import org.apache.iotdb.db.protocol.influxdb.meta.InfluxDBMetaManagerFactory;
import org.apache.iotdb.db.protocol.influxdb.parser.InfluxDBStatementGenerator;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxQueryStatement;
import org.apache.iotdb.db.protocol.influxdb.util.InfluxReqAndRespUtils;
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

/**
 * When using NewIoTDB, use this object to handle read and write requests of the influxdb protocol
 */
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

  /**
   * open session
   *
   * @param req InfluxOpenSessionReq
   * @return InfluxOpenSessionResp
   * @throws TException
   */
  @Override
  public InfluxOpenSessionResp openSession(InfluxOpenSessionReq req) throws TException {
    TSOpenSessionReq tsOpenSessionReq = InfluxReqAndRespUtils.convertOpenSessionReq(req);
    TSOpenSessionResp tsOpenSessionResp = clientRPCService.openSession(tsOpenSessionReq);
    return InfluxReqAndRespUtils.convertOpenSessionResp(tsOpenSessionResp);
  }

  /**
   * close session
   *
   * @param req InfluxCloseSessionReq
   * @return InfluxTSStatus
   */
  @Override
  public InfluxTSStatus closeSession(InfluxCloseSessionReq req) {
    TSCloseSessionReq tsCloseSessionReq = InfluxReqAndRespUtils.convertCloseSessionReq(req);
    TSStatus tsStatus = clientRPCService.closeSession(tsCloseSessionReq);
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus);
  }

  /**
   * Handling insert requests
   *
   * @param req InfluxWritePointsReq
   * @return InfluxTSStatus
   */
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

  /**
   * Create a database in the influxdb semantics
   *
   * @param req InfluxCreateDatabaseReq
   * @return InfluxTSStatus
   */
  @Override
  public InfluxTSStatus createDatabase(InfluxCreateDatabaseReq req) {
    TSStatus tsStatus =
        clientRPCService.setStorageGroup(req.sessionId, "root." + req.getDatabase());
    if (tsStatus.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      tsStatus.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      tsStatus.setMessage("Execute successfully");
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(tsStatus);
  }

  /**
   * Process query requests
   *
   * @param req InfluxQueryReq
   * @return InfluxQueryResultRsp
   * @throws TException
   */
  @Override
  public InfluxQueryResultRsp query(InfluxQueryReq req) throws TException {
    Statement queryStatement = InfluxDBStatementGenerator.generate(req.command);
    if (!(queryStatement instanceof InfluxQueryStatement)) {
      throw new IllegalArgumentException("not query sql");
    }
    ((InfluxQueryStatement) queryStatement).semanticCheck();

    return queryHandler.queryInfluxDB(
        req.database, (InfluxQueryStatement) queryStatement, req.sessionId);
  }

  /**
   * execute sql statement
   *
   * @param sql sql statement
   * @param sessionId session id
   * @return TSExecuteStatementResp
   */
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

  /** handle client exit, close session and resource */
  @Override
  public void handleClientExit() {
    clientRPCService.handleClientExit();
  }
}
