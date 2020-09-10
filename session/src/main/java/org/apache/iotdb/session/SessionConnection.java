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

package org.apache.iotdb.session;

import java.time.ZoneId;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionConnection {

  private static final Logger logger = LoggerFactory.getLogger(SessionConnection.class);
  private Session session;
  private TTransport transport;
  private TSIService.Iface client;
  private long sessionId;
  private long statementId;
  private ZoneId zoneId;

  public SessionConnection(Session session, EndPoint endPoint) throws IoTDBConnectionException {
    this.session = session;
    init(endPoint);
  }

  private void init(EndPoint endPoint) throws IoTDBConnectionException {
    transport = new TFastFramedTransport(
        new TSocket(endPoint.getIp(), endPoint.getPort(), session.connectionTimeoutInMs));

    if (!transport.isOpen()) {
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IoTDBConnectionException(e);
      }
    }

    if (session.enableRPCCompression) {
      client = new TSIService.Client(new TCompactProtocol(transport));
    } else {
      client = new TSIService.Client(new TBinaryProtocol(transport));
    }

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername(session.username);
    openReq.setPassword(session.password);

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());

      if (session.protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn("Protocol differ, Client version is {}}, but Server version is {}",
            session.protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue());
        if (openResp.getServerProtocolVersion().getValue() == 0) {// less than 0.10
          throw new TException(String
              .format("Protocol not supported, Client version is %s, but Server version is %s",
                  session.protocolVersion.getValue(),
                  openResp.getServerProtocolVersion().getValue()));
        }
      }

      sessionId = openResp.getSessionId();
      statementId = client.requestStatementId(sessionId);

      if (zoneId != null) {
        setTimeZone(zoneId.toString());
      } else {
        zoneId = ZoneId.of(getTimeZone());
      }

    } catch (Exception e) {
      transport.close();
      throw new IoTDBConnectionException(e);
    }
  }


  public void close() throws IoTDBConnectionException {
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      client.closeSession(req);
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  protected void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, zoneId);
    TSStatus resp;
    try {
      resp = client.setTimeZone(req);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
    RpcUtils.verifySuccess(resp);
    this.zoneId = ZoneId.of(zoneId);
  }

  protected String getTimeZone()
      throws StatementExecutionException, IoTDBConnectionException {
    if (zoneId != null) {
      return zoneId.toString();
    }
    TSGetTimeZoneResp resp;
    try {
      resp = client.getTimeZone(sessionId);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
    RpcUtils.verifySuccess(resp.getStatus());
    return resp.getTimeZone();
  }

  protected void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    try {
      RpcUtils.verifySuccessWithRedirection(client.setStorageGroup(sessionId, storageGroup));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    try {
      RpcUtils.verifySuccessWithRedirection(client.deleteStorageGroups(sessionId, storageGroups));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void createTimeseries(TSCreateTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createTimeseries(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void createMultiTimeseries(TSCreateMultiTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createMultiTimeseries(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = executeQueryStatement(String.format("SHOW TIMESERIES %s", path));
    boolean result = dataSet.hasNext();
    dataSet.closeOperationHandle();
    return result;
  }

  protected SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    execReq.setFetchSize(session.fetchSize);
    TSExecuteStatementResp execResp;
    try {
      execResp = client.executeQueryStatement(execReq);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet(sql, execResp.getColumns(), execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(), client, sessionId, execResp.queryDataSet,
        execResp.isIgnoreTimeStamp());
  }

  protected void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertStringRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertStringRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertTablet(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void insertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertTablets(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void deleteData(TSDeleteDataReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.deleteData(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void testInsertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertStringRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void testInsertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void testInsertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertStringRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void testInsertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void testInsertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertTablet(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  protected void testInsertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertTablets(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

}
