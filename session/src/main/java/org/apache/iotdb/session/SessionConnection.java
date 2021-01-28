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
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionConnection {

  private static final Logger logger = LoggerFactory.getLogger(SessionConnection.class);
  public static final String MSG_RECONNECTION_FAIL = "Fail to reconnect to server. Please check server status";
  private Session session;
  private TTransport transport;
  private TSIService.Iface client;
  private long sessionId;
  private long statementId;
  private ZoneId zoneId;
  private EndPoint endPoint;

  //TestOnly
  public SessionConnection() {
  }

  public SessionConnection(Session session, EndPoint endPoint, ZoneId zoneId)
      throws IoTDBConnectionException {
    this.session = session;
    this.endPoint = endPoint;
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    init(endPoint);
  }

  private void init(EndPoint endPoint) throws IoTDBConnectionException {
    RpcTransportFactory.setInitialBufferCapacity(session.initialBufferCapacity);
    RpcTransportFactory.setMaxLength(session.maxFrameSize);
    transport = RpcTransportFactory.INSTANCE.getTransport(
        new TSocket(endPoint.getIp(), endPoint.getPort(), session.connectionTimeoutInMs));

    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    if (session.enableRPCCompression) {
      client = new TSIService.Client(new TCompactProtocol(transport));
    } else {
      client = new TSIService.Client(new TBinaryProtocol(transport));
    }
    client = RpcUtils.newSynchronizedClient(client);

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername(session.username);
    openReq.setPassword(session.password);
    openReq.setZoneId(zoneId.toString());

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());

      if (Session.protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn("Protocol differ, Client version is {}}, but Server version is {}",
            Session.protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue());
        // less than 0.10
        if (openResp.getServerProtocolVersion().getValue() == 0) {
          throw new TException(String
              .format("Protocol not supported, Client version is %s, but Server version is %s",
                  Session.protocolVersion.getValue(),
                  openResp.getServerProtocolVersion().getValue()));
        }
      }

      sessionId = openResp.getSessionId();
      statementId = client.requestStatementId(sessionId);

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
      if (reconnect()) {
        try {
          req.setSessionId(sessionId);
          resp = client.setTimeZone(req);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
    RpcUtils.verifySuccess(resp);
    this.zoneId = ZoneId.of(zoneId);
  }

  protected String getTimeZone() {
    if (zoneId == null) {
      zoneId = ZoneId.systemDefault();
    }
    return zoneId.toString();
  }

  protected void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    try {
      RpcUtils.verifySuccessWithRedirection(client.setStorageGroup(sessionId, storageGroup));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.setStorageGroup(sessionId, storageGroup));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    try {
      RpcUtils.verifySuccessWithRedirection(client.deleteStorageGroups(sessionId, storageGroups));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.deleteStorageGroups(sessionId, storageGroups));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void createTimeseries(TSCreateTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createTimeseries(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void createMultiTimeseries(TSCreateMultiTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createMultiTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createMultiTimeseries(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected boolean checkTimeseriesExists(String path, long timeout)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = null;
    try {
      dataSet = executeQueryStatement(String.format("SHOW TIMESERIES %s", path), timeout);
      return dataSet.hasNext();
    } finally {
      if (dataSet != null) {
        dataSet.closeOperationHandle();
      }
    }
  }

  protected SessionDataSet executeQueryStatement(String sql, long timeout)
      throws StatementExecutionException, IoTDBConnectionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    execReq.setFetchSize(session.fetchSize);
    execReq.setTimeout(timeout);
    TSExecuteStatementResp execResp;
    try {
      execResp = client.executeQueryStatement(execReq);
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeQueryStatement(execReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet(sql, execResp.getColumns(), execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(), client, sessionId, execResp.queryDataSet,
        execResp.isIgnoreTimeStamp(), timeout);
  }


  protected void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          RpcUtils.verifySuccess(client.executeUpdateStatement(execReq).status);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    TSRawDataQueryReq execReq = new TSRawDataQueryReq(sessionId, paths, startTime, endTime,
        statementId);
    execReq.setFetchSize(session.fetchSize);
    TSExecuteStatementResp execResp;
    try {
      execResp = client.executeRawDataQuery(execReq);
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeRawDataQuery(execReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet("", execResp.getColumns(), execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(), client, sessionId, execResp.queryDataSet,
        execResp.isIgnoreTimeStamp());
  }

  protected void insertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertStringRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertStringRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertStringRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertStringRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertRecordsOfOneDevice(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertRecordsOfOneDevice(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertTablet(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertTablet(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void insertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirectionForInsertTablets(client.insertTablets(request), request);
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertTablets(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public void deleteData(TSDeleteDataReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.deleteData(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.deleteData(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(
            MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void testInsertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertStringRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertStringRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void testInsertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public void testInsertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertStringRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertStringRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public void testInsertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void testInsertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertTablet(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertTablet(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void testInsertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.testInsertTablets(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.testInsertTablets(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  private boolean reconnect() {
    boolean flag = false;
    for (int i = 1; i <= Config.RETRY_NUM; i++) {
      try {
        if (transport != null) {
          close();
          init(endPoint);
          flag = true;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(Config.RETRY_INTERVAL_MS);
        } catch (InterruptedException e1) {
          logger.error("reconnect is interrupted.", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }
}
