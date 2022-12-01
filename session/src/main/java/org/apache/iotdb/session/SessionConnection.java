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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.SessionTimeoutException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSAppendSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSDropSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetSystemStatusResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSOperationSyncWriteReq;
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetUsingTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.session.util.SystemStatus;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;

public class SessionConnection {

  private static final Logger logger = LoggerFactory.getLogger(SessionConnection.class);
  public static final String MSG_RECONNECTION_FAIL =
      "Fail to reconnect to server. Please check server status.";
  private Session session;
  private TTransport transport;
  private TSIService.Iface client;
  private long sessionId;
  private long statementId;
  private ZoneId zoneId;
  private EndPoint endPoint;
  private List<EndPoint> endPointList = new ArrayList<>();
  private boolean enableRedirect = false;
  public static final String VERSION = "version";
  public static final String AUTH_ENABLE_AUDIT = "enableAudit";

  // TestOnly
  public SessionConnection() {}

  public SessionConnection(Session session, EndPoint endPoint, ZoneId zoneId)
      throws IoTDBConnectionException {
    this.session = session;
    this.endPoint = endPoint;
    endPointList.add(endPoint);
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    try {
      init(endPoint);
    } catch (IoTDBConnectionException e) {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  public SessionConnection(Session session, ZoneId zoneId) throws IoTDBConnectionException {
    this.session = session;
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    this.endPointList = SessionUtils.parseSeedNodeUrls(session.nodeUrls);
    initClusterConn();
  }

  private void init(EndPoint endPoint) throws IoTDBConnectionException {
    RpcTransportFactory.setDefaultBufferCapacity(session.thriftDefaultBufferSize);
    RpcTransportFactory.setThriftMaxFrameSize(session.thriftMaxFrameSize);
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endPoint.getIp(), endPoint.getPort(), session.connectionTimeoutInMs);
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
    openReq.putToConfiguration(VERSION, session.version.toString());
    openReq.putToConfiguration(AUTH_ENABLE_AUDIT, String.valueOf(session.enableAudit));
    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());

      if (Session.protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn(
            "Protocol differ, Client version is {}}, but Server version is {}",
            Session.protocolVersion.getValue(),
            openResp.getServerProtocolVersion().getValue());
        // less than 0.10
        if (openResp.getServerProtocolVersion().getValue() == 0) {
          throw new TException(
              String.format(
                  "Protocol not supported, Client version is %s, but Server version is %s",
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

  private void initClusterConn() throws IoTDBConnectionException {
    for (EndPoint endPoint : endPointList) {
      try {
        session.defaultEndPoint = endPoint;
        init(endPoint);
      } catch (IoTDBConnectionException e) {
        if (!reconnect()) {
          logger.error("Cluster has no nodes to connect");
          throw new IoTDBConnectionException(logForReconnectionFailure());
        }
      }
      break;
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

  protected TSIService.Iface getClient() {
    return client;
  }

  protected SystemStatus getSystemStatus() throws IoTDBConnectionException {
    TSGetSystemStatusResp resp;
    try {
      resp = client.getSystemStatus(sessionId);
    } catch (TException e) {
      if (reconnect()) {
        try {
          resp = client.getSystemStatus(sessionId);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return SystemStatus.valueOf(resp.getSystemStatus());
  }

  protected void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, zoneId);
    TSStatus resp;
    try {
      resp = client.setTimeZone(req);
      verifySuccessWrapper(resp);
    } catch (TException e) {
      if (reconnect()) {
        try {
          req.setSessionId(sessionId);
          resp = client.setTimeZone(req);
          verifySuccessWrapper(resp);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
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
      verifySuccessWithRedirectionWrapper(client.setStorageGroup(sessionId, storageGroup));
    } catch (TException e) {
      if (reconnect()) {
        try {
          verifySuccessWrapper(client.setStorageGroup(sessionId, storageGroup));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    try {
      verifySuccessWithRedirectionWrapper(client.deleteStorageGroups(sessionId, storageGroups));
    } catch (TException e) {
      if (reconnect()) {
        try {
          verifySuccessWrapper(client.deleteStorageGroups(sessionId, storageGroups));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void createTimeseries(TSCreateTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.createTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.createTimeseries(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void createAlignedTimeseries(TSCreateAlignedTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.createAlignedTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.createAlignedTimeseries(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void createMultiTimeseries(TSCreateMultiTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.createMultiTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.createMultiTimeseries(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected boolean checkTimeseriesExists(String path, long timeout)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = null;
    try {
      try {
        dataSet = executeQueryStatement(String.format("SHOW TIMESERIES %s", path), timeout);
      } catch (RedirectException e) {
        throw new StatementExecutionException("need to redirect query, should not see this.", e);
      }
      return dataSet.hasNext();
    } finally {
      if (dataSet != null) {
        dataSet.closeOperationHandle();
      }
    }
  }

  protected SessionDataSet executeQueryStatement(String sql, long timeout)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    execReq.setFetchSize(session.fetchSize);
    execReq.setTimeout(timeout);
    TSExecuteStatementResp execResp;
    try {
      execReq.setEnableRedirectQuery(enableRedirect);
      execResp = client.executeQueryStatement(execReq);
      verifySuccessWithRedirectionWrapper(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeQueryStatement(execReq);
          verifySuccessWithRedirectionWrapper(execResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return new SessionDataSet(
        sql,
        execResp.getColumns(),
        execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(),
        statementId,
        client,
        sessionId,
        execResp.queryDataSet,
        execResp.isIgnoreTimeStamp(),
        timeout);
  }

  protected void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      execReq.setEnableRedirectQuery(enableRedirect);
      TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
      verifySuccessWrapper(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          verifySuccessWrapper(client.executeUpdateStatement(execReq).status);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSRawDataQueryReq execReq =
        new TSRawDataQueryReq(sessionId, paths, startTime, endTime, statementId);
    execReq.setFetchSize(session.fetchSize);
    TSExecuteStatementResp execResp;
    try {
      execReq.setEnableRedirectQuery(enableRedirect);
      execResp = client.executeRawDataQuery(execReq);
      verifySuccessWithRedirectionWrapper(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeRawDataQuery(execReq);
          verifySuccessWithRedirectionWrapper(execResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return new SessionDataSet(
        "",
        execResp.getColumns(),
        execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(),
        statementId,
        client,
        sessionId,
        execResp.queryDataSet,
        execResp.isIgnoreTimeStamp());
  }

  protected SessionDataSet executeLastDataQuery(List<String> paths, long time)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSLastDataQueryReq tsLastDataQueryReq =
        new TSLastDataQueryReq(sessionId, paths, time, statementId);
    tsLastDataQueryReq.setFetchSize(session.fetchSize);
    tsLastDataQueryReq.setEnableRedirectQuery(enableRedirect);
    TSExecuteStatementResp tsExecuteStatementResp;
    try {
      tsExecuteStatementResp = client.executeLastDataQuery(tsLastDataQueryReq);
      verifySuccessWithRedirectionWrapper(tsExecuteStatementResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          tsLastDataQueryReq.setSessionId(sessionId);
          tsLastDataQueryReq.setStatementId(statementId);
          tsExecuteStatementResp = client.executeLastDataQuery(tsLastDataQueryReq);
          verifySuccessWithRedirectionWrapper(tsExecuteStatementResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return new SessionDataSet(
        "",
        tsExecuteStatementResp.getColumns(),
        tsExecuteStatementResp.getDataTypeList(),
        tsExecuteStatementResp.columnNameIndexMap,
        tsExecuteStatementResp.getQueryId(),
        statementId,
        client,
        sessionId,
        tsExecuteStatementResp.queryDataSet,
        tsExecuteStatementResp.isIgnoreTimeStamp());
  }

  protected void insertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.insertRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  private void verifySuccessWrapper(TSStatus tsStatus)
      throws StatementExecutionException, TException {
    try {
      RpcUtils.verifySuccess(tsStatus);
    } catch (SessionTimeoutException e) {
      throw new TException(e);
    }
  }

  private void verifySuccessWithRedirectionWrapper(TSStatus tsStatus)
      throws StatementExecutionException, RedirectException, TException {
    try {
      RpcUtils.verifySuccessWithRedirection(tsStatus);
    } catch (SessionTimeoutException e) {
      throw new TException(e);
    }
  }

  private void verifySuccessWithRedirectionForMultiDevicesWrapper(
      TSStatus tsStatus, List<String> prefixPaths)
      throws StatementExecutionException, RedirectException, TException {
    try {
      RpcUtils.verifySuccessWithRedirectionForMultiDevices(tsStatus, prefixPaths);
    } catch (SessionTimeoutException e) {
      throw new TException(e);
    }
  }

  protected void insertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.insertStringRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertStringRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionForMultiDevicesWrapper(
          client.insertRecords(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionForMultiDevicesWrapper(
          client.insertStringRecords(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertStringRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.insertRecordsOfOneDevice(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertRecordsOfOneDevice(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.insertStringRecordsOfOneDevice(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertStringRecordsOfOneDevice(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.insertTablet(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertTablet(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionForMultiDevicesWrapper(
          client.insertTablets(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.insertTablets(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      verifySuccessWrapper(client.deleteTimeseries(sessionId, paths));
    } catch (TException e) {
      if (reconnect()) {
        try {
          verifySuccessWrapper(client.deleteTimeseries(sessionId, paths));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  public void deleteData(TSDeleteDataReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.deleteData(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.deleteData(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void testInsertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertStringRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertStringRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void testInsertRecord(TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertRecord(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertRecord(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  public void testInsertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertStringRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertStringRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  public void testInsertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertRecords(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertRecords(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void testInsertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertTablet(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertTablet(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void testInsertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.testInsertTablets(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.testInsertTablets(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  private boolean reconnect() {
    boolean connectedSuccess = false;
    Random random = new Random();
    for (int i = 1; i <= Config.RETRY_NUM; i++) {
      if (transport != null) {
        transport.close();
        int currHostIndex = random.nextInt(endPointList.size());
        int tryHostNum = 0;
        for (int j = currHostIndex; j < endPointList.size(); j++) {
          if (tryHostNum == endPointList.size()) {
            break;
          }
          session.defaultEndPoint = endPointList.get(j);
          this.endPoint = endPointList.get(j);
          if (j == endPointList.size() - 1) {
            j = -1;
          }
          tryHostNum++;
          try {
            init(endPoint);
            connectedSuccess = true;
          } catch (IoTDBConnectionException e) {
            logger.error("The current node may have been down {},try next node", endPoint);
            continue;
          }
          break;
        }
      }
      if (connectedSuccess) {
        break;
      }
    }
    return connectedSuccess;
  }

  protected void createSchemaTemplate(TSCreateSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.createSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.createSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void appendSchemaTemplate(TSAppendSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.appendSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.appendSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void pruneSchemaTemplate(TSPruneSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.pruneSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.pruneSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected TSQueryTemplateResp querySchemaTemplate(TSQueryTemplateReq req)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateResp execResp;
    try {
      execResp = client.querySchemaTemplate(req);
      verifySuccessWrapper(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execResp = client.querySchemaTemplate(req);
          verifySuccessWrapper(execResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return execResp;
  }

  protected void setSchemaTemplate(TSSetSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.setSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.setSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void unsetSchemaTemplate(TSUnsetSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.unsetSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.unsetSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void setUsingTemplate(TSSetUsingTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.setUsingTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.setUsingTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void deactivateTemplate(String tName, String pPath)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      verifySuccessWrapper(client.unsetUsingTemplate(sessionId, tName, pPath));
    } catch (TException e) {
      if (reconnect()) {
        try {
          verifySuccessWrapper(client.unsetUsingTemplate(sessionId, tName, pPath));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected void dropSchemaTemplate(TSDropSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWrapper(client.dropSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.dropSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void executeOperationSync(TSOperationSyncWriteReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      verifySuccessWithRedirectionWrapper(client.executeOperationSync(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          verifySuccessWrapper(client.executeOperationSync(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  protected TSBackupConfigurationResp getBackupConfiguration()
      throws StatementExecutionException, IoTDBConnectionException {
    TSBackupConfigurationResp execResp;
    try {
      execResp = client.getBackupConfiguration();
      verifySuccessWrapper(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execResp = client.getBackupConfiguration();
          verifySuccessWrapper(execResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return execResp;
  }

  public boolean isEnableRedirect() {
    return enableRedirect;
  }

  public void setEnableRedirect(boolean enableRedirect) {
    this.enableRedirect = enableRedirect;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(EndPoint endPoint) {
    this.endPoint = endPoint;
  }

  // error log for connection failure
  private String logForReconnectionFailure() {
    if (endPointList == null) {
      return MSG_RECONNECTION_FAIL;
    }
    StringJoiner urls = new StringJoiner(",");
    for (EndPoint endPoint : endPointList) {
      StringJoiner url = new StringJoiner(":");
      url.add(endPoint.getIp());
      url.add(String.valueOf(endPoint.getPort()));
      urls.add(url.toString());
    }
    return MSG_RECONNECTION_FAIL.concat(urls.toString());
  }

  public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {
    try {
      return client.fetchAllConnectionsInfo();
    } catch (TException e) {
      if (reconnect()) {
        try {
          return client.fetchAllConnectionsInfo();
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  @Override
  public String toString() {
    return "SessionConnection{" + " endPoint=" + endPoint + "}";
  }
}
