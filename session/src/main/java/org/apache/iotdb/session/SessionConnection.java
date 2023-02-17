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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSAggregationQueryReq;
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
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.session.util.SessionUtils;

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
  private IClientRPCService.Iface client;
  private long sessionId;
  private long statementId;
  private ZoneId zoneId;
  private TEndPoint endPoint;
  private List<TEndPoint> endPointList = new ArrayList<>();
  private boolean enableRedirect = false;

  // TestOnly
  public SessionConnection() {}

  public SessionConnection(Session session, TEndPoint endPoint, ZoneId zoneId)
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

  private void init(TEndPoint endPoint) throws IoTDBConnectionException {
    RpcTransportFactory.setDefaultBufferCapacity(session.thriftDefaultBufferSize);
    RpcTransportFactory.setThriftMaxFrameSize(session.thriftMaxFrameSize);
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endPoint.getIp(), endPoint.getPort(), session.connectionTimeoutInMs);
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    if (session.enableRPCCompression) {
      client = new IClientRPCService.Client(new TCompactProtocol(transport));
    } else {
      client = new IClientRPCService.Client(new TBinaryProtocol(transport));
    }
    client = RpcUtils.newSynchronizedClient(client);

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername(session.username);
    openReq.setPassword(session.password);
    openReq.setZoneId(zoneId.toString());
    openReq.putToConfiguration("version", session.version.toString());

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
    for (TEndPoint endPoint : endPointList) {
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

  protected IClientRPCService.Iface getClient() {
    return client;
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    RpcUtils.verifySuccess(resp);
    setTimeZoneOfSession(zoneId);
  }

  protected void setTimeZoneOfSession(String zoneId) {
    this.zoneId = ZoneId.of(zoneId);
  }

  protected String getTimeZone() {
    if (zoneId == null) {
      zoneId = ZoneId.systemDefault();
    }
    return zoneId.toString();
  }

  protected void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.setStorageGroup(sessionId, storageGroup));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.setStorageGroup(sessionId, storageGroup));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.deleteStorageGroups(sessionId, storageGroups));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.deleteStorageGroups(sessionId, storageGroups));
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void createAlignedTimeseries(TSCreateAlignedTimeseriesReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createAlignedTimeseries(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createAlignedTimeseries(request));
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
      execResp = client.executeQueryStatementV2(execReq);
      RpcUtils.verifySuccessWithRedirection(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeQueryStatementV2(execReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet(
        sql,
        execResp.getColumns(),
        execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(),
        statementId,
        client,
        sessionId,
        execResp.queryResult,
        execResp.isIgnoreTimeStamp(),
        timeout,
        execResp.moreData,
        session.fetchSize);
  }

  protected void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      execReq.setEnableRedirectQuery(enableRedirect);
      TSExecuteStatementResp execResp = client.executeUpdateStatementV2(execReq);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          RpcUtils.verifySuccess(client.executeUpdateStatementV2(execReq).status);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected SessionDataSet executeRawDataQuery(
      List<String> paths, long startTime, long endTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSRawDataQueryReq execReq =
        new TSRawDataQueryReq(sessionId, paths, startTime, endTime, statementId);
    execReq.setFetchSize(session.fetchSize);
    execReq.setTimeout(timeOut);
    TSExecuteStatementResp execResp;
    try {
      execReq.setEnableRedirectQuery(enableRedirect);
      execResp = client.executeRawDataQueryV2(execReq);
      RpcUtils.verifySuccessWithRedirection(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execReq.setSessionId(sessionId);
          execReq.setStatementId(statementId);
          execResp = client.executeRawDataQueryV2(execReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet(
        "",
        execResp.getColumns(),
        execResp.getDataTypeList(),
        execResp.columnNameIndexMap,
        execResp.getQueryId(),
        statementId,
        client,
        sessionId,
        execResp.queryResult,
        execResp.isIgnoreTimeStamp(),
        execResp.moreData);
  }

  protected SessionDataSet executeLastDataQuery(List<String> paths, long time, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSLastDataQueryReq tsLastDataQueryReq =
        new TSLastDataQueryReq(sessionId, paths, time, statementId);
    tsLastDataQueryReq.setFetchSize(session.fetchSize);
    tsLastDataQueryReq.setEnableRedirectQuery(enableRedirect);
    tsLastDataQueryReq.setTimeout(timeOut);
    TSExecuteStatementResp tsExecuteStatementResp;
    try {
      tsExecuteStatementResp = client.executeLastDataQueryV2(tsLastDataQueryReq);
      RpcUtils.verifySuccessWithRedirection(tsExecuteStatementResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          tsLastDataQueryReq.setSessionId(sessionId);
          tsLastDataQueryReq.setStatementId(statementId);
          tsExecuteStatementResp = client.executeLastDataQueryV2(tsLastDataQueryReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(tsExecuteStatementResp.getStatus());
    return new SessionDataSet(
        "",
        tsExecuteStatementResp.getColumns(),
        tsExecuteStatementResp.getDataTypeList(),
        tsExecuteStatementResp.columnNameIndexMap,
        tsExecuteStatementResp.getQueryId(),
        statementId,
        client,
        sessionId,
        tsExecuteStatementResp.queryResult,
        tsExecuteStatementResp.isIgnoreTimeStamp(),
        tsExecuteStatementResp.moreData);
  }

  protected SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSAggregationQueryReq req = createAggregationQueryReq(paths, aggregations);
    return executeAggregationQuery(req);
  }

  protected SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSAggregationQueryReq req = createAggregationQueryReq(paths, aggregations);
    req.setStartTime(startTime);
    req.setEndTime(endTime);
    return executeAggregationQuery(req);
  }

  protected SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSAggregationQueryReq req = createAggregationQueryReq(paths, aggregations);
    req.setStartTime(startTime);
    req.setEndTime(endTime);
    req.setInterval(interval);
    return executeAggregationQuery(req);
  }

  protected SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval,
      long slidingStep)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSAggregationQueryReq req = createAggregationQueryReq(paths, aggregations);
    req.setStartTime(startTime);
    req.setEndTime(endTime);
    req.setInterval(interval);
    req.setSlidingStep(slidingStep);
    return executeAggregationQuery(req);
  }

  private SessionDataSet executeAggregationQuery(TSAggregationQueryReq tsAggregationQueryReq)
      throws StatementExecutionException, IoTDBConnectionException, RedirectException {
    TSExecuteStatementResp tsExecuteStatementResp;
    try {
      tsExecuteStatementResp = client.executeAggregationQueryV2(tsAggregationQueryReq);
      RpcUtils.verifySuccessWithRedirection(tsExecuteStatementResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          tsAggregationQueryReq.setSessionId(sessionId);
          tsAggregationQueryReq.setStatementId(statementId);
          tsExecuteStatementResp = client.executeAggregationQuery(tsAggregationQueryReq);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(tsExecuteStatementResp.getStatus());
    return new SessionDataSet(
        "",
        tsExecuteStatementResp.getColumns(),
        tsExecuteStatementResp.getDataTypeList(),
        tsExecuteStatementResp.columnNameIndexMap,
        tsExecuteStatementResp.getQueryId(),
        statementId,
        client,
        sessionId,
        tsExecuteStatementResp.queryResult,
        tsExecuteStatementResp.isIgnoreTimeStamp(),
        tsExecuteStatementResp.moreData);
  }

  private TSAggregationQueryReq createAggregationQueryReq(
      List<String> paths, List<TAggregationType> aggregations) {
    TSAggregationQueryReq req =
        new TSAggregationQueryReq(sessionId, statementId, paths, aggregations);
    req.setFetchSize(session.getFetchSize());
    req.setTimeout(session.getQueryTimeout());
    return req;
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirectionForMultiDevices(
          client.insertRecords(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertRecords(request));
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
      RpcUtils.verifySuccessWithRedirectionForMultiDevices(
          client.insertStringRecords(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertStringRecords(request));
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirection(client.insertStringRecordsOfOneDevice(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertStringRecordsOfOneDevice(request));
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void insertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccessWithRedirectionForMultiDevices(
          client.insertTablets(request), request.getPrefixPaths());
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.insertTablets(request));
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
      RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
    } catch (TException e) {
      if (reconnect()) {
        try {
          RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
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
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  private boolean reconnect() {
    boolean connectedSuccess = false;
    Random random = new Random();
    for (int i = 1; i <= SessionConfig.RETRY_NUM; i++) {
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
      RpcUtils.verifySuccess(client.createSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createSchemaTemplate(request));
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
      RpcUtils.verifySuccess(client.appendSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.appendSchemaTemplate(request));
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
      RpcUtils.verifySuccess(client.pruneSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.pruneSchemaTemplate(request));
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
    req.setSessionId(sessionId);
    try {
      execResp = client.querySchemaTemplate(req);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execResp = client.querySchemaTemplate(req);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return execResp;
  }

  protected void setSchemaTemplate(TSSetSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.setSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.setSchemaTemplate(request));
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
      RpcUtils.verifySuccess(client.unsetSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.unsetSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected void dropSchemaTemplate(TSDropSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.dropSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.dropSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
  }

  protected TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    TSBackupConfigurationResp execResp;
    try {
      execResp = client.getBackupConfiguration();
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      if (reconnect()) {
        try {
          execResp = client.getBackupConfiguration();
          RpcUtils.verifySuccess(execResp.getStatus());
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }
    return execResp;
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

  public boolean isEnableRedirect() {
    return enableRedirect;
  }

  public void setEnableRedirect(boolean enableRedirect) {
    this.enableRedirect = enableRedirect;
  }

  public TEndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(TEndPoint endPoint) {
    this.endPoint = endPoint;
  }

  // error log for connection failure
  private String logForReconnectionFailure() {
    if (endPointList == null) {
      return MSG_RECONNECTION_FAIL;
    }
    StringJoiner urls = new StringJoiner(",");
    for (TEndPoint endPoint : endPointList) {
      StringJoiner url = new StringJoiner(":");
      url.add(endPoint.getIp());
      url.add(String.valueOf(endPoint.getPort()));
      urls.add(url.toString());
    }
    return MSG_RECONNECTION_FAIL.concat(urls.toString());
  }

  @Override
  public String toString() {
    return "SessionConnection{" + " endPoint=" + endPoint + "}";
  }
}
