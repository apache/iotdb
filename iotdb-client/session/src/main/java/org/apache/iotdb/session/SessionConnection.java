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
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TCreateTimeseriesUsingSchemaTemplateReq;
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
import org.apache.iotdb.service.rpc.thrift.TSFastLastDataQueryForOneDeviceReq;
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
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@SuppressWarnings("java:S2142")
public class SessionConnection {

  private static final Logger logger = LoggerFactory.getLogger(SessionConnection.class);
  public static final String MSG_RECONNECTION_FAIL =
      "Fail to reconnect to server. Please check server status.";
  protected Session session;
  private TTransport transport;
  protected IClientRPCService.Iface client;
  private long sessionId;
  private long statementId;
  private ZoneId zoneId;
  private TEndPoint endPoint;
  private List<TEndPoint> endPointList = new ArrayList<>();
  private boolean enableRedirect = false;

  private final Supplier<List<TEndPoint>> availableNodes;

  private final int maxRetryCount;

  private final long retryIntervalInMs;

  private final String sqlDialect;

  private final String database;

  // ms is 1_000, us is 1_000_000, ns is 1_000_000_000
  private int timeFactor = 1_000;

  // TestOnly
  public SessionConnection() {
    availableNodes = Collections::emptyList;
    this.maxRetryCount = Math.max(0, SessionConfig.MAX_RETRY_COUNT);
    this.retryIntervalInMs = Math.max(0, SessionConfig.RETRY_INTERVAL_IN_MS);
    this.sqlDialect = "tree";
    database = null;
  }

  public SessionConnection(
      Session session,
      TEndPoint endPoint,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs,
      String sqlDialect,
      String database)
      throws IoTDBConnectionException {
    this.session = session;
    this.endPoint = endPoint;
    endPointList.add(endPoint);
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    this.availableNodes = availableNodes;
    this.maxRetryCount = Math.max(0, maxRetryCount);
    this.retryIntervalInMs = Math.max(0, retryIntervalInMs);
    this.sqlDialect = sqlDialect;
    this.database = database;
    try {
      init(endPoint, session.useSSL, session.trustStore, session.trustStorePwd);
    } catch (StatementExecutionException e) {
      throw new IoTDBConnectionException(e.getMessage());
    } catch (IoTDBConnectionException e) {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  public SessionConnection(
      Session session,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs,
      String sqlDialect,
      String database)
      throws IoTDBConnectionException {
    this.session = session;
    this.zoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
    this.endPointList = SessionUtils.parseSeedNodeUrls(session.nodeUrls);
    this.availableNodes = availableNodes;
    this.maxRetryCount = Math.max(0, maxRetryCount);
    this.retryIntervalInMs = Math.max(0, retryIntervalInMs);
    this.sqlDialect = sqlDialect;
    this.database = database;
    initClusterConn();
  }

  private void init(TEndPoint endPoint, boolean useSSL, String trustStore, String trustStorePwd)
      throws IoTDBConnectionException, StatementExecutionException {
    DeepCopyRpcTransportFactory.setDefaultBufferCapacity(session.thriftDefaultBufferSize);
    DeepCopyRpcTransportFactory.setThriftMaxFrameSize(session.thriftMaxFrameSize);
    try {
      if (useSSL) {
        transport =
            DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                endPoint.getIp(),
                endPoint.getPort(),
                session.connectionTimeoutInMs,
                trustStore,
                trustStorePwd);
      } else {
        transport =
            DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                // as there is a try-catch already, we do not need to use TSocket.wrap
                endPoint.getIp(), endPoint.getPort(), session.connectionTimeoutInMs);
      }
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
    openReq.putToConfiguration("sql_dialect", sqlDialect);
    if (database != null) {
      openReq.putToConfiguration("db", database);
    }

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());
      this.timeFactor = RpcUtils.getTimeFactor(openResp);
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

    } catch (StatementExecutionException e) {
      transport.close();
      throw e;
    } catch (Exception e) {
      transport.close();
      throw new IoTDBConnectionException(e);
    }
  }

  @SuppressWarnings({"squid:S1751"}) // Loops with at most one iteration should be refactored
  private void initClusterConn() throws IoTDBConnectionException {
    for (TEndPoint tEndPoint : endPointList) {
      try {
        session.defaultEndPoint = tEndPoint;
        init(tEndPoint, session.useSSL, session.trustStore, session.trustStorePwd);
      } catch (IoTDBConnectionException e) {
        if (!reconnect()) {
          logger.error("Cluster has no nodes to connect");
          throw new IoTDBConnectionException(logForReconnectionFailure());
        }
      } catch (StatementExecutionException e) {
        throw new IoTDBConnectionException(e.getMessage());
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
        session.fetchSize,
        zoneId,
        timeFactor,
        execResp.isSetTableModel() && execResp.isTableModel(),
        execResp.getColumnIndex2TsBlockColumnIndexList());
  }

  protected void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {

    TSExecuteStatementReq request = new TSExecuteStatementReq(sessionId, sql, statementId);

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = executeNonQueryStatementInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        RpcUtils.verifySuccess(status);
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus executeNonQueryStatementInternal(TSExecuteStatementReq request)
      throws TException {
    request.setSessionId(sessionId);
    request.setStatementId(statementId);
    TSExecuteStatementResp resp = client.executeUpdateStatementV2(request);
    if (resp.isSetDatabase()) {
      session.changeDatabase(resp.getDatabase());
    }
    return resp.status;
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
        execResp.moreData,
        zoneId,
        timeFactor,
        execResp.isSetTableModel() && execResp.isTableModel(),
        execResp.getColumnIndex2TsBlockColumnIndexList());
  }

  protected Pair<SessionDataSet, TEndPoint> executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    TSFastLastDataQueryForOneDeviceReq req =
        new TSFastLastDataQueryForOneDeviceReq(sessionId, db, device, sensors, statementId);
    req.setFetchSize(session.fetchSize);
    req.setEnableRedirectQuery(enableRedirect);
    req.setLegalPathNodes(isLegalPathNodes);
    req.setTimeout(timeOut);
    TSExecuteStatementResp tsExecuteStatementResp = null;
    TEndPoint redirectedEndPoint = null;
    try {
      tsExecuteStatementResp = client.executeFastLastDataQueryForOneDeviceV2(req);
      RpcUtils.verifySuccessWithRedirection(tsExecuteStatementResp.getStatus());
    } catch (RedirectException e) {
      redirectedEndPoint = e.getEndPoint();
    } catch (TException e) {
      if (reconnect()) {
        try {
          req.setSessionId(sessionId);
          req.setStatementId(statementId);
          tsExecuteStatementResp = client.executeFastLastDataQueryForOneDeviceV2(req);
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(logForReconnectionFailure());
      }
    }

    RpcUtils.verifySuccess(tsExecuteStatementResp.getStatus());
    return new Pair<>(
        new SessionDataSet(
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
            tsExecuteStatementResp.moreData,
            zoneId,
            timeFactor,
            tsExecuteStatementResp.isSetTableModel() && tsExecuteStatementResp.isTableModel(),
            tsExecuteStatementResp.getColumnIndex2TsBlockColumnIndexList()),
        redirectedEndPoint);
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
        tsExecuteStatementResp.moreData,
        zoneId,
        timeFactor,
        tsExecuteStatementResp.isSetTableModel() && tsExecuteStatementResp.isTableModel(),
        tsExecuteStatementResp.getColumnIndex2TsBlockColumnIndexList());
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
        tsExecuteStatementResp.moreData,
        zoneId,
        timeFactor,
        tsExecuteStatementResp.isSetTableModel() && tsExecuteStatementResp.isTableModel(),
        tsExecuteStatementResp.getColumnIndex2TsBlockColumnIndexList());
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
    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertRecordInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirection(status);
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertRecordInternal(TSInsertRecordReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertRecord(request);
  }

  protected void insertRecord(TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertRecordInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirection(status);
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertRecordInternal(TSInsertStringRecordReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertStringRecord(request);
  }

  protected void insertRecords(TSInsertRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertRecordsInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirectionForMultiDevices(status, request.getPrefixPaths());
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertRecordsInternal(TSInsertRecordsReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertRecords(request);
  }

  protected void insertRecords(TSInsertStringRecordsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertRecordsInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirectionForMultiDevices(status, request.getPrefixPaths());
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertRecordsInternal(TSInsertStringRecordsReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertStringRecords(request);
  }

  protected void insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertRecordsOfOneDeviceInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirection(status);
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertRecordsOfOneDeviceInternal(TSInsertRecordsOfOneDeviceReq request)
      throws TException {
    request.setSessionId(sessionId);
    return client.insertRecordsOfOneDevice(request);
  }

  protected void insertStringRecordsOfOneDevice(TSInsertStringRecordsOfOneDeviceReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertStringRecordsOfOneDeviceInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirection(status);
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertStringRecordsOfOneDeviceInternal(
      TSInsertStringRecordsOfOneDeviceReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertStringRecordsOfOneDevice(request);
  }

  protected void withRetry(TFunction<TSStatus> function)
      throws StatementExecutionException, RedirectException, IoTDBConnectionException {
    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = function.run();
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirection(status);
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  protected void insertTablet(TSInsertTabletReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {
    withRetry(() -> insertTabletInternal(request));
  }

  private TSStatus insertTabletInternal(TSInsertTabletReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertTablet(request);
  }

  protected void insertTablets(TSInsertTabletsReq request)
      throws IoTDBConnectionException, StatementExecutionException, RedirectException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = insertTabletsInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        if (i == 0) {
          // first time succeed, take account for redirection info
          RpcUtils.verifySuccessWithRedirectionForMultiDevices(status, request.getPrefixPaths());
        } else {
          // if it's retry, just ignore redirection info
          RpcUtils.verifySuccess(status);
        }
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus insertTabletsInternal(TSInsertTabletsReq request) throws TException {
    request.setSessionId(sessionId);
    return client.insertTablets(request);
  }

  protected void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = client.deleteTimeseries(sessionId, paths);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        RpcUtils.verifySuccess(status);
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  public void deleteData(TSDeleteDataReq request)
      throws IoTDBConnectionException, StatementExecutionException {

    TException lastTException = null;
    TSStatus status = null;
    for (int i = 0; i <= maxRetryCount; i++) {
      if (i > 0) {
        // re-init the TException and TSStatus
        lastTException = null;
        status = null;
        // not first time, we need to sleep and then reconnect
        try {
          TimeUnit.MILLISECONDS.sleep(retryIntervalInMs);
        } catch (InterruptedException e) {
          // just ignore
        }
        if (!reconnect()) {
          // reconnect failed, just continue to make another retry.
          continue;
        }
      }
      try {
        status = deleteDataInternal(request);
        // need retry
        if (status.isSetNeedRetry() && status.isNeedRetry()) {
          continue;
        }
        // succeed or don't need to retry
        RpcUtils.verifySuccess(status);
        return;
      } catch (TException e) {
        // all network exception need retry until reaching maxRetryCount
        lastTException = e;
      }
    }

    if (status != null) {
      RpcUtils.verifySuccess(status);
    } else if (lastTException != null) {
      throw new IoTDBConnectionException(lastTException);
    } else {
      throw new IoTDBConnectionException(logForReconnectionFailure());
    }
  }

  private TSStatus deleteDataInternal(TSDeleteDataReq request) throws TException {
    request.setSessionId(sessionId);
    return client.deleteData(request);
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

  @SuppressWarnings({
    "squid:S3776"
  }) // ignore Cognitive Complexity of methods should not be too high
  private boolean reconnect() {
    boolean connectedSuccess = false;
    SecureRandom random = new SecureRandom();
    for (int i = 1; i <= SessionConfig.RETRY_NUM; i++) {
      if (transport != null) {
        transport.close();
        endPointList = availableNodes.get();
        int currHostIndex = random.nextInt(endPointList.size());
        int tryHostNum = 0;
        for (int j = currHostIndex; j < endPointList.size(); j++) {
          if (tryHostNum == endPointList.size()) {
            break;
          }
          this.endPoint = endPointList.get(j);
          if (j == endPointList.size() - 1) {
            j = -1;
          }
          tryHostNum++;
          try {
            init(endPoint, session.useSSL, session.trustStore, session.trustStorePwd);
            connectedSuccess = true;
          } catch (IoTDBConnectionException e) {
            logger.warn("The current node may have been down {}, try next node", endPoint);
            continue;
          } catch (StatementExecutionException e) {
            logger.warn("login in failed, because {}", e.getMessage());
          }
          break;
        }
      }
      if (connectedSuccess) {
        // remove the broken end point
        session.removeBrokenSessionConnection(this);
        session.defaultEndPoint = this.endPoint;
        session.defaultSessionConnection = this;
        if (session.endPointToSessionConnection == null) {
          session.endPointToSessionConnection = new ConcurrentHashMap<>();
        }
        session.endPointToSessionConnection.put(session.defaultEndPoint, this);
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

  protected void createTimeseriesUsingSchemaTemplate(
      TCreateTimeseriesUsingSchemaTemplateReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createTimeseriesUsingSchemaTemplate(request));
    } catch (TException e) {
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createTimeseriesUsingSchemaTemplate(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
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
    for (TEndPoint end : endPointList) {
      StringJoiner url = new StringJoiner(":");
      url.add(end.getIp());
      url.add(String.valueOf(end.getPort()));
      urls.add(url.toString());
    }
    return MSG_RECONNECTION_FAIL.concat(urls.toString());
  }

  @Override
  public String toString() {
    return "SessionConnection{" + " endPoint=" + endPoint + "}";
  }

  private interface TFunction<T> {
    T run() throws TException;
  }
}
