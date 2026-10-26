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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TElasticFramedTransport;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.session.i18n.SessionMessages;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThriftConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftConnection.class);

  protected final TEndPoint endPoint;

  protected final int thriftDefaultBufferSize;

  protected final int thriftMaxFrameSize;

  protected final int connectionTimeoutInMs;

  protected volatile TTransport transport;
  private final Object transportLock = new Object();
  private final AtomicBoolean disposed = new AtomicBoolean();
  private volatile boolean closed;
  protected IClientRPCService.Iface client;
  protected long sessionId;
  protected long statementId;
  private ZoneId zoneId;

  private int timeFactor;

  public ThriftConnection(
      TEndPoint endPoint,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      int connectionTimeoutInMs) {
    this.endPoint = endPoint;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
  }

  public void init(
      boolean useSSL,
      String trustStore,
      String trustStorePwd,
      String keyStore,
      String keyStorePwd,
      String sslProtocol,
      String username,
      String password,
      boolean enableRPCCompression,
      ZoneId zoneId,
      String version)
      throws IoTDBConnectionException {
    checkNotClosed();
    try {
      TTransport prepared =
          createTransport(useSSL, trustStore, trustStorePwd, keyStore, keyStorePwd, sslProtocol);
      boolean published;
      synchronized (transportLock) {
        published = !closed;
        if (published) {
          transport = prepared;
        }
      }
      if (!published) {
        prepared.close();
        throw new IoTDBConnectionException(SessionMessages.SESSION_NOT_OPEN);
      }
      if (!prepared.isOpen()) {
        prepared.open();
      }
      checkNotClosed();
      this.zoneId = zoneId;
    } catch (TTransportException e) {
      close();
      throw new IoTDBConnectionException(e);
    }

    if (enableRPCCompression) {
      client = new IClientRPCService.Client(new TCompactProtocol(transport));
    } else {
      client = new IClientRPCService.Client(new TBinaryProtocol(transport));
    }
    client = RpcUtils.newSynchronizedClient(client);

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername(username);
    openReq.setPassword(password);
    openReq.setZoneId(zoneId.toString());
    openReq.putToConfiguration("version", version);

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());

      this.timeFactor = RpcUtils.getTimeFactor(openResp);

      if (Session.protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        LOGGER.warn(
            SessionMessages.LOG_PROTOCOL_DIFFER_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_9C8EC583,
            Session.protocolVersion.getValue(),
            openResp.getServerProtocolVersion().getValue());
        // less than 0.10
        if (openResp.getServerProtocolVersion().getValue() == 0) {
          throw new TException(
              String.format(
                  SessionMessages
                      .EXCEPTION_PROTOCOL_NOT_SUPPORTED_CLIENT_VERSION_ARG_BUT_SERVER_VERSION_ARG_53F892DC,
                  Session.protocolVersion.getValue(),
                  openResp.getServerProtocolVersion().getValue()));
        }
      }

      sessionId = openResp.getSessionId();
      statementId = client.requestStatementId(sessionId);

    } catch (Exception e) {
      close();
      throw new IoTDBConnectionException(e);
    }
  }

  protected SessionDataSet executeQueryStatement(String sql, long timeout, int fetchSize)
      throws StatementExecutionException, IoTDBConnectionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    execReq.setFetchSize(fetchSize);
    execReq.setTimeout(timeout);
    TSExecuteStatementResp execResp;
    try {
      execResp = client.executeQueryStatementV2(execReq);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
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
        fetchSize,
        zoneId,
        timeFactor,
        execResp.isSetTableModel() && execResp.isTableModel(),
        execResp.getColumnIndex2TsBlockColumnIndexList());
  }

  /** Create outside the publication lock: TLS setup itself can perform network I/O. */
  protected TTransport createTransport(
      boolean useSSL,
      String trustStore,
      String trustStorePwd,
      String keyStore,
      String keyStorePwd,
      String sslProtocol)
      throws TTransportException {
    DeepCopyRpcTransportFactory factory =
        DeepCopyRpcTransportFactory.getInstance(thriftDefaultBufferSize, thriftMaxFrameSize);
    return useSSL
        ? factory.getTransport(
            endPoint.getIp(),
            endPoint.getPort(),
            connectionTimeoutInMs,
            trustStore,
            trustStorePwd,
            keyStore,
            keyStorePwd,
            sslProtocol)
        : factory.getTransport(endPoint.getIp(), endPoint.getPort(), connectionTimeoutInMs);
  }

  private void checkNotClosed() throws IoTDBConnectionException {
    if (closed) {
      throw new IoTDBConnectionException(SessionMessages.SESSION_NOT_OPEN);
    }
  }

  /**
   * Abort socket I/O without waiting for the synchronized RPC client or releasing buffers still
   * owned by the refresh thread. Closing the Socket directly also prevents TSocket.open() from
   * creating a replacement socket if closure races with the start of TCP connect.
   */
  void abort() {
    TTransport current;
    synchronized (transportLock) {
      closed = true;
      current = transport;
    }
    if (current == null) {
      return;
    }
    TTransport underlying =
        current instanceof TElasticFramedTransport
            ? ((TElasticFramedTransport) current).getSocket()
            : current;
    if (underlying instanceof TSocket) {
      Socket socket = ((TSocket) underlying).getSocket();
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          LOGGER.warn(SessionMessages.CLOSING_SESSION_FAILED, sessionId, endPoint, e);
        }
      }
    } else {
      underlying.close();
    }
  }

  /** Release the transport after its refresh owner has finished using the buffers. */
  public void close() {
    abort();
    if (disposed.compareAndSet(false, true) && transport != null) {
      // Closing the transport also releases the server-side session through handleClientExit().
      transport.close();
    }
  }
}
