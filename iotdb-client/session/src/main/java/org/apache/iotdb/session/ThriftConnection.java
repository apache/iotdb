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
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

public class ThriftConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftConnection.class);

  protected final TEndPoint endPoint;

  protected final int thriftDefaultBufferSize;

  protected final int thriftMaxFrameSize;

  protected final int connectionTimeoutInMs;

  protected TTransport transport;
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
      String username,
      String password,
      boolean enableRPCCompression,
      ZoneId zoneId,
      String version)
      throws IoTDBConnectionException {
    DeepCopyRpcTransportFactory.setDefaultBufferCapacity(thriftDefaultBufferSize);
    DeepCopyRpcTransportFactory.setThriftMaxFrameSize(thriftMaxFrameSize);
    try {
      if (useSSL) {
        transport =
            DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                endPoint.getIp(),
                endPoint.getPort(),
                connectionTimeoutInMs,
                trustStore,
                trustStorePwd);
      } else {
        transport =
            DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                // as there is a try-catch already, we do not need to use TSocket.wrap
                endPoint.getIp(), endPoint.getPort(), connectionTimeoutInMs);
      }
      if (!transport.isOpen()) {
        transport.open();
      }
      this.zoneId = zoneId;
    } catch (TTransportException e) {
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

  public void close() {
    if (transport != null && transport.isOpen()) {
      try {
        if (client != null) {
          client.closeSession(new TSCloseSessionReq(sessionId));
        }
      } catch (TException e) {
        LOGGER.warn("Closing Session-{} with {} failed.", sessionId, endPoint);
        if (transport.isOpen()) {
          transport.close();
        }
      }
    }
  }
}
