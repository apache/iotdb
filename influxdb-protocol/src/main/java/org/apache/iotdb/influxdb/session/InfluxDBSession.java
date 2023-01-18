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

package org.apache.iotdb.influxdb.session;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.influxdb.util.JacksonUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxCloseSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxCreateDatabaseReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxDBService;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxEndPoint;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxOpenSessionResp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxQueryResultRsp;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.InfluxWritePointsReq;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.session.SessionConnection.MSG_RECONNECTION_FAIL;

public class InfluxDBSession {
  private static final Logger logger = LoggerFactory.getLogger(InfluxDBSession.class);

  private TTransport transport;
  private InfluxDBService.Iface client;
  private List<InfluxEndPoint> endPointList = new ArrayList<>();
  private long sessionId;

  protected String username;
  protected String password;
  protected int fetchSize;
  protected ZoneId zoneId;
  protected InfluxEndPoint defaultEndPoint;
  protected int thriftDefaultBufferSize;
  protected int thriftMaxFrameSize;

  private boolean isClosed = true;
  protected boolean enableRPCCompression;
  protected int connectionTimeoutInMs;

  public InfluxDBSession(String host, int rpcPort, String username, String password) {
    this(
        host,
        rpcPort,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        ZoneId.systemDefault(),
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE);
  }

  public InfluxDBSession(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize) {
    this.defaultEndPoint = new InfluxEndPoint(host, rpcPort);
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public synchronized void open() throws IoTDBConnectionException {
    open(false, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }
    this.enableRPCCompression = enableRPCCompression;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    this.endPointList.add(defaultEndPoint);

    init();
    isClosed = false;
  }

  public void init() throws IoTDBConnectionException {
    RpcTransportFactory.setDefaultBufferCapacity(thriftDefaultBufferSize);
    RpcTransportFactory.setThriftMaxFrameSize(thriftMaxFrameSize);
    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              defaultEndPoint.getIp(), defaultEndPoint.getPort(), connectionTimeoutInMs);
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new IoTDBConnectionException(e);
    }

    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      client = new InfluxDBService.Client(new TCompactProtocol(transport));
    } else {
      client = new InfluxDBService.Client(new TBinaryProtocol(transport));
    }
    client = RpcUtils.newSynchronizedClient(client);

    InfluxOpenSessionReq openReq = new InfluxOpenSessionReq();
    openReq.setUsername(username);
    openReq.setPassword(password);
    openReq.setZoneId(zoneId.toString());

    try {
      InfluxOpenSessionResp openResp = client.openSession(openReq);
      RpcUtils.verifySuccess(openResp.getStatus());
      sessionId = openResp.getSessionId();

    } catch (Exception e) {
      transport.close();
      throw new IoTDBConnectionException(e);
    }
  }

  public void writePoints(InfluxWritePointsReq request)
      throws StatementExecutionException, IoTDBConnectionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.writePoints(request));
    } catch (TException e) {
      logger.error(e.getMessage());
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.writePoints(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public QueryResult query(InfluxQueryReq request)
      throws StatementExecutionException, IoTDBConnectionException {
    request.setSessionId(sessionId);
    try {
      InfluxQueryResultRsp tsQueryResultRsp = client.query(request);
      RpcUtils.verifySuccess(tsQueryResultRsp.status);
      return JacksonUtils.json2Bean(tsQueryResultRsp.resultJsonString, QueryResult.class);
    } catch (TException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          InfluxQueryResultRsp tsQueryResultRsp = client.query(request);
          RpcUtils.verifySuccess(tsQueryResultRsp.status);
          return JacksonUtils.json2Bean(tsQueryResultRsp.resultJsonString, QueryResult.class);
        } catch (TException e1) {
          throw new IoTDBConnectionException(e1);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public void createDatabase(InfluxCreateDatabaseReq request)
      throws StatementExecutionException, IoTDBConnectionException {
    request.setSessionId(sessionId);
    try {
      RpcUtils.verifySuccess(client.createDatabase(request));
    } catch (TException e) {
      e.printStackTrace();
      logger.error(e.getMessage());
      if (reconnect()) {
        try {
          request.setSessionId(sessionId);
          RpcUtils.verifySuccess(client.createDatabase(request));
        } catch (TException tException) {
          throw new IoTDBConnectionException(tException);
        }
      } else {
        throw new IoTDBConnectionException(MSG_RECONNECTION_FAIL);
      }
    }
  }

  public synchronized void close() {
    if (isClosed) {
      return;
    }
    try {
      client.closeSession(new InfluxCloseSessionReq(sessionId));
    } catch (TException e) {
      throw new InfluxDBException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      if (transport != null) {
        transport.close();
      }
      if (!isClosed) {
        isClosed = true;
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
          defaultEndPoint = endPointList.get(j);
          if (j == endPointList.size() - 1) {
            j = -1;
          }
          tryHostNum++;
          try {
            init();
            connectedSuccess = true;
          } catch (IoTDBConnectionException e) {
            logger.error("The current node may have been down {},try next node", defaultEndPoint);
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
}
