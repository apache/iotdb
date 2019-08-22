/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.jdbc.Utils;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.service.rpc.thrift.TSBatchInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {

  private Logger logger = LoggerFactory.getLogger(Session.class);
  private String host;
  private int port;
  private String username;
  private String password;
  private final List<TSProtocolVersion> supportedProtocols = new LinkedList<>();
  public TSIService.Iface client = null;
  public TS_SessionHandle sessionHandle = null;
  private TSocket transport;
  private boolean isClosed = true;
  private ZoneId zoneId;

  public Session(String host, int port) {
    this(host, port, Config.DEFAULT_USER, Config.DEFALUT_PASSWORD);
  }

  public Session(String host, String port, String username, String password) {
    this(host, Integer.parseInt(port), username, password);
  }

  public Session(String host, int port, String username, String password) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    supportedProtocols.add(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1);
  }

  public void open() {
    open(false, 0);
  }

  public void open(boolean enableRPCCompression, int connectionTimeoutInMs) {
    transport = new TSocket(host, port, connectionTimeoutInMs);
    if (!transport.isOpen()) {
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IoTDBSessionException(e);
      }
    }

    if(enableRPCCompression) {
      client = new TSIService.Client(new TCompactProtocol(transport));
    }
    else {
      client = new TSIService.Client(new TBinaryProtocol(transport));
    }

    TSOpenSessionReq openReq = new TSOpenSessionReq(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1);
    openReq.setUsername(username);
    openReq.setPassword(password);

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      // validate connection
      try {
        Utils.verifySuccess(openResp.getStatus());
      } catch (IoTDBSQLException e) {
        transport.close();
        throw e;
      }
      if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
        throw new TException("Unsupported IoTDB protocol");
      }
      sessionHandle = openResp.getSessionHandle();

      if (zoneId != null) {
        setTimeZone(zoneId.toString());
      } else {
        zoneId = ZoneId.of(getTimeZone());
      }

    } catch (TException | IoTDBSQLException e) {
      throw new IoTDBSessionException(String.format("Can not open session to %s:%s with username: %s, password: %s.",
          host, port, username, password), e);
    }
    isClosed = false;

    client = IoTDBConnection.newSynchronizedClient(client);

  }

  public void close() {
    if (isClosed) {
      return;
    }
    TSCloseSessionReq req = new TSCloseSessionReq(sessionHandle);
    try {
      client.closeSession(req);
    } catch (TException e) {
      throw new IoTDBSessionException("Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      isClosed = true;
      if (transport != null) {
        transport.close();
      }
    }
  }

  public TSExecuteBatchStatementResp insertBatch(IoTDBRowBatch rowBatch) {
    TSBatchInsertionReq request = new TSBatchInsertionReq();
    request.deviceId = rowBatch.getDeviceId();
    request.measurements = rowBatch.getMeasurements();
    request.timestamps = rowBatch.getTimestamps();
    request.columns = rowBatch.getColumns();
    request.types = rowBatch.getDataTypes();
    try {
      return client.insertBatch(request);
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  public String getTimeZone() throws TException, IoTDBSQLException {
    if (zoneId != null) {
      return zoneId.toString();
    }

    TSGetTimeZoneResp resp = client.getTimeZone();
    Utils.verifySuccess(resp.getStatus());
    return resp.getTimeZone();
  }

  public void setTimeZone(String zoneId) throws TException, IoTDBSQLException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(zoneId);
    TSSetTimeZoneResp resp = client.setTimeZone(req);
    Utils.verifySuccess(resp.getStatus());
    this.zoneId = ZoneId.of(zoneId);
  }

}
