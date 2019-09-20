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

import java.util.List;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.time.ZoneId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  private String host;
  private int port;
  private String username;
  private String password;
  private final TSProtocolVersion protocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1;
  private TSIService.Iface client = null;
  private TS_SessionHandle sessionHandle = null;
  private TSocket transport;
  private boolean isClosed = true;
  private ZoneId zoneId;

  public Session(String host, int port) {
    this(host, port, Config.DEFAULT_USER, Config.DEFAULT_PASSWORD);
  }

  public Session(String host, String port, String username, String password) {
    this(host, Integer.parseInt(port), username, password);
  }

  public Session(String host, int port, String username, String password) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
  }

  public synchronized void open() throws IoTDBSessionException {
    open(false, 0);
  }

  public synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBSessionException {
    if (!isClosed) {
      return;
    }
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

      RpcUtils.verifySuccess(openResp.getStatus());

      if (protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        throw new TException(String
            .format("Protocol not supported, Client version is {}, but Server version is {}",
                protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue()));
      }

      sessionHandle = openResp.getSessionHandle();

      if (zoneId != null) {
        setTimeZone(zoneId.toString());
      } else {
        zoneId = ZoneId.of(getTimeZone());
      }

    } catch (TException | IoTDBRPCException e) {
      transport.close();
      throw new IoTDBSessionException(String.format("Can not open session to %s:%s with user: %s.",
          host, port, username), e);
    }
    isClosed = false;

    client = RpcUtils.newSynchronizedClient(client);

  }

  public synchronized void close() throws IoTDBSessionException {
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

  public synchronized TSExecuteBatchStatementResp insertBatch(RowBatch rowBatch) throws IoTDBSessionException {
    TSBatchInsertionReq request = new TSBatchInsertionReq();
    request.deviceId = rowBatch.deviceId;
    for (MeasurementSchema measurementSchema: rowBatch.measurements) {
      request.addToMeasurements(measurementSchema.getMeasurementId());
      request.addToTypes(measurementSchema.getType().ordinal());
    }
    request.setTimestamps(SessionUtils.getTimeBuffer(rowBatch));
    request.setValues(SessionUtils.getValueBuffer(rowBatch));
    request.setSize(rowBatch.batchSize);

    try {
      return checkAndReturn(client.insertBatch(request));
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  public synchronized TSStatus insert(String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBSessionException {
    TSInsertReq request = new TSInsertReq();
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);

    try {
      return checkAndReturn(client.insertRow(request));
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  public synchronized TSStatus delete(String path, long time) throws IoTDBSessionException {
    TSDeleteReq request = new TSDeleteReq();
    request.setPath(path);
    request.setTimestamp(time);

    try {
      return checkAndReturn(client.deleteData(request));
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  public synchronized TSStatus setStorageGroup(String storageGroupId) throws IoTDBSessionException {
    try {
      return checkAndReturn(client.setStorageGroup(storageGroupId));
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  public synchronized TSStatus createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor) throws IoTDBSessionException {
    TSCreateTimeseriesReq request = new TSCreateTimeseriesReq();
    request.setPath(path);
    request.setDataType(dataType.ordinal());
    request.setEncoding(encoding.ordinal());
    request.setCompressor(compressor.ordinal());

    try {
      return checkAndReturn(client.createTimeseries(request));
    } catch (TException e) {
      throw new IoTDBSessionException(e);
    }
  }

  private TSStatus checkAndReturn(TSStatus resp) {
    if (resp.statusType.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error(resp.statusType.getMessage());
    }
    return resp;
  }

  private TSExecuteBatchStatementResp checkAndReturn(TSExecuteBatchStatementResp resp) {
    if (resp.status.statusType.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      logger.error(resp.status.statusType.getMessage());
    }
    return resp;
  }

  public synchronized String getTimeZone() throws TException, IoTDBRPCException {
    if (zoneId != null) {
      return zoneId.toString();
    }

    TSGetTimeZoneResp resp = client.getTimeZone();
    RpcUtils.verifySuccess(resp.getStatus());
    return resp.getTimeZone();
  }

  public synchronized void setTimeZone(String zoneId) throws TException, IoTDBRPCException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(zoneId);
    TSStatus resp = client.setTimeZone(req);
    RpcUtils.verifySuccess(resp);
    this.zoneId = ZoneId.of(zoneId);
  }

}
