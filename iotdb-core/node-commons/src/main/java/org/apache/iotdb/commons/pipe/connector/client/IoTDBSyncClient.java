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

package org.apache.iotdb.commons.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBSyncClient extends IClientRPCService.Client
    implements ThriftClient, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncClient.class);

  private static final AtomicInteger SLICE_ORDER_ID_GENERATOR = new AtomicInteger(0);

  private final String ipAddress;
  private final int port;
  private final TEndPoint endPoint;

  public IoTDBSyncClient(
      ThriftClientProperty property,
      String ipAddress,
      int port,
      boolean useSSL,
      String trustStore,
      String trustStorePwd)
      throws TTransportException {
    super(
        property
            .getProtocolFactory()
            .getProtocol(
                useSSL
                    ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                        ipAddress,
                        port,
                        property.getConnectionTimeoutMs(),
                        trustStore,
                        trustStorePwd)
                    : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                        ipAddress, port, property.getConnectionTimeoutMs())));
    this.ipAddress = ipAddress;
    this.port = port;
    this.endPoint = new TEndPoint(ipAddress, port);
    final TTransport transport = getInputProtocol().getTransport();
    if (!transport.isOpen()) {
      transport.open();
    }
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public int getPort() {
    return port;
  }

  public TEndPoint getEndPoint() {
    return endPoint;
  }

  public void setTimeout(int timeout) {
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @Override
  public TPipeTransferResp pipeTransfer(final TPipeTransferReq req) throws TException {
    final int bodySizeLimit = PipeConfig.getInstance().getPipeConnectorRequestSliceThresholdBytes();
    if (req.getVersion() != IoTDBConnectorRequestVersion.VERSION_1.getVersion()
        || req.body.limit() < bodySizeLimit) {
      return super.pipeTransfer(req);
    }

    LOGGER.warn(
        "The body size of the request is too large. The request will be sliced. Origin req: {}-{}. "
            + "Request body size: {}, threshold: {}",
        req.getVersion(),
        req.getType(),
        req.body.limit(),
        bodySizeLimit);

    try {
      final int sliceOrderId = SLICE_ORDER_ID_GENERATOR.getAndIncrement();
      // Slice the buffer to avoid the buffer being too large
      final int sliceCount =
          req.body.limit() / bodySizeLimit + (req.body.limit() % bodySizeLimit == 0 ? 0 : 1);
      for (int i = 0; i < sliceCount; ++i) {
        final int startIndexInBody = i * bodySizeLimit;
        final int endIndexInBody = Math.min((i + 1) * bodySizeLimit, req.body.limit());
        final TPipeTransferResp sliceResp =
            super.pipeTransfer(
                PipeTransferSliceReq.toTPipeTransferReq(
                    sliceOrderId,
                    req.getType(),
                    i,
                    sliceCount,
                    req.body.duplicate(),
                    startIndexInBody,
                    endIndexInBody));

        if (i == sliceCount - 1) {
          return sliceResp;
        }

        if (sliceResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new PipeConnectionException(
              String.format(
                  "Failed to transfer slice. Origin req: %s-%s, slice index: %d, slice count: %d. Reason: %s",
                  req.getVersion(), req.getType(), i, sliceCount, sliceResp.getStatus()));
        }
      }

      // Should not reach here
      return super.pipeTransfer(req);
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to transfer slice. Origin req: {}-{}. Retry the whole transfer.",
          req.getVersion(),
          req.getType(),
          e);
      // Fall back to the original behavior
      return super.pipeTransfer(req);
    }
  }

  @Override
  public void close() throws Exception {
    invalidate();
  }

  @Override
  public void invalidate() {
    if (getInputProtocol().getTransport().isOpen()) {
      getInputProtocol().getTransport().close();
    }
  }

  @Override
  public void invalidateAll() {
    invalidate();
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return true;
  }
}
