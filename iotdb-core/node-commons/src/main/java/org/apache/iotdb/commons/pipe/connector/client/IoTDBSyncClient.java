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
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class IoTDBSyncClient extends IClientRPCService.Client
    implements ThriftClient, AutoCloseable {

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
