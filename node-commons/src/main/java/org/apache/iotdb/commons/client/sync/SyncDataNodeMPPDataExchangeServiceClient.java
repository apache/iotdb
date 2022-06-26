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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.lang.reflect.Constructor;
import java.net.SocketException;

public class SyncDataNodeMPPDataExchangeServiceClient extends MPPDataExchangeService.Client
    implements SyncThriftClient, AutoCloseable {

  private final TEndPoint endPoint;
  private final ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager;

  public SyncDataNodeMPPDataExchangeServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      TEndPoint endPoint,
      ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager)
      throws TTransportException {
    super(
        protocolFactory.getProtocol(
            RpcTransportFactory.INSTANCE.getTransport(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    endPoint.getIp(),
                    endPoint.getPort(),
                    connectionTimeout))));
    this.endPoint = endPoint;
    this.clientManager = clientManager;
    getInputProtocol().getTransport().open();
  }

  public void close() {
    if (clientManager != null) {
      clientManager.returnClient(endPoint, this);
    }
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  public void invalidate() {
    getInputProtocol().getTransport().close();
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endPoint);
  }

  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  @Override
  public String toString() {
    return String.format("SyncDataNodeMPPDataExchangeServiceClient{%s}", endPoint);
  }

  public static class Factory
      extends BaseClientFactory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

    public Factory(
        ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager,
        ClientFactoryProperty clientFactoryProperty) {
      super(clientManager, clientFactoryProperty);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeMPPDataExchangeServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncDataNodeMPPDataExchangeServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      Constructor<SyncDataNodeMPPDataExchangeServiceClient> constructor =
          SyncDataNodeMPPDataExchangeServiceClient.class.getConstructor(
              TProtocolFactory.class, int.class, endpoint.getClass(), clientManager.getClass());
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              SyncDataNodeMPPDataExchangeServiceClient.class,
              constructor,
              clientFactoryProperty.getProtocolFactory(),
              clientFactoryProperty.getConnectionTimeoutMs(),
              endpoint,
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeMPPDataExchangeServiceClient> pooledObject) {
      return pooledObject.getObject() != null
          && pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
