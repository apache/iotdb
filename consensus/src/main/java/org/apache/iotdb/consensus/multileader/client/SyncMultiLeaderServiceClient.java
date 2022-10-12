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

package org.apache.iotdb.consensus.multileader.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.sync.SyncThriftClient;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.multileader.thrift.MultiLeaderConsensusIService;
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

public class SyncMultiLeaderServiceClient extends MultiLeaderConsensusIService.Client
    implements SyncThriftClient, AutoCloseable {

  private final TEndPoint endPoint;
  private final ClientManager<TEndPoint, SyncMultiLeaderServiceClient> clientManager;

  public SyncMultiLeaderServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      TEndPoint endPoint,
      ClientManager<TEndPoint, SyncMultiLeaderServiceClient> clientManager)
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

  @TestOnly
  public TEndPoint getTEndpoint() {
    return endPoint;
  }

  @TestOnly
  public ClientManager<TEndPoint, SyncMultiLeaderServiceClient> getClientManager() {
    return clientManager;
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
    return String.format("SyncMultiLeaderServiceClient{%s}", endPoint);
  }

  public static class Factory extends BaseClientFactory<TEndPoint, SyncMultiLeaderServiceClient> {

    public Factory(
        ClientManager<TEndPoint, SyncMultiLeaderServiceClient> clientManager,
        ClientFactoryProperty clientFactoryProperty) {
      super(clientManager, clientFactoryProperty);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncMultiLeaderServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncMultiLeaderServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      Constructor<SyncMultiLeaderServiceClient> constructor =
          SyncMultiLeaderServiceClient.class.getConstructor(
              TProtocolFactory.class, int.class, endpoint.getClass(), clientManager.getClass());
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              SyncMultiLeaderServiceClient.class,
              constructor,
              clientFactoryProperty.getProtocolFactory(),
              clientFactoryProperty.getConnectionTimeoutMs(),
              endpoint,
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncMultiLeaderServiceClient> pooledObject) {
      return pooledObject.getObject() != null
          && pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
