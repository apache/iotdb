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
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.pipe.thrift.PipeConsensusIService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;

public class SyncPipeConsensusServiceClient extends PipeConsensusIService.Client
    implements ThriftClient, AutoCloseable {

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, SyncPipeConsensusServiceClient> clientManager;
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  public SyncPipeConsensusServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncPipeConsensusServiceClient> clientManager)
      throws TTransportException {
    super(
        property
            .getProtocolFactory()
            .getProtocol(
                commonConfig.isEnableInternalSSL()
                    ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                        endpoint.getIp(),
                        endpoint.getPort(),
                        property.getConnectionTimeoutMs(),
                        commonConfig.getTrustStorePath(),
                        commonConfig.getTrustStorePwd(),
                        commonConfig.getKeyStorePath(),
                        commonConfig.getKeyStorePwd())
                    : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                        new TSocket(
                            TConfigurationConst.defaultTConfiguration,
                            endpoint.getIp(),
                            endpoint.getPort(),
                            property.getConnectionTimeoutMs()))));
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endpoint = endpoint;
    this.clientManager = clientManager;
    if (!getInputProtocol().getTransport().isOpen()) {
      getInputProtocol().getTransport().open();
    }
  }

  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @Override
  public void close() {
    clientManager.returnClient(endpoint, this);
  }

  @Override
  public void invalidate() {
    getInputProtocol().getTransport().close();
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endpoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  @Override
  public String toString() {
    return String.format("SyncPipeConsensusServiceClient{%s}", endpoint);
  }

  public TEndPoint getTEndpoint() {
    return endpoint;
  }

  public static class Factory
      extends ThriftClientFactory<TEndPoint, SyncPipeConsensusServiceClient> {

    public Factory(
        ClientManager<TEndPoint, SyncPipeConsensusServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncPipeConsensusServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncPipeConsensusServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              SyncPipeConsensusServiceClient.class,
              SyncPipeConsensusServiceClient.class.getConstructor(
                  thriftClientProperty.getClass(), endpoint.getClass(), clientManager.getClass()),
              thriftClientProperty,
              endpoint,
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncPipeConsensusServiceClient> pooledObject) {
      return pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
