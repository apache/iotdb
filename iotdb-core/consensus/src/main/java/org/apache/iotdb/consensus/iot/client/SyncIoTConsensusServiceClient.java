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

package org.apache.iotdb.consensus.iot.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.TrustedChannelFailureHandler;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.iot.thrift.IoTConsensusIService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class SyncIoTConsensusServiceClient extends IoTConsensusIService.Client
    implements ThriftClient, AutoCloseable {
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, SyncIoTConsensusServiceClient> clientManager;

  public SyncIoTConsensusServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncIoTConsensusServiceClient> clientManager)
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
                        commonConfig.getKeyStorePwd(),
                        commonConfig.getSslProtocol())
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
    return String.format("SyncIoTConsensusServiceClient{%s}", endpoint);
  }

  public static class Factory
      extends ThriftClientFactory<TEndPoint, SyncIoTConsensusServiceClient> {

    private final TrustedChannelFailureReporter trustedChannelFailureReporter;

    public Factory(
        ClientManager<TEndPoint, SyncIoTConsensusServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      this(clientManager, thriftClientProperty, null, TrustedChannelFailureHandler.NO_OP);
    }

    public Factory(
        ClientManager<TEndPoint, SyncIoTConsensusServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        TEndPoint initiator,
        TrustedChannelFailureHandler trustedChannelFailureHandler) {
      super(clientManager, thriftClientProperty);
      this.trustedChannelFailureReporter =
          new TrustedChannelFailureReporter(initiator, trustedChannelFailureHandler);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncIoTConsensusServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncIoTConsensusServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      try {
        return new DefaultPooledObject<>(
            SyncThriftClientWithErrorHandler.newErrorHandlerWithFailureHandler(
                SyncIoTConsensusServiceClient.class,
                SyncIoTConsensusServiceClient.class.getConstructor(
                    thriftClientProperty.getClass(), endpoint.getClass(), clientManager.getClass()),
                (failure, client) -> trustedChannelFailureReporter.report(failure, client.endpoint),
                thriftClientProperty,
                endpoint,
                clientManager));
      } catch (final Exception e) {
        trustedChannelFailureReporter.report(e, endpoint);
        throw e;
      }
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncIoTConsensusServiceClient> pooledObject) {
      return pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
