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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;
import java.util.Objects;
import java.util.function.BiConsumer;

public class SyncDataNodeInternalServiceClient extends IDataNodeRPCService.Client
    implements ThriftClient, AutoCloseable {

  private static final BiConsumer<Throwable, TEndPoint> NO_OP_FAILURE_REPORTER =
      (failure, target) -> {
        // Do nothing.
      };

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  public SyncDataNodeInternalServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager)
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

  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @TestOnly
  public TEndPoint getTEndpoint() {
    return endpoint;
  }

  @TestOnly
  public ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> getClientManager() {
    return clientManager;
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
    return String.format("SyncDataNodeInternalServiceClient{%s}", endpoint);
  }

  public static class Factory
      extends ThriftClientFactory<TEndPoint, SyncDataNodeInternalServiceClient> {

    private final BiConsumer<Throwable, TEndPoint> failureReporter;

    public Factory(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty) {
      this(clientManager, thriftClientProperty, NO_OP_FAILURE_REPORTER);
    }

    public Factory(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        BiConsumer<Throwable, TEndPoint> failureReporter) {
      super(clientManager, thriftClientProperty);
      this.failureReporter = Objects.requireNonNull(failureReporter);
    }

    @Override
    public void destroyObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeInternalServiceClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<SyncDataNodeInternalServiceClient> makeObject(TEndPoint endpoint)
        throws Exception {
      try {
        return new DefaultPooledObject<>(
            SyncThriftClientWithErrorHandler.newErrorHandlerWithFailureHandler(
                SyncDataNodeInternalServiceClient.class,
                SyncDataNodeInternalServiceClient.class.getConstructor(
                    thriftClientProperty.getClass(), endpoint.getClass(), clientManager.getClass()),
                (failure, client) -> reportFailure(failure, endpoint, failureReporter),
                thriftClientProperty,
                endpoint,
                clientManager));
      } catch (final Exception e) {
        reportFailure(e, endpoint, failureReporter);
        throw e;
      }
    }

    @Override
    public boolean validateObject(
        TEndPoint endpoint, PooledObject<SyncDataNodeInternalServiceClient> pooledObject) {
      return pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }

  private static void reportFailure(
      final Throwable failure,
      final TEndPoint target,
      final BiConsumer<Throwable, TEndPoint> failureReporter) {
    try {
      failureReporter.accept(failure, target);
    } catch (final RuntimeException reportingFailure) {
      if (reportingFailure != failure) {
        failure.addSuppressed(reportingFailure);
      }
    }
  }
}
