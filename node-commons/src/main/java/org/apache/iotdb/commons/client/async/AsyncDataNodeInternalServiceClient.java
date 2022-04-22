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

package org.apache.iotdb.commons.client.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.AsyncBaseClientFactory;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerProperty;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

public class AsyncDataNodeInternalServiceClient extends InternalService.AsyncClient {

  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  public AsyncDataNodeInternalServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      TEndPoint endpoint,
      TAsyncClientManager tClientManager,
      ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager)
      throws IOException {
    super(
        protocolFactory,
        tClientManager,
        TNonblockingSocketWrapper.wrap(endpoint.getIp(), endpoint.getPort(), connectionTimeout));
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  @TestOnly
  public TEndPoint getEndpoint() {
    return endpoint;
  }

  @TestOnly
  public ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> getClientManager() {
    return clientManager;
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  /**
   * return self if clientManager is not null, the method doesn't need to call by user, it will
   * trigger once client transport complete.
   */
  private void returnSelf() {
    if (clientManager != null) {
      clientManager.returnClient(endpoint, this);
    }
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static class Factory
      extends AsyncBaseClientFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager,
        ClientManagerProperty<AsyncDataNodeInternalServiceClient> clientManagerProperty) {
      super(clientManager, clientManagerProperty);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncDataNodeInternalServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataNodeInternalServiceClient> makeObject(TEndPoint endPoint)
        throws Exception {
      TAsyncClientManager tManager = tManagers[clientCnt.incrementAndGet() % tManagers.length];
      tManager = tManager == null ? new TAsyncClientManager() : tManager;
      return new DefaultPooledObject<>(
          new AsyncDataNodeInternalServiceClient(
              clientManagerProperty.getProtocolFactory(),
              clientManagerProperty.getConnectionTimeoutMs(),
              endPoint,
              tManager,
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncDataNodeInternalServiceClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isReady();
    }
  }
}
