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
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncPipeDataTransferServiceClient extends IClientRPCService.AsyncClient
    implements ThriftClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AsyncPipeDataTransferServiceClient.class);

  private final boolean printLogWhenEncounterException;

  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager;

  private final AtomicBoolean shouldReturnSelf = new AtomicBoolean(true);

  public AsyncPipeDataTransferServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      TAsyncClientManager tClientManager,
      ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tClientManager,
        TNonblockingSocketWrapper.wrap(
            endpoint.getIp(), endpoint.getPort(), property.getConnectionTimeoutMs()));
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  @Override
  public void onError(Exception e) {
    super.onError(e);
    ThriftClient.resolveException(e, this);
    returnSelf();
  }

  @Override
  public void invalidate() {
    if (!hasError()) {
      super.onError(new Exception("This client has been invalidated"));
    }
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endpoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  /**
   * return self, the method doesn't need to be called by the user and will be triggered after the
   * RPC is finished.
   */
  public void returnSelf() {
    if (shouldReturnSelf.get()) {
      clientManager.returnClient(endpoint, this);
    }
  }

  public void setShouldReturnSelf(boolean shouldReturnSelf) {
    this.shouldReturnSelf.set(shouldReturnSelf);
  }

  private void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  private boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      if (printLogWhenEncounterException) {
        LOGGER.error("Unexpected exception occurs in {} : {}", this, e.getMessage());
      }
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("AsyncPipeDataTransferServiceClient{%s}", endpoint);
  }

  public static class Factory
      extends AsyncThriftClientFactory<TEndPoint, AsyncPipeDataTransferServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncPipeDataTransferServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncPipeDataTransferServiceClient> makeObject(TEndPoint endPoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new AsyncPipeDataTransferServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncPipeDataTransferServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
