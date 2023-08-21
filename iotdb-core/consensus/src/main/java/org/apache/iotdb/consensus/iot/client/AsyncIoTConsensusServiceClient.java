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
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.consensus.iot.thrift.IoTConsensusIService;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AsyncIoTConsensusServiceClient extends IoTConsensusIService.AsyncClient
    implements ThriftClient {

  private static final Logger logger =
      LoggerFactory.getLogger(AsyncIoTConsensusServiceClient.class);

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager;

  public AsyncIoTConsensusServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      TAsyncClientManager tAsyncClientManager,
      ClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tAsyncClientManager,
        TNonblockingSocketWrapper.wrap(
            endpoint.getIp(), endpoint.getPort(), property.getConnectionTimeoutMs()));
    setTimeout(property.getConnectionTimeoutMs());
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
  private void returnSelf() {
    clientManager.returnClient(endpoint, this);
  }

  private void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      logger.info("Unexpected exception occurs in {} :", this, e);
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("AsyncConfigNodeIServiceClient{%s}", endpoint);
  }

  public static class Factory
      extends AsyncThriftClientFactory<TEndPoint, AsyncIoTConsensusServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncIoTConsensusServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncIoTConsensusServiceClient> makeObject(TEndPoint endPoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new AsyncIoTConsensusServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncIoTConsensusServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
