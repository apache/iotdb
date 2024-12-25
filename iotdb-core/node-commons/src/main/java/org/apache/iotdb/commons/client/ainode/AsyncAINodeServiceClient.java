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

package org.apache.iotdb.commons.client.ainode;

import org.apache.iotdb.ainode.rpc.thrift.IAINodeRPCService;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;

import java.io.IOException;

public class AsyncAINodeServiceClient extends IAINodeRPCService.AsyncClient
    implements ThriftClient {

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endPoint;
  private final ClientManager<TEndPoint, AsyncAINodeServiceClient> clientManager;

  public AsyncAINodeServiceClient(
      ThriftClientProperty property,
      TEndPoint endPoint,
      TAsyncClientManager tClientManager,
      ClientManager<TEndPoint, AsyncAINodeServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tClientManager,
        TNonblockingSocketWrapper.wrap(
            endPoint.getIp(), endPoint.getPort(), property.getConnectionTimeoutMs()));
    setTimeout(property.getConnectionTimeoutMs());
    this.printLogWhenEncounterException = property.isPrintLogWhenEncounterException();
    this.endPoint = endPoint;
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
    clientManager.clear(endPoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return printLogWhenEncounterException;
  }

  private void returnSelf() {
    clientManager.returnClient(endPoint, this);
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
      if (printLogWhenEncounterException) {
        logger.error("Unexpected exception occurs in {} : {}", this, e.getMessage());
      }
      return false;
    }
  }

  public static class Factory
      extends AsyncThriftClientFactory<TEndPoint, AsyncAINodeServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncAINodeServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncAINodeServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncAINodeServiceClient> makeObject(TEndPoint endPoint) throws Exception {
      return new DefaultPooledObject<>(
          new AsyncAINodeServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncAINodeServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
