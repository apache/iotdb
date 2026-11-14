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

package org.apache.iotdb.consensus.raft.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.raft.thrift.RaftConsensusIService;
import org.apache.iotdb.rpc.TNonblockingTransportWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Pooled async client (one in-flight call per connection); returns itself to the pool. */
public class AsyncRaftServiceClient extends RaftConsensusIService.AsyncClient
    implements ThriftClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRaftServiceClient.class);
  private static final CommonConfig COMMON = CommonDescriptor.getInstance().getConfig();

  private final boolean printLogWhenEncounterException;
  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;

  public AsyncRaftServiceClient(
      ThriftClientProperty property,
      TEndPoint endpoint,
      TAsyncClientManager tAsyncClientManager,
      ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager)
      throws IOException {
    super(
        property.getProtocolFactory(),
        tAsyncClientManager,
        COMMON.isEnableInternalSSL()
            ? TNonblockingTransportWrapper.wrap(
                endpoint.getIp(),
                endpoint.getPort(),
                property.getConnectionTimeoutMs(),
                COMMON.getKeyStorePath(),
                COMMON.getKeyStorePwd(),
                COMMON.getTrustStorePath(),
                COMMON.getTrustStorePwd())
            : TNonblockingTransportWrapper.wrap(
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
      LOGGER.debug("{} not ready: {}", this, e.toString());
      return false;
    }
  }

  @Override
  public String toString() {
    return "AsyncRaftServiceClient{" + endpoint + "}";
  }

  public static class Factory extends AsyncThriftClientFactory<TEndPoint, AsyncRaftServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager,
        ThriftClientProperty thriftClientProperty,
        String threadName) {
      super(clientManager, thriftClientProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncRaftServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncRaftServiceClient> makeObject(TEndPoint endPoint) throws Exception {
      return new DefaultPooledObject<>(
          new AsyncRaftServiceClient(
              thriftClientProperty,
              endPoint,
              tManagers[clientCnt.incrementAndGet() % tManagers.length],
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncRaftServiceClient> pooledObject) {
      return pooledObject.getObject().isReady();
    }
  }
}
