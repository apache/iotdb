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

package org.apache.iotdb.consensus.natraft.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.factory.AsyncThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.consensus.raft.thrift.NoMemberException;
import org.apache.iotdb.consensus.raft.thrift.RaftService;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncRaftServiceClient extends RaftService.AsyncClient {

  private static final Logger logger = LoggerFactory.getLogger(AsyncRaftServiceClient.class);

  private final TEndPoint endpoint;
  private final ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;
  private static final AtomicInteger createCnt = new AtomicInteger();
  private static final AtomicInteger closeCnt = new AtomicInteger();

  public AsyncRaftServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      TEndPoint endpoint,
      TAsyncClientManager tClientManager,
      ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager)
      throws IOException {
    super(
        protocolFactory,
        tClientManager,
        TNonblockingSocketWrapper.wrap(endpoint.getIp(), endpoint.getPort(), connectionTimeout));
    this.endpoint = endpoint;
    this.clientManager = clientManager;
    if (createCnt.incrementAndGet() % 1000 == 0) {
      logger.info("Created {} clients", createCnt.get(), new Exception());
    }
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
    if (closeCnt.incrementAndGet() % 1000 == 0) {
      logger.info("Closed {} clients", closeCnt.get(), new Exception());
    }
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

  /**
   * This method will be automatically called by the thrift selector thread, and we'll just simulate
   * the behavior in our test
   */
  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  /**
   * This method will be automatically called by the thrift selector thread, and we'll just simulate
   * the behavior in our test
   */
  @Override
  public void onError(Exception e) {
    if (e.getCause() instanceof NoMemberException
        || e instanceof TApplicationException
            && (e.getMessage() != null && e.getMessage().contains("No such member"))) {
      logger.debug(e.getMessage());
      ___currentMethod = null;
      returnSelf();
      return;
    }
    super.onError(e);
    returnSelf();
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      if (!(e.getCause() instanceof ConnectException)) {
        logger.info("Unexpected exception occurs in {} :", this, e);
      } else {
        logger.debug("Cannot connect: {}", e.getMessage());
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {
          // ignore
        }
      }
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("AsyncRaftServiceClient{%s}", endpoint);
  }

  public static class Factory extends AsyncThriftClientFactory<TEndPoint, AsyncRaftServiceClient> {

    public Factory(
        ClientManager<TEndPoint, AsyncRaftServiceClient> clientManager,
        ThriftClientProperty clientFactoryProperty,
        String threadName) {
      super(clientManager, clientFactoryProperty, threadName);
    }

    @Override
    public void destroyObject(
        TEndPoint endPoint, PooledObject<AsyncRaftServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncRaftServiceClient> makeObject(TEndPoint endPoint) throws Exception {
      TAsyncClientManager tManager = tManagers[clientCnt.incrementAndGet() % tManagers.length];
      tManager = tManager == null ? new TAsyncClientManager() : tManager;
      return new DefaultPooledObject<>(
          new AsyncRaftServiceClient(
              thriftClientProperty.getProtocolFactory(),
              thriftClientProperty.getConnectionTimeoutMs(),
              endPoint,
              tManager,
              clientManager));
    }

    @Override
    public boolean validateObject(
        TEndPoint endPoint, PooledObject<AsyncRaftServiceClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isReady();
    }
  }

  public TEndPoint getEndpoint() {
    return endpoint;
  }
}
