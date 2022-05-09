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

package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.client.BaseFactory;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else.
 */
public class AsyncDataClient extends TSDataService.AsyncClient {

  private static final Logger logger = LoggerFactory.getLogger(AsyncDataClient.class);

  private Node node;
  private ClientCategory category;
  private IClientManager clientManager;

  @TestOnly
  public AsyncDataClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport) {
    super(protocolFactory, clientManager, transport);
  }

  public AsyncDataClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager tClientManager,
      Node node,
      ClientCategory category)
      throws IOException {
    // the difference of the two clients lies in the port
    super(
        protocolFactory,
        tClientManager,
        TNonblockingSocketWrapper.wrap(
            node.getInternalIp(),
            ClientUtils.getPort(node, category),
            ClusterConstant.getConnectionTimeoutInMS()));
    this.node = node;
    this.category = category;
  }

  public AsyncDataClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager tClientManager,
      Node node,
      ClientCategory category,
      IClientManager manager)
      throws IOException {
    this(protocolFactory, tClientManager, node, category);
    this.clientManager = manager;
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public boolean isValid() {
    return ___transport != null;
  }

  /**
   * return self if clientPool is not null, the method doesn't need to call by user, it will trigger
   * once client transport complete.
   */
  private void returnSelf() {
    if (clientManager != null) {
      clientManager.returnAsyncClient(this, node, category);
    }
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  @Override
  public String toString() {
    return "Async"
        + category.getName()
        + "{"
        + "node="
        + node
        + ","
        + "port="
        + ClientUtils.getPort(node, category)
        + '}';
  }

  public Node getNode() {
    return node;
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @TestOnly
  TAsyncMethodCall<Object> getCurrMethod() {
    return ___currentMethod;
  }

  public static class AsyncDataClientFactory extends AsyncBaseFactory<Node, AsyncDataClient> {

    public AsyncDataClientFactory(TProtocolFactory protocolFactory, ClientCategory category) {
      super(protocolFactory, category);
    }

    public AsyncDataClientFactory(
        TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
      super(protocolFactory, category, clientManager);
    }

    @Override
    public void destroyObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataClient> makeObject(Node node) throws Exception {
      TAsyncClientManager manager = managers[clientCnt.incrementAndGet() % managers.length];
      manager = manager == null ? new TAsyncClientManager() : manager;
      return new DefaultPooledObject<>(
          new AsyncDataClient(protocolFactory, manager, node, category, clientPoolManager));
    }

    @Override
    public boolean validateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isValid();
    }
  }

  public static class SingleManagerFactory extends BaseFactory<Node, AsyncDataClient> {

    public SingleManagerFactory(TProtocolFactory protocolFactory) {
      super(protocolFactory, ClientCategory.DATA);
      managers = new TAsyncClientManager[1];
      try {
        managers[0] = new TAsyncClientManager();
      } catch (IOException e) {
        logger.error("Cannot create data heartbeat client manager for factory", e);
      }
    }

    public SingleManagerFactory(TProtocolFactory protocolFactory, IClientManager clientManager) {
      this(protocolFactory);
      this.clientPoolManager = clientManager;
    }

    @Override
    public void activateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {}

    @Override
    public void destroyObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataClient> makeObject(Node node) throws Exception {
      return new DefaultPooledObject<>(
          new AsyncDataClient(
              protocolFactory, managers[0], node, ClientCategory.DATA, clientPoolManager));
    }

    @Override
    public void passivateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {}

    @Override
    public boolean validateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isValid();
    }
  }
}
