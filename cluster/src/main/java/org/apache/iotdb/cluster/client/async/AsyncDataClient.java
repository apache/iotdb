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

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientPool;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.AsyncClient;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
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
public class AsyncDataClient extends AsyncClient {

  private static final Logger logger = LoggerFactory.getLogger(AsyncDataClient.class);

  private Node node;
  private ClientCategory category;
  private IClientPool clientPool;

  @TestOnly
  public AsyncDataClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport) {
    super(protocolFactory, clientManager, transport);
  }

  public AsyncDataClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      Node node,
      ClientCategory category)
      throws IOException {
    // the difference of the two clients lies in the port
    super(
        protocolFactory,
        clientManager,
        TNonblockingSocketWrapper.wrap(
            node.getInternalIp(),
            ClientUtils.getPort(node, category),
            ClusterConstant.getConnectionTimeoutInMS()));
    this.node = node;
    this.category = category;
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public boolean isValid() {
    return ___transport != null;
  }

  public void setClientPool(IClientPool clientPool) {
    this.clientPool = clientPool;
  }

  /** return self if clientPool is not null */
  public void returnSelf() {
    if (clientPool != null) clientPool.returnAsyncClient(this, node, category);
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
    // TODO: active node status
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

    @Override
    public void destroyObject(Node node, PooledObject<AsyncDataClient> pooledObject)
        throws Exception {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataClient> makeObject(Node node) throws Exception {
      TAsyncClientManager manager = managers[clientCnt.incrementAndGet() % managers.length];
      manager = manager == null ? new TAsyncClientManager() : manager;
      return new DefaultPooledObject<>(
          new AsyncDataClient(protocolFactory, manager, node, category));
    }

    @Override
    public boolean validateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isValid();
    }
  }

  public static class SingleManagerFactory
      implements KeyedPooledObjectFactory<Node, AsyncDataClient> {

    private TAsyncClientManager manager;
    private TProtocolFactory protocolFactory;

    public SingleManagerFactory(TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
      try {
        manager = new TAsyncClientManager();
      } catch (IOException e) {
        logger.error("Cannot init manager of SingleThreadFactoryAsync", e);
      }
    }

    @Override
    public void activateObject(Node node, PooledObject<AsyncDataClient> pooledObject)
        throws Exception {}

    @Override
    public void destroyObject(Node node, PooledObject<AsyncDataClient> pooledObject)
        throws Exception {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataClient> makeObject(Node node) throws Exception {
      return new DefaultPooledObject<>(
          new AsyncDataClient(protocolFactory, manager, node, ClientCategory.DATA));
    }

    @Override
    public void passivateObject(Node node, PooledObject<AsyncDataClient> pooledObject)
        throws Exception {}

    @Override
    public boolean validateObject(Node node, PooledObject<AsyncDataClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isValid();
    }
  }
}
