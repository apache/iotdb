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
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.TAsyncMethodCall;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;

import java.io.IOException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else.
 */
public class AsyncMetaClient extends TSMetaService.AsyncClient {

  private Node node;
  private ClientCategory category;
  private IClientManager clientManager;

  public AsyncMetaClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport) {
    super(protocolFactory, clientManager, transport);
  }

  public AsyncMetaClient(
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

  public AsyncMetaClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      Node node,
      ClientCategory category,
      IClientManager manager)
      throws IOException {
    this(protocolFactory, clientManager, node, category);
    this.clientManager = manager;
  }

  /**
   * return self if clientManager is not null, the method doesn't need to call by user, it will
   * trigger once client transport complete.
   */
  public void returnSelf() {
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

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public Node getNode() {
    return node;
  }

  @TestOnly
  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean isValid() {
    return ___transport != null;
  }

  @TestOnly
  TAsyncMethodCall<Object> getCurrMethod() {
    return ___currentMethod;
  }

  public static class AsyncMetaClientFactory extends AsyncBaseFactory<Node, AsyncMetaClient> {

    public AsyncMetaClientFactory(TProtocolFactory protocolFactory, ClientCategory category) {
      super(protocolFactory, category);
    }

    public AsyncMetaClientFactory(
        TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
      super(protocolFactory, category, clientManager);
    }

    @Override
    public void activateObject(Node node, PooledObject<AsyncMetaClient> pooledObject) {}

    @Override
    public void destroyObject(Node node, PooledObject<AsyncMetaClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncMetaClient> makeObject(Node node) throws Exception {
      TAsyncClientManager manager = managers[clientCnt.incrementAndGet() % managers.length];
      manager = manager == null ? new TAsyncClientManager() : manager;
      return new DefaultPooledObject<>(
          new AsyncMetaClient(protocolFactory, manager, node, category, clientPoolManager));
    }

    @Override
    public void passivateObject(Node node, PooledObject<AsyncMetaClient> pooledObject) {}

    @Override
    public boolean validateObject(Node node, PooledObject<AsyncMetaClient> pooledObject) {
      return pooledObject != null && pooledObject.getObject().isValid();
    }
  }
}
