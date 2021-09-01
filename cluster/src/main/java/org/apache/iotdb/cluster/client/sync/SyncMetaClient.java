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

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientPool;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.Client;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else.
 */
public class SyncMetaClient extends Client {

  private Node node;
  private ClientCategory category;
  private IClientPool clientPool;

  public SyncMetaClient(TProtocolFactory protocolFactory, Node node, ClientCategory category)
      throws TTransportException {
    super(
        protocolFactory.getProtocol(
            RpcTransportFactory.INSTANCE.getTransport(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    node.getInternalIp(),
                    ClientUtils.getPort(node, category),
                    ClusterConstant.getConnectionTimeoutInMS()))));
    this.node = node;
    this.category = category;
    getInputProtocol().getTransport().open();
  }

  public void setClientPool(IClientPool clientPool) {
    this.clientPool = clientPool;
  }

  public void returnSelf() {
    if (clientPool != null) clientPool.returnSyncClient(this, node, category);
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  @TestOnly
  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public void close() {
    getInputProtocol().getTransport().close();
  }

  public Node getNode() {
    return node;
  }

  @Override
  public String toString() {
    return "Sync"
        + category.getName()
        + "{"
        + "node="
        + node
        + ","
        + "port="
        + ClientUtils.getPort(node, category)
        + '}';
  }

  public static class SyncMetaClientFactory
      implements KeyedPooledObjectFactory<Node, SyncMetaClient> {

    private TProtocolFactory protocolFactory;
    private ClientCategory category;

    public SyncMetaClientFactory(TProtocolFactory protocolFactory, ClientCategory category) {
      this.protocolFactory = protocolFactory;
      this.category = category;
    }

    @Override
    public void activateObject(Node node, PooledObject<SyncMetaClient> pooledObject)
        throws Exception {}

    @Override
    public void destroyObject(Node node, PooledObject<SyncMetaClient> pooledObject)
        throws Exception {
      pooledObject.getObject().getInputProtocol().getTransport().close();
    }

    @Override
    public PooledObject<SyncMetaClient> makeObject(Node node) throws Exception {
      return new DefaultPooledObject<>(new SyncMetaClient(protocolFactory, node, category));
    }

    @Override
    public void passivateObject(Node node, PooledObject<SyncMetaClient> pooledObject)
        throws Exception {}

    @Override
    public boolean validateObject(Node node, PooledObject<SyncMetaClient> pooledObject) {
      return pooledObject.getObject() != null
          && pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
