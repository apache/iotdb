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

import org.apache.iotdb.cluster.client.BaseFactory;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else.
 */
// TODO: Refine the interfaces of TSDataService. TSDataService interfaces doesn't need extends
// RaftService interfaces.
public class SyncDataClient extends TSDataService.Client {

  private Node node;
  private ClientCategory category;
  private IClientManager clientManager;

  @TestOnly
  public SyncDataClient(TProtocol prot) {
    super(prot);
  }

  public SyncDataClient(TProtocolFactory protocolFactory, Node node, ClientCategory category)
      throws TTransportException {

    // the difference of the two clients lies in the port
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

  public SyncDataClient(
      TProtocolFactory protocolFactory, Node node, ClientCategory category, IClientManager manager)
      throws TTransportException {
    this(protocolFactory, node, category);
    this.clientManager = manager;
  }

  public void returnSelf() {
    if (clientManager != null) {
      clientManager.returnSyncClient(this, node, category);
    }
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  public void close() {
    getInputProtocol().getTransport().close();
  }

  @TestOnly
  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
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

  public Node getNode() {
    return node;
  }

  public static class SyncDataClientFactory extends BaseFactory<Node, SyncDataClient> {

    public SyncDataClientFactory(TProtocolFactory protocolFactory, ClientCategory category) {
      super(protocolFactory, category);
    }

    public SyncDataClientFactory(
        TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
      super(protocolFactory, category, clientManager);
    }

    @Override
    public void activateObject(Node node, PooledObject<SyncDataClient> pooledObject) {
      pooledObject.getObject().setTimeout(ClusterConstant.getConnectionTimeoutInMS());
    }

    @Override
    public void destroyObject(Node node, PooledObject<SyncDataClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<SyncDataClient> makeObject(Node node) throws Exception {
      return new DefaultPooledObject<>(
          new SyncDataClient(protocolFactory, node, category, clientPoolManager));
    }

    @Override
    public boolean validateObject(Node node, PooledObject<SyncDataClient> pooledObject) {
      return pooledObject.getObject() != null
          && pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
