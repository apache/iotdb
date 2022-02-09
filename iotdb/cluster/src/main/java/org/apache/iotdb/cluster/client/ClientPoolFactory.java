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

package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.time.Duration;

public class ClientPoolFactory {

  protected long waitClientTimeoutMS;
  protected int maxConnectionForEachNode;
  private final TProtocolFactory protocolFactory;
  private GenericKeyedObjectPoolConfig poolConfig;
  private IClientManager clientManager;

  public ClientPoolFactory() {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    this.waitClientTimeoutMS = config.getWaitClientTimeoutMS();
    this.maxConnectionForEachNode = config.getMaxClientPerNodePerMember();
    protocolFactory =
        config.isRpcThriftCompressionEnabled()
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();
    poolConfig = new GenericKeyedObjectPoolConfig();
    poolConfig.setMaxTotalPerKey(maxConnectionForEachNode);
    poolConfig.setMaxWait(Duration.ofMillis(waitClientTimeoutMS));
    poolConfig.setTestOnReturn(true);
    poolConfig.setTestOnBorrow(true);
  }

  public void setClientManager(IClientManager clientManager) {
    this.clientManager = clientManager;
  }

  public GenericKeyedObjectPool<Node, RaftService.Client> createSyncDataPool(
      ClientCategory category) {
    return new GenericKeyedObjectPool<>(
        new SyncDataClient.SyncDataClientFactory(protocolFactory, category, clientManager),
        poolConfig);
  }

  public GenericKeyedObjectPool<Node, RaftService.Client> createSyncMetaPool(
      ClientCategory category) {
    return new GenericKeyedObjectPool<>(
        new SyncMetaClient.SyncMetaClientFactory(protocolFactory, category, clientManager),
        poolConfig);
  }

  public GenericKeyedObjectPool<Node, RaftService.AsyncClient> createAsyncDataPool(
      ClientCategory category) {
    return new GenericKeyedObjectPool<>(
        new AsyncDataClient.AsyncDataClientFactory(protocolFactory, category, clientManager),
        poolConfig);
  }

  public GenericKeyedObjectPool<Node, RaftService.AsyncClient> createAsyncMetaPool(
      ClientCategory category) {
    return new GenericKeyedObjectPool<>(
        new AsyncMetaClient.AsyncMetaClientFactory(protocolFactory, category, clientManager),
        poolConfig);
  }

  public GenericKeyedObjectPool<Node, RaftService.AsyncClient> createSingleManagerAsyncDataPool() {
    return new GenericKeyedObjectPool<>(
        new AsyncDataClient.SingleManagerFactory(protocolFactory, clientManager), poolConfig);
  }
}
