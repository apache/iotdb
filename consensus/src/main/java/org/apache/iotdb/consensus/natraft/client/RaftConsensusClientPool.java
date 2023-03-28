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
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class RaftConsensusClientPool {

  private RaftConsensusClientPool() {}

  public static class AsyncRaftServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncRaftServiceClient> {

    private final RaftConfig config;
    private static final String RAFT_CONSENSUS_CLIENT_POOL_THREAD_NAME = "RaftConsensusClientPool";

    public AsyncRaftServiceClientPoolFactory(RaftConfig config) {
      this.config = config;
    }

    @Override
    public KeyedObjectPool<TEndPoint, AsyncRaftServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncRaftServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncRaftServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(config.getRpcConfig().getConnectionTimeoutInMs())
                  .setRpcThriftCompressionEnabled(
                      config.getRpcConfig().isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(
                      config.getRpcConfig().getSelectorNumOfClientManager())
                  .build(),
              RAFT_CONSENSUS_CLIENT_POOL_THREAD_NAME),
          new ClientPoolProperty.Builder<AsyncRaftServiceClient>()
              .setMaxClientNumForEachNode(config.getRpcConfig().getMaxClientNumForEachNode())
              .setCoreClientNumForEachNode(config.getRpcConfig().getCoreClientNumForEachNode())
              .build()
              .getConfig());
    }
  }
}
