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

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else or there may be conflicts.
 */
public class AsyncMetaHeartbeatClient extends AsyncMetaClient {

  private AsyncMetaHeartbeatClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      Node node,
      AsyncClientPool pool)
      throws IOException {
    super(
        protocolFactory,
        clientManager,
        TNonblockingSocketWrapper.wrap(
            node.getInternalIp(),
            node.getMetaPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET,
            RaftServer.getConnectionTimeoutInMS()));
    this.node = node;
    this.pool = pool;
  }

  public static class FactoryAsync extends AsyncClientFactory {

    public FactoryAsync(TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
    }

    @Override
    public RaftService.AsyncClient getAsyncClient(Node node, AsyncClientPool pool)
        throws IOException {
      TAsyncClientManager manager = managers[clientCnt.incrementAndGet() % managers.length];
      manager = manager == null ? new TAsyncClientManager() : manager;
      return new AsyncMetaHeartbeatClient(protocolFactory, manager, node, pool);
    }
  }

  @Override
  public String toString() {
    return "AsyncMetaHeartbeatClient{"
        + "node="
        + super.getNode()
        + ","
        + "metaHeartbeatPort="
        + (super.getNode().getMetaPort() + ClusterUtils.META_HEARTBEAT_PORT_OFFSET)
        + '}';
  }
}
