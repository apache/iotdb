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

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;

/**
 * Notice: Because a client will be returned to a pool immediately after a successful request, you
 * should not cache it anywhere else or there may be conflicts.
 */
public class SyncDataHeartbeatClient extends TSDataServiceClient {

  private SyncDataHeartbeatClient(
      TProtocolFactory protocolFactory, Node target, SyncClientPool pool)
      throws TTransportException {
    // the difference of the two clients lies in the port
    super(
        protocolFactory,
        target.getInternalIp(),
        target.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET,
        ClusterConstant.getConnectionTimeoutInMS(),
        target,
        pool);
  }

  @Override
  public String toString() {
    return String.format(
        "SyncDataHBClient (ip = %s, port = %d, id = %d)",
        target.getInternalIp(),
        target.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET,
        target.getNodeIdentifier());
  }

  public static class Factory implements SyncClientFactory {

    private TProtocolFactory protocolFactory;

    public Factory(TProtocolFactory protocolFactory) {
      this.protocolFactory = protocolFactory;
    }

    @Override
    public SyncDataHeartbeatClient getSyncClient(Node node, SyncClientPool pool)
        throws TTransportException {
      return new SyncDataHeartbeatClient(protocolFactory, node, pool);
    }

    @Override
    public String nodeInfo(Node node) {
      return String.format(
          "DataHBNode (ip = %s, port = %d, id = %d)",
          node.getInternalIp(),
          node.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET,
          node.getNodeIdentifier());
    }
  }
}
