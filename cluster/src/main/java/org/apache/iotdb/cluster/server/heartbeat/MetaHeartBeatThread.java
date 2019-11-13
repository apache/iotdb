/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaHeartBeatThread extends HeartBeatThread {

  private static final Logger logger = LoggerFactory.getLogger(MetaHeartBeatThread.class);
  private MetaClusterServer raftServer;

  public MetaHeartBeatThread(MetaClusterServer raftServer) {
    super(raftServer);
    this.raftServer = raftServer;
  }

  @Override
  void sendHeartbeat(Node node, AsyncClient client) {
    // if the node's identifier is not clear, require it
    request.setRequireIdentifier(!node.isSetNodeIdentifier());
    synchronized (raftServer.getIdConflictNodes()) {
      request.unsetRegenerateIdentifier();
      Integer conflictId = raftServer.getIdConflictNodes().get(node);
      if (conflictId != null) {
        request.setRegenerateIdentifier(true);
      }
    }

    // if the node requires the node list and it is ready (all nodes' ids are known), send it
    if (raftServer.isNodeBlind(node)) {
      if (raftServer.allNodesIdKnown()) {
        logger.debug("Send node list to {}", node);
        request.setNodeSet(raftServer.getAllNodes());
        // if the node does not receive the list, it will require it in the next heartbeat, so
        // we can remove it now
        raftServer.removeBlindNode(node);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Known nodes: {}, all nodes: {}", raftServer.getIdNodeMap(),
              raftServer.getAllNodes());
        }
      }
    }

    super.sendHeartbeat(node, client);
  }
}
