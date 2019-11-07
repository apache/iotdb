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
package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient.addNode_call;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaCluster manages cluster metadata, such as what nodes are in the cluster and data partition.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {

  }

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {
    if (node == thisNode) {
      resultHandler.onError(new AddSelfException());
    }

    if (character == NodeCharacter.LEADER) {
      // node adding must be serialized
      synchronized (logManager) {
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.setIp(node.getIp());
        addNodeLog.setPort(node.getPort());

        AppendLogResult result = sendLogToFollowers(addNodeLog);
        switch (result) {
          case OK:
            resultHandler.onComplete(AGREE);
            return;
          case TIME_OUT:
            resultHandler.onError(new RequestTimeOutException(addNodeLog));
            return;
          case LEADERSHIP_STALE:
          default:
            // if the leader is found, forward to it
        }
      }
    }
    if (character == NodeCharacter.FOLLOWER && leader != null) {
      AsyncClient client = (AsyncClient) connectNode(leader);
      if (client != null) {
        try {
          client.addNode(node, new AsyncMethodCallback<addNode_call>() {
            @Override
            public void onComplete(addNode_call response) {
              try {
                resultHandler.onComplete(response.getResult());
              } catch (TException e) {
                resultHandler.onError(e);
              }
            }

            @Override
            public void onError(Exception exception) {
              resultHandler.onError(exception);
            }
          });
          return;
        } catch (TException e) {
          nodeClientMap.remove(node);
          nodeTransportMap.remove(node).close();
          logger.warn("Cannot connect to node {}", node, e);
        }
      }
    }
    resultHandler.onError(new LeaderUnknownException());
  }

  @Override
  public void apply(Log log) {
    if (log instanceof AddNodeLog) {
      AddNodeLog addNodeLog = (AddNodeLog) log;
      Node newNode = new Node();
      newNode.setIp(addNodeLog.getIp());
      newNode.setPort(addNodeLog.getPort());
      allNodes.add(newNode);
      saveNodes();
    }
  }

  @Override
  void handleCatchUp(Node follower, long followerLastLogIndex) {

  }
}
