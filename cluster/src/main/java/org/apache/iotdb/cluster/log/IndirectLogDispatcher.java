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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClusterUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * IndirectLogDispatcher sends entries only to a pre-selected subset of followers instead of all
 * followers and let the selected followers to broadcast the log to other followers.
 */
public class IndirectLogDispatcher extends LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(IndirectLogDispatcher.class);
  private Map<Node, List<Node>> directToIndirectFollowerMap;

  public IndirectLogDispatcher(RaftMember member) {
    super(member);
  }

  @Override
  LogDispatcher.DispatcherThread newDispatcherThread(
      Node node, BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  @Override
  void createQueueAndBindingThreads() {
    recalculateDirectFollowerMap();
  }

  public void recalculateDirectFollowerMap() {
    List<Node> allNodes = new ArrayList<>(member.getAllNodes());
    allNodes.removeIf(n -> ClusterUtils.isNodeEquals(n, member.getThisNode()));
    QueryCoordinator instance = QueryCoordinator.getINSTANCE();
    List<Node> orderedNodes = instance.reorderNodes(allNodes);
    synchronized (this) {
      executorService.shutdown();
      try {
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Dispatcher thread pool of {} cannot be shutdown within 10s", member);
      }
      executorService = Executors.newCachedThreadPool();

      directToIndirectFollowerMap = new HashMap<>();
      for (int i = 0, j = orderedNodes.size() - 1; i <= j; i++, j--) {
        if (i != j) {
          directToIndirectFollowerMap.put(
              orderedNodes.get(i), Collections.singletonList(orderedNodes.get(j)));
        } else {
          directToIndirectFollowerMap.put(orderedNodes.get(i), Collections.emptyList());
        }
      }
    }

    for (Node node : directToIndirectFollowerMap.keySet()) {
      nodesLogQueues.put(node, createQueueAndBindingThread(node));
    }
  }

  class DispatcherThread extends LogDispatcher.DispatcherThread {

    DispatcherThread(Node receiver, BlockingQueue<SendLogRequest> logBlockingDeque) {
      super(receiver, logBlockingDeque);
    }

    @Override
    void sendLog(SendLogRequest logRequest) {
      logRequest.getAppendEntryRequest().setSubReceivers(directToIndirectFollowerMap.get(receiver));
      super.sendLog(logRequest);
    }

    @Override
    protected AppendEntriesRequest prepareRequest(
        List<ByteBuffer> logList, List<SendLogRequest> currBatch, int firstIndex) {
      return super.prepareRequest(logList, currBatch, firstIndex)
          .setSubReceivers(directToIndirectFollowerMap.get(receiver));
    }
  }
}
