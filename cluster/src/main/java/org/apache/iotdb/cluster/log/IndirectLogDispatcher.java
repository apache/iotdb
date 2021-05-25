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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.iotdb.cluster.log.LogDispatcher.DispatcherThread;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.ClusterUtils;

/**
 * IndirectLogDispatcher sends entries only to a pre-selected subset of followers instead of all
 * followers and let the selected followers to broadcast the log to other followers.
 */
public class IndirectLogDispatcher extends LogDispatcher {

  private Map<Node, List<Node>> directToIndirectFollowerMap = new HashMap<>();

  public IndirectLogDispatcher(RaftMember member) {
    super(member);
    recalculateDirectFollowerMap();
  }

  @Override
  LogDispatcher.DispatcherThread newDispatcherThread(Node node,
      BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  @Override
  void createQueueAndBindingThreads() {
    for (Node node : directToIndirectFollowerMap.keySet()) {
      nodeLogQueues.add(createQueueAndBindingThread(node));
    }
  }

  public void recalculateDirectFollowerMap() {
    List<Node> allNodes = new ArrayList<>(member.getAllNodes());
    allNodes.removeIf(n -> ClusterUtils.isNodeEquals(n, member.getThisNode()));
    QueryCoordinator instance = QueryCoordinator.getINSTANCE();
    List<Node> orderedNodes = instance.reorderNodes(allNodes);

    synchronized (this) {
      directToIndirectFollowerMap.clear();
      for (int i = 0, j = orderedNodes.size() - 1; i <= j; i++, j--) {
        if (i != j) {
          directToIndirectFollowerMap.put(orderedNodes.get(i),
              Collections.singletonList(orderedNodes.get(j)));
        } else {
          directToIndirectFollowerMap.put(orderedNodes.get(i), Collections.emptyList());
        }
      }
    }
  }

  class DispatcherThread extends LogDispatcher.DispatcherThread {

    DispatcherThread(Node receiver,
        BlockingQueue<SendLogRequest> logBlockingDeque) {
      super(receiver, logBlockingDeque);
    }

    @Override
    void sendLog(SendLogRequest logRequest) {
      Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
          logRequest.getLog().getCreateTime());
      member.sendLogToFollower(
          logRequest.getLog(),
          logRequest.getVoteCounter(),
          receiver,
          logRequest.getLeaderShipStale(),
          logRequest.getNewLeaderTerm(),
          logRequest.getAppendEntryRequest(), directToIndirectFollowerMap.get(receiver));
      Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_END.calOperationCostTimeFromStart(
          logRequest.getLog().getCreateTime());
    }
  }
}
