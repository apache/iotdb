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

import org.apache.iotdb.cluster.log.logtypes.FragmentedLog;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class FragmentedLogDispatcher extends LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(FragmentedLogDispatcher.class);

  public FragmentedLogDispatcher(RaftMember member) {
    super(member);
  }

  public void offer(SendLogRequest request) {
    if (!(request.getVotingLog().getLog() instanceof FragmentedLog)) {
      super.offer(request);
      return;
    }
    // do serialization here to avoid taking LogManager for too long

    long startTime = Statistic.LOG_DISPATCHER_LOG_ENQUEUE.getOperationStartTime();
    request.getVotingLog().getLog().setEnqueueTime(System.nanoTime());

    int i = 0;
    for (Pair<Node, BlockingQueue<SendLogRequest>> entry : nodesLogQueuesList) {
      BlockingQueue<SendLogRequest> nodeLogQueue = entry.right;
      SendLogRequest fragmentedRequest = new SendLogRequest(request);
      fragmentedRequest.setVotingLog(new VotingLog(request.getVotingLog()));
      fragmentedRequest
          .getVotingLog()
          .setLog(new FragmentedLog((FragmentedLog) request.getVotingLog().getLog(), i++));

      boolean addSucceeded = addToQueue(nodeLogQueue, request);

      if (!addSucceeded) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the request to this node",
            entry.left,
            member.getName());
      } else {
        request.setEnqueueTime(System.nanoTime());
      }
    }
    Statistic.LOG_DISPATCHER_LOG_ENQUEUE.calOperationCostTimeFromStart(startTime);

    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.LOG_DISPATCHER_FROM_CREATE_TO_ENQUEUE.calOperationCostTimeFromStart(
          request.getVotingLog().getLog().getCreateTime());
    }
  }

  LogDispatcher.DispatcherThread newDispatcherThread(
      Node node, BlockingQueue<SendLogRequest> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  class DispatcherThread extends LogDispatcher.DispatcherThread {

    DispatcherThread(Node receiver, BlockingQueue<SendLogRequest> logBlockingDeque) {
      super(receiver, logBlockingDeque);
    }

    @Override
    protected void serializeEntries() {
      for (SendLogRequest request : currBatch) {
        Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
            request.getVotingLog().getLog().getEnqueueTime());
        Statistic.LOG_DISPATCHER_FROM_CREATE_TO_DEQUEUE.calOperationCostTimeFromStart(
            request.getVotingLog().getLog().getCreateTime());
        long start = Statistic.RAFT_SENDER_SERIALIZE_LOG.getOperationStartTime();
        request.getAppendEntryRequest().entry = request.getVotingLog().getLog().serialize();
        Statistic.RAFT_SENDER_SERIALIZE_LOG.calOperationCostTimeFromStart(start);
      }
    }
  }
}
