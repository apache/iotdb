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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.Timer;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LogDispatcher servers a raft leader by queuing logs that the leader wants to send to the
 * follower and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  private RaftMember member;
  private List<BlockingQueue<SendLogRequest>> nodeLogQueues =
      new ArrayList<>();
  private ExecutorService executorService;

  public LogDispatcher(RaftMember member) {
    this.member = member;
    executorService = Executors.newCachedThreadPool();
    for (Node node : member.getAllNodes()) {
      if (!node.equals(member.getThisNode())) {
        nodeLogQueues.add(createQueueAndBindingThread(node));
      }
    }
  }

  public void offer(SendLogRequest log) {
    for (BlockingQueue<SendLogRequest> nodeLogQueue : nodeLogQueues) {
      nodeLogQueue.offer(log);
      log.enqueueTime = System.nanoTime();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} is enqueued in {} queues", log.log, nodeLogQueues.size());
    }
  }

  private BlockingQueue<SendLogRequest> createQueueAndBindingThread(Node node) {
    BlockingQueue<SendLogRequest> logBlockingQueue =
        new ArrayBlockingQueue<>(4096);
    int bindingThreadNum = 1;
    for (int i = 0; i < bindingThreadNum; i++) {
      executorService.submit(new DispatcherThread(node, logBlockingQueue));
    }
    return logBlockingQueue;
  }

  public static class SendLogRequest {

    public Log log;
    public AtomicInteger voteCounter;
    public AtomicBoolean leaderShipStale;
    public AtomicLong newLeaderTerm;
    public AppendEntryRequest appendEntryRequest;
    public long enqueueTime;

    public SendLogRequest(Log log, AtomicInteger voteCounter,
        AtomicBoolean leaderShipStale, AtomicLong newLeaderTerm,
        AppendEntryRequest appendEntryRequest) {
      this.log = log;
      this.voteCounter = voteCounter;
      this.leaderShipStale = leaderShipStale;
      this.newLeaderTerm = newLeaderTerm;
      this.appendEntryRequest = appendEntryRequest;
    }
  }

  class DispatcherThread implements Runnable {

    private Node receiver;
    private Peer peer;
    private BlockingQueue<SendLogRequest> logBlockingDeque;
    private List<SendLogRequest> currBatch = new ArrayList<>();

    DispatcherThread(Node receiver,
        BlockingQueue<SendLogRequest> logBlockingDeque) {
      this.receiver = receiver;
      this.logBlockingDeque = logBlockingDeque;
      this.peer = member.getPeerMap().computeIfAbsent(receiver,
          r -> new Peer(member.getLogManager().getLastLogIndex()));
    }

    @Override
    public void run() {
      Thread.currentThread().setName("LogDispatcher-" + member.getName() + "-" + receiver);
      try {
        while (!Thread.interrupted()) {
          SendLogRequest poll = logBlockingDeque.take();
          long inQueueTime = System.nanoTime() - poll.enqueueTime;
          Timer.logDispatcherLogInQueueCounter.incrementAndGet();
          Timer.logDispatcherLogInQueueMS.addAndGet(inQueueTime);
          currBatch.add(poll);
          logBlockingDeque.drainTo(currBatch);
          if (logger.isDebugEnabled()) {
            logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
          }
          for (SendLogRequest request : currBatch) {
            sendLog(request);
          }
          currBatch.clear();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
      logger.info("Dispatcher exits");
    }

    private void sendLog(SendLogRequest logRequest) {
      member.sendLogToFollower(logRequest.log, logRequest.voteCounter, receiver,
          logRequest.leaderShipStale, logRequest.newLeaderTerm, logRequest.appendEntryRequest);
    }
  }
}
