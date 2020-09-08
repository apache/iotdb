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

import static org.apache.iotdb.cluster.server.Timer.currentBatchHisto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.Timer;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.LogCatchUpInBatchHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.jdbc.Config;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
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
      Timer.queueHisto[nodeLogQueue.size()]++;
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
    public long createTime;

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
          currBatch.add(poll);
          logBlockingDeque.drainTo(currBatch);
          if (logger.isDebugEnabled()) {
            logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
          }
          Timer.currentBatchHisto[currBatch.size()]++;

          List<ByteBuffer> logList = new ArrayList<>();;
          for (SendLogRequest request : currBatch) {
            logList.add(request.appendEntryRequest.entry);
          }
          AppendEntriesRequest appendEntriesReques = prepareRequest(logList, 0, currBatch);
          if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
            appendEntriesAsync(logList, appendEntriesReques, new ArrayList<>(currBatch));
          } else {
            appendEntriesSync(logList, appendEntriesReques, currBatch);
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

    private boolean appendEntriesAsync(List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch)
        throws TException, InterruptedException {
      AtomicBoolean appendSucceed = new AtomicBoolean(false);

      AsyncMethodCallback<Long> handler = new AppendEntriesHandler(currBatch);
      synchronized (appendSucceed) {
        appendSucceed.set(false);
        AsyncClient client = member.getAsyncClient(receiver);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Catching up {} with {} logs", member.getName(), receiver, logList.size());
        }
        client.appendEntries(request, handler);
        appendSucceed.wait(ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS());
      }
      return appendSucceed.get();
    }

    private boolean appendEntriesSync(List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch) {
      AtomicBoolean appendSucceed = new AtomicBoolean(false);

      Client client = member.getSyncClient(receiver);
      AsyncMethodCallback<Long> handler = new AppendEntriesHandler(currBatch);
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Catching up {} with {} logs", member.getName(), receiver, logList.size());
        }
        long result = client.appendEntries(request);
        handler.onComplete(result);
        return appendSucceed.get();
      } catch (TException e) {
        handler.onError(e);
        logger.warn("Failed logs: {}, first index: {}", logList, request.prevLogIndex + 1);
        return false;
      } finally {
        member.putBackSyncClient(client);
      }
    }

    private AppendEntriesRequest prepareRequest(List<ByteBuffer> logList, int startPos, List<SendLogRequest> currBatch) {
      AppendEntriesRequest request = new AppendEntriesRequest();

      if (member.getHeader() != null) {
        request.setHeader(member.getHeader());
      }
      request.setLeader(member.getThisNode());
      request.setLeaderCommit(member.getLogManager().getCommitLogIndex());

      synchronized (member.getTerm()) {
        request.setTerm(member.getTerm().get());
      }

      request.setEntries(logList);
      // set index for raft
      request.setPrevLogIndex(currBatch.get(startPos).log.getCurrLogIndex() - 1);
      if (startPos != 0) {
        request.setPrevLogTerm(currBatch.get(startPos - 1).log.getCurrLogTerm());
      } else {
        try {
          request.setPrevLogTerm(
              member.getLogManager().getTerm(currBatch.get(0).log.getCurrLogIndex() - 1));
        } catch (Exception e) {
          logger.error("getTerm failed for newly append entries", e);
        }
      }
      return request;
    }

    private void sendLog(SendLogRequest logRequest) {
      Timer.logDispatcherLogInQueue.add(System.nanoTime() - logRequest.createTime);
      member.sendLogToFollower(logRequest.log, logRequest.voteCounter, receiver,
          logRequest.leaderShipStale, logRequest.newLeaderTerm, logRequest.appendEntryRequest);
      Timer.logDispatcherFromCreateToEnd.add(System.nanoTime() - logRequest.createTime);
    }

    class AppendEntriesHandler implements AsyncMethodCallback<Long> {

      private final List<AsyncMethodCallback<Long>> singleEntryHandlers;

      private AppendEntriesHandler(List<SendLogRequest> batch) {
        singleEntryHandlers = new ArrayList<>(batch.size());
        for (SendLogRequest sendLogRequest : batch) {
          AppendNodeEntryHandler handler = getAppendNodeEntryHandler(sendLogRequest.log, sendLogRequest.voteCounter
              , receiver,
              sendLogRequest.leaderShipStale, sendLogRequest.newLeaderTerm, peer);
          singleEntryHandlers.add(handler);
        }
      }

      @Override
      public void onComplete(Long aLong) {
        for (AsyncMethodCallback<Long> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onComplete(aLong);
        }
      }

      @Override
      public void onError(Exception e) {
        for (AsyncMethodCallback<Long> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onError(e);
        }
      }
    }
  }



  public AppendNodeEntryHandler getAppendNodeEntryHandler(Log log, AtomicInteger voteCounter,
      Node node, AtomicBoolean leaderShipStale, AtomicLong newLeaderTerm, Peer peer) {
    AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
    handler.setReceiver(node);
    handler.setVoteCounter(voteCounter);
    handler.setLeaderShipStale(leaderShipStale);
    handler.setLog(log);
    handler.setMember(member);
    handler.setPeer(peer);
    handler.setReceiverTerm(newLeaderTerm);
    return handler;
  }
}
