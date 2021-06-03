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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.TestOnly;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A LogDispatcher serves a raft leader by queuing logs that the leader wants to send to its
 * followers and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  private RaftMember member;
  private boolean useBatchInLogCatchUp =
      ClusterDescriptor.getInstance().getConfig().isUseBatchInLogCatchUp();
  private List<BlockingQueue<SendLogRequest>> nodeLogQueues = new ArrayList<>();
  private ExecutorService executorService;
  private static ExecutorService serializationService =
      Executors.newFixedThreadPool(
          Runtime.getRuntime().availableProcessors(),
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DispatcherEncoder-%d").build());

  public LogDispatcher(RaftMember member) {
    this.member = member;
    executorService = Executors.newCachedThreadPool();
    for (Node node : member.getAllNodes()) {
      if (!node.equals(member.getThisNode())) {
        nodeLogQueues.add(createQueueAndBindingThread(node));
      }
    }
  }

  @TestOnly
  public void close() throws InterruptedException {
    executorService.shutdownNow();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
  }

  public void offer(SendLogRequest log) {
    // do serialization here to avoid taking LogManager for too long
    if (!nodeLogQueues.isEmpty()) {
      log.serializedLogFuture =
          serializationService.submit(
              () -> {
                ByteBuffer byteBuffer = log.getLog().serialize();
                log.getLog().setByteSize(byteBuffer.array().length);
                return byteBuffer;
              });
    }
    for (int i = 0; i < nodeLogQueues.size(); i++) {
      BlockingQueue<SendLogRequest> nodeLogQueue = nodeLogQueues.get(i);
      try {
        boolean addSucceeded;
        if (ClusterDescriptor.getInstance().getConfig().isWaitForSlowNode()) {
          addSucceeded =
              nodeLogQueue.offer(
                  log,
                  ClusterDescriptor.getInstance().getConfig().getWriteOperationTimeoutMS(),
                  TimeUnit.MILLISECONDS);
        } else {
          addSucceeded = nodeLogQueue.add(log);
        }

        if (!addSucceeded) {
          logger.debug(
              "Log queue[{}] of {} is full, ignore the log to this node", i, member.getName());
        } else {
          log.setEnqueueTime(System.nanoTime());
        }
      } catch (IllegalStateException e) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the log to this node", i, member.getName());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private BlockingQueue<SendLogRequest> createQueueAndBindingThread(Node node) {
    BlockingQueue<SendLogRequest> logBlockingQueue =
        new ArrayBlockingQueue<>(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    int bindingThreadNum = 1;
    for (int i = 0; i < bindingThreadNum; i++) {
      executorService.submit(new DispatcherThread(node, logBlockingQueue));
    }
    return logBlockingQueue;
  }

  public static class SendLogRequest {

    private Log log;
    private AtomicInteger voteCounter;
    private AtomicBoolean leaderShipStale;
    private AtomicLong newLeaderTerm;
    private AppendEntryRequest appendEntryRequest;
    private long enqueueTime;
    private Future<ByteBuffer> serializedLogFuture;

    public SendLogRequest(
        Log log,
        AtomicInteger voteCounter,
        AtomicBoolean leaderShipStale,
        AtomicLong newLeaderTerm,
        AppendEntryRequest appendEntryRequest) {
      this.setLog(log);
      this.setVoteCounter(voteCounter);
      this.setLeaderShipStale(leaderShipStale);
      this.setNewLeaderTerm(newLeaderTerm);
      this.setAppendEntryRequest(appendEntryRequest);
    }

    public AtomicInteger getVoteCounter() {
      return voteCounter;
    }

    public void setVoteCounter(AtomicInteger voteCounter) {
      this.voteCounter = voteCounter;
    }

    public Log getLog() {
      return log;
    }

    public void setLog(Log log) {
      this.log = log;
    }

    public long getEnqueueTime() {
      return enqueueTime;
    }

    public void setEnqueueTime(long enqueueTime) {
      this.enqueueTime = enqueueTime;
    }

    public AtomicBoolean getLeaderShipStale() {
      return leaderShipStale;
    }

    public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
      this.leaderShipStale = leaderShipStale;
    }

    public AtomicLong getNewLeaderTerm() {
      return newLeaderTerm;
    }

    void setNewLeaderTerm(AtomicLong newLeaderTerm) {
      this.newLeaderTerm = newLeaderTerm;
    }

    public AppendEntryRequest getAppendEntryRequest() {
      return appendEntryRequest;
    }

    public void setAppendEntryRequest(AppendEntryRequest appendEntryRequest) {
      this.appendEntryRequest = appendEntryRequest;
    }

    @Override
    public String toString() {
      return "SendLogRequest{" + "log=" + log + '}';
    }
  }

  class DispatcherThread implements Runnable {

    private Node receiver;
    private BlockingQueue<SendLogRequest> logBlockingDeque;
    private List<SendLogRequest> currBatch = new ArrayList<>();
    private Peer peer;

    DispatcherThread(Node receiver, BlockingQueue<SendLogRequest> logBlockingDeque) {
      this.receiver = receiver;
      this.logBlockingDeque = logBlockingDeque;
      this.peer =
          member
              .getPeerMap()
              .computeIfAbsent(receiver, r -> new Peer(member.getLogManager().getLastLogIndex()));
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
          for (SendLogRequest request : currBatch) {
            request.getAppendEntryRequest().entry = request.serializedLogFuture.get();
          }
          sendBatchLogs(currBatch);
          currBatch.clear();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
      logger.info("Dispatcher exits");
    }

    private void appendEntriesAsync(
        List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch)
        throws TException {
      AsyncMethodCallback<Long> handler = new AppendEntriesHandler(currBatch);
      AsyncClient client = member.getSendLogAsyncClient(receiver);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: append entries {} with {} logs", member.getName(), receiver, logList.size());
      }
      if (client != null) {
        client.appendEntries(request, handler);
      }
    }

    private void appendEntriesSync(
        List<ByteBuffer> logList, AppendEntriesRequest request, List<SendLogRequest> currBatch) {

      long startTime = Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.getOperationStartTime();
      if (!member.waitForPrevLog(peer, currBatch.get(0).getLog())) {
        logger.warn(
            "{}: node {} timed out when appending {}",
            member.getName(),
            receiver,
            currBatch.get(0).getLog());
        return;
      }
      Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);

      Client client = member.getSyncClient(receiver);
      if (client == null) {
        logger.error("No available client for {}", receiver);
        return;
      }
      AsyncMethodCallback<Long> handler = new AppendEntriesHandler(currBatch);
      startTime = Timer.Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
      try {
        long result = client.appendEntries(request);
        Timer.Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(startTime);
        if (result != -1 && logger.isInfoEnabled()) {
          logger.info(
              "{}: Append {} logs to {}, resp: {}",
              member.getName(),
              logList.size(),
              receiver,
              result);
        }
        handler.onComplete(result);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        handler.onError(e);
        logger.warn("Failed logs: {}, first index: {}", logList, request.prevLogIndex + 1);
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }

    private AppendEntriesRequest prepareRequest(
        List<ByteBuffer> logList, List<SendLogRequest> currBatch, int firstIndex) {
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
      request.setPrevLogIndex(currBatch.get(firstIndex).getLog().getCurrLogIndex() - 1);
      try {
        request.setPrevLogTerm(currBatch.get(firstIndex).getAppendEntryRequest().prevLogTerm);
      } catch (Exception e) {
        logger.error("getTerm failed for newly append entries", e);
      }
      return request;
    }

    private void sendLogs(List<SendLogRequest> currBatch) throws TException {
      int logIndex = 0;
      logger.debug(
          "send logs from index {} to {}",
          currBatch.get(0).getLog().getCurrLogIndex(),
          currBatch.get(currBatch.size() - 1).getLog().getCurrLogIndex());
      while (logIndex < currBatch.size()) {
        long logSize = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
        List<ByteBuffer> logList = new ArrayList<>();
        int prevIndex = logIndex;

        for (; logIndex < currBatch.size(); logIndex++) {
          long curSize = currBatch.get(logIndex).getAppendEntryRequest().entry.array().length;
          if (logSize - curSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
            break;
          }
          logSize -= curSize;
          Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
              currBatch.get(logIndex).getLog().getCreateTime());
          logList.add(currBatch.get(logIndex).getAppendEntryRequest().entry);
        }

        AppendEntriesRequest appendEntriesRequest = prepareRequest(logList, currBatch, prevIndex);
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        } else {
          appendEntriesSync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        }
        for (; prevIndex < logIndex; prevIndex++) {
          Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_END.calOperationCostTimeFromStart(
              currBatch.get(prevIndex).getLog().getCreateTime());
        }
      }
    }

    private void sendBatchLogs(List<SendLogRequest> currBatch) throws TException {
      if (currBatch.size() > 1) {
        if (useBatchInLogCatchUp) {
          sendLogs(currBatch);
        } else {
          for (SendLogRequest batch : currBatch) {
            sendLog(batch);
          }
        }
      } else {
        sendLog(currBatch.get(0));
      }
    }

    private void sendLog(SendLogRequest logRequest) {
      Timer.Statistic.LOG_DISPATCHER_LOG_IN_QUEUE.calOperationCostTimeFromStart(
          logRequest.getLog().getCreateTime());
      member.sendLogToFollower(
          logRequest.getLog(),
          logRequest.getVoteCounter(),
          receiver,
          logRequest.getLeaderShipStale(),
          logRequest.getNewLeaderTerm(),
          logRequest.getAppendEntryRequest());
      Timer.Statistic.LOG_DISPATCHER_FROM_CREATE_TO_END.calOperationCostTimeFromStart(
          logRequest.getLog().getCreateTime());
    }

    class AppendEntriesHandler implements AsyncMethodCallback<Long> {

      private final List<AsyncMethodCallback<Long>> singleEntryHandlers;

      private AppendEntriesHandler(List<SendLogRequest> batch) {
        singleEntryHandlers = new ArrayList<>(batch.size());
        for (SendLogRequest sendLogRequest : batch) {
          AppendNodeEntryHandler handler =
              getAppendNodeEntryHandler(
                  sendLogRequest.getLog(),
                  sendLogRequest.getVoteCounter(),
                  receiver,
                  sendLogRequest.getLeaderShipStale(),
                  sendLogRequest.getNewLeaderTerm(),
                  peer);
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

      private AppendNodeEntryHandler getAppendNodeEntryHandler(
          Log log,
          AtomicInteger voteCounter,
          Node node,
          AtomicBoolean leaderShipStale,
          AtomicLong newLeaderTerm,
          Peer peer) {
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
  }
}
