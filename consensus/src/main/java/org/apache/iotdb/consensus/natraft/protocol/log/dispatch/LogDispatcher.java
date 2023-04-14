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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;
import org.apache.iotdb.tsfile.compress.ICompressor;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.consensus.natraft.utils.NodeUtils.unionNodes;

/**
 * A LogDispatcher serves a raft leader by queuing logs that the leader wants to send to its
 * followers and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  protected RaftMember member;
  private RaftConfig config;
  protected List<Peer> allNodes;
  protected List<Peer> newNodes;
  protected Map<Peer, BlockingQueue<VotingEntry>> nodesLogQueuesMap = new HashMap<>();
  protected Map<Peer, Boolean> nodesEnabled;
  protected Map<Peer, RateLimiter> nodesRateLimiter = new HashMap<>();
  protected Map<Peer, Double> nodesRate = new HashMap<>();
  protected Map<Peer, ExecutorService> executorServices = new HashMap<>();
  protected boolean queueOrdered;
  protected boolean enableCompressedDispatching;
  protected ICompressor compressor;

  public int bindingThreadNum;
  public int maxBatchSize = 10;

  public LogDispatcher(RaftMember member, RaftConfig config) {
    this.member = member;
    this.config = config;
    this.queueOrdered = !(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance());
    this.enableCompressedDispatching = config.isEnableCompressedDispatching();
    this.compressor = ICompressor.getCompressor(config.getDispatchingCompressionType());
    this.bindingThreadNum = config.getDispatcherBindingThreadNum();
    this.allNodes = member.getAllNodes();
    this.newNodes = member.getNewNodes();
    createQueueAndBindingThreads(unionNodes(allNodes, newNodes));
    maxBatchSize = config.getLogNumInBatch();
  }

  public void updateRateLimiter() {
    logger.info("{}: TEndPoint rates: {}", member.getName(), nodesRate);
    for (Entry<Peer, Double> nodeDoubleEntry : nodesRate.entrySet()) {
      nodesRateLimiter.put(
          nodeDoubleEntry.getKey(), RateLimiter.create(nodeDoubleEntry.getValue()));
    }
  }

  void createQueue(Peer node) {
    BlockingQueue<VotingEntry> logBlockingQueue;
    logBlockingQueue = new ArrayBlockingQueue<>(config.getMaxNumOfLogsInMem());
    nodesLogQueuesMap.put(node, logBlockingQueue);
    nodesRate.put(node, Double.MAX_VALUE);
    nodesRateLimiter.put(node, RateLimiter.create(Double.MAX_VALUE));

    for (int i = 0; i < bindingThreadNum; i++) {
      executorServices
          .computeIfAbsent(node, n -> createPool(node))
          .submit(newDispatcherThread(node, logBlockingQueue));
    }
  }

  ExecutorService createPool(Peer node) {
    return IoTDBThreadPoolFactory.newCachedThreadPool(
        "LogDispatcher-"
            + member.getName()
            + "-"
            + node.getEndpoint().getIp()
            + "-"
            + node.getEndpoint().getPort()
            + "-"
            + node.getNodeId());
  }

  void createQueueAndBindingThreads(Collection<Peer> peers) {
    for (Peer node : peers) {
      if (!node.equals(member.getThisNode())) {
        createQueue(node);
      }
    }
    updateRateLimiter();
  }

  @TestOnly
  public void close() throws InterruptedException {
    for (Entry<Peer, ExecutorService> entry : executorServices.entrySet()) {
      ExecutorService pool = entry.getValue();
      pool.shutdownNow();
      boolean closeSucceeded = pool.awaitTermination(10, TimeUnit.SECONDS);
      if (!closeSucceeded) {
        logger.warn("Cannot shut down dispatcher pool of {}-{}", member.getName(), entry.getKey());
      }
    }
  }

  protected boolean addToQueue(BlockingQueue<VotingEntry> nodeLogQueue, VotingEntry request) {
    synchronized (nodeLogQueue) {
      boolean added = nodeLogQueue.add(request);
      if (added) {
        nodeLogQueue.notifyAll();
      }
      return added;
    }
  }

  public void offer(VotingEntry request) {

    for (Entry<Peer, BlockingQueue<VotingEntry>> entry : nodesLogQueuesMap.entrySet()) {
      if (nodesEnabled != null && !this.nodesEnabled.getOrDefault(entry.getKey(), false)) {
        continue;
      }

      BlockingQueue<VotingEntry> nodeLogQueue = entry.getValue();
      try {
        boolean addSucceeded = addToQueue(nodeLogQueue, request);

        if (!addSucceeded) {
          logger.debug(
              "Log queue[{}] of {} is full, ignore the request to this node",
              entry.getKey(),
              member.getName());
        }
      } catch (IllegalStateException e) {
        logger.debug(
            "Log queue[{}] of {} is full, ignore the request to this node",
            entry.getKey(),
            member.getName());
      }
    }
  }

  DispatcherThread newDispatcherThread(Peer node, BlockingQueue<VotingEntry> logBlockingQueue) {
    return new DispatcherThread(node, logBlockingQueue);
  }

  public void applyNewNodes() {
    allNodes = newNodes;
    newNodes = null;

    List<Peer> nodesToRemove = new ArrayList<>();
    for (Entry<Peer, ExecutorService> entry : executorServices.entrySet()) {
      if (!allNodes.contains(entry.getKey())) {
        nodesToRemove.add(entry.getKey());
      }
    }
    for (Peer peer : nodesToRemove) {
      ExecutorService executorService = executorServices.remove(peer);
      executorService.shutdownNow();
      nodesRate.remove(peer);
      nodesRateLimiter.remove(peer);
      nodesEnabled.remove(peer);
      nodesLogQueuesMap.remove(peer);
    }
  }

  protected class DispatcherThread implements Runnable {

    Peer receiver;
    private final BlockingQueue<VotingEntry> logBlockingDeque;
    protected List<VotingEntry> currBatch = new ArrayList<>();
    private final String baseName;

    protected DispatcherThread(Peer receiver, BlockingQueue<VotingEntry> logBlockingDeque) {
      this.receiver = receiver;
      this.logBlockingDeque = logBlockingDeque;
      baseName = "LogDispatcher-" + member.getName() + "-" + receiver;
    }

    @Override
    public void run() {
      if (logger.isDebugEnabled()) {
        Thread.currentThread().setName(baseName);
      }
      try {
        while (!Thread.interrupted()) {
          synchronized (logBlockingDeque) {
            VotingEntry poll = logBlockingDeque.poll();
            if (poll != null) {
              currBatch.add(poll);
              logBlockingDeque.drainTo(currBatch, maxBatchSize - 1);
            } else {
              logBlockingDeque.wait(10);
              continue;
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
          }
          serializeEntries();
          if (!queueOrdered) {
            currBatch.sort(Comparator.comparingLong(s -> s.getEntry().getCurrLogIndex()));
          }
          sendLogs(currBatch);
          currBatch.clear();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
      logger.info("Dispatcher exits");
    }

    protected void serializeEntries() throws InterruptedException {
      for (VotingEntry request : currBatch) {

        request.getAppendEntryRequest().entry = request.getEntry().serialize();
        request.getEntry().setByteSize(request.getAppendEntryRequest().entry.limit());
        logger.debug(
            "{}/{}={}",
            request.getEntry().estimateSize(),
            request.getAppendEntryRequest().entry.remaining(),
            request.getEntry().estimateSize()
                * 1.0
                / request.getAppendEntryRequest().entry.remaining());
      }
    }

    private void appendEntriesAsync(
        List<ByteBuffer> logList, AppendEntriesRequest request, List<VotingEntry> currBatch) {
      AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
      AsyncRaftServiceClient client = member.getClient(receiver.getEndpoint());
      try {
        long startTime = Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
        AppendEntryResult appendEntryResult = SyncClientAdaptor.appendEntries(client, request);
        Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(startTime);
        handler.onComplete(appendEntryResult);
      } catch (Exception e) {
        handler.onError(e);
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: append entries {} with {} logs", member.getName(), receiver, logList.size());
      }
    }

    private void appendEntriesAsync(
        List<ByteBuffer> logList,
        AppendCompressedEntriesRequest request,
        List<VotingEntry> currBatch) {
      AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
      AsyncRaftServiceClient client = member.getClient(receiver.getEndpoint());
      try {
        long startTime = Statistic.RAFT_SENDER_SEND_LOG.getOperationStartTime();
        AppendEntryResult appendEntryResult =
            SyncClientAdaptor.appendCompressedEntries(client, request);
        Statistic.RAFT_SENDER_SEND_LOG.calOperationCostTimeFromStart(startTime);
        handler.onComplete(appendEntryResult);
      } catch (Exception e) {
        handler.onError(e);
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: append entries {} with {} logs", member.getName(), receiver, logList.size());
      }
    }

    protected AppendEntriesRequest prepareRequest(List<ByteBuffer> logList) {
      AppendEntriesRequest request = new AppendEntriesRequest();

      request.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
      request.setLeader(member.getThisNode().getEndpoint());
      request.setLeaderId(member.getThisNode().getNodeId());
      request.setLeaderCommit(member.getLogManager().getCommitLogIndex());
      request.setTerm(member.getStatus().getTerm().get());
      request.setEntries(logList);
      return request;
    }

    protected AppendCompressedEntriesRequest prepareCompressedRequest(List<ByteBuffer> logList) {
      AppendCompressedEntriesRequest request = new AppendCompressedEntriesRequest();

      request.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
      request.setLeader(member.getThisNode().getEndpoint());
      request.setLeaderId(member.getThisNode().getNodeId());
      request.setLeaderCommit(member.getLogManager().getCommitLogIndex());
      request.setTerm(member.getStatus().getTerm().get());
      request.setEntryBytes(LogUtils.compressEntries(logList, compressor));
      request.setCompressionType((byte) compressor.getType().ordinal());
      return request;
    }

    private void sendLogs(List<VotingEntry> currBatch) {
      if (currBatch.isEmpty()) {
        return;
      }

      int logIndex = 0;
      logger.debug(
          "send logs from index {} to {}",
          currBatch.get(0).getEntry().getCurrLogIndex(),
          currBatch.get(currBatch.size() - 1).getEntry().getCurrLogIndex());
      while (logIndex < currBatch.size()) {
        long logSize = 0;
        long logSizeLimit = config.getThriftMaxFrameSize();
        List<ByteBuffer> logList = new ArrayList<>();
        int prevIndex = logIndex;

        for (; logIndex < currBatch.size(); logIndex++) {
          VotingEntry entry = currBatch.get(logIndex);
          long curSize = entry.getAppendEntryRequest().entry.remaining();
          if (logSizeLimit - curSize - logSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
            break;
          }
          logSize += curSize;
          logList.add(entry.getAppendEntryRequest().entry.slice());
          Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENDING.calOperationCostTimeFromStart(
              entry.getEntry().createTime);
        }

        if (!enableCompressedDispatching) {
          AppendEntriesRequest appendEntriesRequest = prepareRequest(logList);
          appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        } else {
          AppendCompressedEntriesRequest appendEntriesRequest = prepareCompressedRequest(logList);
          appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
        }

        if (config.isUseFollowerLoadBalance()) {
          FlowMonitorManager.INSTANCE.report(receiver.getEndpoint(), logSize);
        }
        nodesRateLimiter.get(receiver).acquire((int) logSize);
      }
    }

    public AppendNodeEntryHandler getAppendNodeEntryHandler(VotingEntry log, Peer node) {
      AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
      handler.setDirectReceiver(node);
      handler.setVotingEntry(log);
      handler.setMember(member);
      return handler;
    }

    class AppendEntriesHandler implements AsyncMethodCallback<AppendEntryResult> {

      private final List<AsyncMethodCallback<AppendEntryResult>> singleEntryHandlers;

      private AppendEntriesHandler(List<VotingEntry> batch) {
        singleEntryHandlers = new ArrayList<>(batch.size());
        for (VotingEntry sendLogRequest : batch) {
          AppendNodeEntryHandler handler = getAppendNodeEntryHandler(sendLogRequest, receiver);
          singleEntryHandlers.add(handler);
        }
      }

      @Override
      public void onComplete(AppendEntryResult aLong) {
        for (AsyncMethodCallback<AppendEntryResult> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onComplete(aLong);
        }
      }

      @Override
      public void onError(Exception e) {
        for (AsyncMethodCallback<AppendEntryResult> singleEntryHandler : singleEntryHandlers) {
          singleEntryHandler.onError(e);
        }
      }
    }
  }

  public Map<Peer, Double> getNodesRate() {
    return nodesRate;
  }

  public Map<Peer, BlockingQueue<VotingEntry>> getNodesLogQueuesMap() {
    return nodesLogQueuesMap;
  }

  public void setNewNodes(List<Peer> newNodes) {
    this.newNodes = newNodes;
    for (Peer newNode : newNodes) {
      if (!allNodes.contains(newNode)) {
        createQueue(newNode);
      }
    }
  }
}
