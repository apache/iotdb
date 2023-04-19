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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.apache.ratis.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class DispatcherThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(DispatcherThread.class);

  private final LogDispatcher logDispatcher;
  Peer receiver;
  private final BlockingQueue<VotingEntry> logBlockingDeque;
  protected List<VotingEntry> currBatch = new ArrayList<>();
  private final String baseName;
  private final RateLimiter rateLimiter;
  private final DispatcherGroup group;
  private long idleTimeSum;
  private long runningTimeSum;
  private long lastDispatchTime;

  protected DispatcherThread(
      LogDispatcher logDispatcher,
      Peer receiver,
      BlockingQueue<VotingEntry> logBlockingDeque,
      RateLimiter rateLimiter,
      DispatcherGroup group) {
    this.logDispatcher = logDispatcher;
    this.receiver = receiver;
    this.logBlockingDeque = logBlockingDeque;
    this.rateLimiter = rateLimiter;
    this.group = group;
    this.baseName = "LogDispatcher-" + logDispatcher.member.getName() + "-" + receiver;
  }

  @Override
  public void run() {
    if (logger.isDebugEnabled()) {
      Thread.currentThread().setName(baseName);
    }
    try {
      long idleStart = System.nanoTime();
      long runningStart = 0;
      while (!Thread.interrupted()) {
        if (group.isDelayed()) {
          if (logBlockingDeque.size() < logDispatcher.maxBatchSize
              && System.nanoTime() - lastDispatchTime < 1_000_000_000L) {
            // the follower is being delayed, if there is not enough requests, and it has
            // dispatched recently, wait for a while to get a larger batch
            Thread.sleep(100);
            continue;
          }
        }

        synchronized (logBlockingDeque) {
          VotingEntry poll = logBlockingDeque.poll();
          if (poll != null) {
            currBatch.add(poll);
            logBlockingDeque.drainTo(currBatch, logDispatcher.maxBatchSize - 1);
          } else {
            logBlockingDeque.wait(10);
            continue;
          }
        }
        long currTime = System.nanoTime();
        idleTimeSum += currTime - idleStart;
        runningStart = currTime;
        if (logger.isDebugEnabled()) {
          logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
        }

        serializeEntries();
        if (!logDispatcher.queueOrdered) {
          currBatch.sort(Comparator.comparingLong(s -> s.getEntry().getCurrLogIndex()));
        }
        sendLogs(currBatch);
        currBatch.clear();

        currTime = System.nanoTime();
        lastDispatchTime = currTime;
        runningTimeSum += currTime - runningStart;
        idleStart = currTime;

        // thread too idle
        if (idleTimeSum * 1.0 / (idleTimeSum + runningTimeSum) > 0.5
            && runningTimeSum > 10_000_000_000L) {
          int remaining = group.getGroupThreadNum().decrementAndGet();
          if (remaining > 1) {
            logger.info("Dispatcher thread too idle");
            group.getGroupThreadNum().incrementAndGet();
            break;
          } else {
            group.getGroupThreadNum().incrementAndGet();
          }
          // thread too busy
        } else if (idleTimeSum * 1.0 / (idleTimeSum + runningTimeSum) < 0.1
            && runningTimeSum > 10_000_000_000L) {
          int groupThreadNum = group.getGroupThreadNum().get();
          if (groupThreadNum < group.getMaxBindingThreadNum()) {
            group.addThread();
          }
          // avoid frequent change
          runningTimeSum = 0;
          idleTimeSum = 0;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error("Unexpected error in log dispatcher", e);
    }
    if (runningTimeSum > 0) {
      logger.info(
          "Dispatcher exits, idle ratio: {}, running time: {}ms, idle time: {}ms, remaining threads: {}",
          idleTimeSum * 1.0 / (idleTimeSum + runningTimeSum),
          runningTimeSum / 1_000_000L,
          idleTimeSum / 1_000_000L,
          group.getGroupThreadNum().decrementAndGet());
    }
  }

  protected void serializeEntries() throws InterruptedException {
    for (VotingEntry request : currBatch) {

      request.getAppendEntryRequest().entry = request.getEntry().serialize();
      request.getEntry().setByteSize(request.getAppendEntryRequest().entry.limit());
    }
  }

  private void appendEntriesAsync(
      List<ByteBuffer> logList, AppendEntriesRequest request, List<VotingEntry> currBatch) {
    AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
    AsyncRaftServiceClient client = logDispatcher.member.getClient(receiver.getEndpoint());
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
          "{}: append entries {} with {} logs",
          logDispatcher.member.getName(),
          receiver,
          logList.size());
    }
  }

  private void appendEntriesAsync(
      List<ByteBuffer> logList,
      AppendCompressedEntriesRequest request,
      List<VotingEntry> currBatch) {
    AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
    AsyncRaftServiceClient client = logDispatcher.member.getClient(receiver.getEndpoint());
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
          "{}: append entries {} with {} logs",
          logDispatcher.member.getName(),
          receiver,
          logList.size());
    }
  }

  protected AppendEntriesRequest prepareRequest(List<ByteBuffer> logList) {
    AppendEntriesRequest request = new AppendEntriesRequest();

    request.setGroupId(logDispatcher.member.getRaftGroupId().convertToTConsensusGroupId());
    request.setLeader(logDispatcher.member.getThisNode().getEndpoint());
    request.setLeaderId(logDispatcher.member.getThisNode().getNodeId());
    request.setLeaderCommit(logDispatcher.member.getLogManager().getCommitLogIndex());
    request.setTerm(logDispatcher.member.getStatus().getTerm().get());
    request.setEntries(logList);
    return request;
  }

  protected AppendCompressedEntriesRequest prepareCompressedRequest(List<ByteBuffer> logList) {
    AppendCompressedEntriesRequest request = new AppendCompressedEntriesRequest();

    request.setGroupId(logDispatcher.member.getRaftGroupId().convertToTConsensusGroupId());
    request.setLeader(logDispatcher.member.getThisNode().getEndpoint());
    request.setLeaderId(logDispatcher.member.getThisNode().getNodeId());
    request.setLeaderCommit(logDispatcher.member.getLogManager().getCommitLogIndex());
    request.setTerm(logDispatcher.member.getStatus().getTerm().get());
    request.setEntryBytes(LogUtils.compressEntries(logList, logDispatcher.compressor));
    request.setCompressionType((byte) logDispatcher.compressor.getType().ordinal());
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
      long logSizeLimit = logDispatcher.getConfig().getThriftMaxFrameSize();
      List<ByteBuffer> logList = new ArrayList<>();
      int prevIndex = logIndex;

      for (; logIndex < currBatch.size(); logIndex++) {
        VotingEntry entry = currBatch.get(logIndex);
        long curSize = entry.getAppendEntryRequest().entry.array().length;
        if (logSizeLimit - curSize - logSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
          break;
        }
        logSize += curSize;
        logList.add(entry.getAppendEntryRequest().entry);
        Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENDING.calOperationCostTimeFromStart(
            entry.getEntry().createTime);
      }

      if (!logDispatcher.enableCompressedDispatching) {
        AppendEntriesRequest appendEntriesRequest = prepareRequest(logList);
        appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
      } else {
        AppendCompressedEntriesRequest appendEntriesRequest = prepareCompressedRequest(logList);
        appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
      }

      if (logDispatcher.getConfig().isUseFollowerLoadBalance()) {
        FlowMonitorManager.INSTANCE.report(receiver.getEndpoint(), logSize);
      }
      rateLimiter.acquire((int) logSize);
    }
  }

  public AppendNodeEntryHandler getAppendNodeEntryHandler(VotingEntry log, Peer node) {
    AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
    handler.setDirectReceiver(node);
    handler.setVotingEntry(log);
    handler.setMember(logDispatcher.member);
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
