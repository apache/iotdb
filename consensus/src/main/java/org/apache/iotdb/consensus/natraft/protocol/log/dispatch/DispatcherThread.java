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

import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
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
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

abstract class DispatcherThread extends DynamicThread {

  private static final Logger logger = LoggerFactory.getLogger(DispatcherThread.class);

  protected final LogDispatcher logDispatcher;
  protected Peer receiver;
  protected List<VotingEntry> currBatch = new ArrayList<>();
  protected final DispatcherGroup group;
  protected long lastDispatchTime;
  protected PublicBAOS batchLogBuffer = new PublicBAOS(64 * 1024);
  protected AtomicReference<byte[]> compressionBuffer = new AtomicReference<>(new byte[64 * 1024]);
  protected ICompressor compressor;

  protected DispatcherThread(LogDispatcher logDispatcher, Peer receiver, DispatcherGroup group) {
    super(group.getDynamicThreadGroup());
    this.logDispatcher = logDispatcher;
    this.receiver = receiver;
    this.group = group;
    this.compressor =
        ICompressor.getCompressor(logDispatcher.getConfig().getDispatchingCompressionType());
  }

  protected abstract boolean fetchLogs() throws InterruptedException;

  @Override
  public void runInternal() {
    try {
      while (!Thread.interrupted() && !group.getDynamicThreadGroup().isStopped()) {
        if (!fetchLogs()) {
          continue;
        }

        idleToRunning();
        if (logger.isDebugEnabled()) {
          logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
        }

        serializeEntries();
        if (!logDispatcher.queueOrdered) {
          currBatch.sort(Comparator.comparingLong(s -> s.getEntry().getCurrLogIndex()));
        }
        sendLogs(currBatch);
        currBatch.clear();
        lastDispatchTime = System.nanoTime();
        runningToIdle();

        if (shouldExit()) {
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error("Unexpected error in log dispatcher", e);
    }
  }

  protected void serializeEntries() throws InterruptedException {
    for (VotingEntry request : currBatch) {
      ByteBuffer serialized = request.getEntry().serialize();
      request.getEntry().setByteSize(serialized.remaining());
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
      if (appendEntryResult != null) {
        handler.onComplete(appendEntryResult);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
      if (appendEntryResult != null) {
        handler.onComplete(appendEntryResult);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
    long startTime = Statistic.RAFT_SENDER_COMPRESS_LOG.getOperationStartTime();
    request.setEntryBytes(
        LogUtils.compressEntries(logList, compressor, request, batchLogBuffer, compressionBuffer));
    Statistic.RAFT_SENDER_COMPRESS_LOG.calOperationCostTimeFromStart(startTime);
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
      long logSizeLimit = logDispatcher.getConfig().getThriftMaxFrameSize();
      List<ByteBuffer> logList = new ArrayList<>();
      int prevIndex = logIndex;

      for (; logIndex < currBatch.size(); logIndex++) {
        VotingEntry entry = currBatch.get(logIndex);
        ByteBuffer serialized = entry.getEntry().serialize();
        long curSize = serialized.remaining();
        if (logSizeLimit - curSize - logSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
          break;
        }
        logSize += curSize;
        logList.add(serialized);
        Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENDING.calOperationCostTimeFromStart(
            entry.getEntry().createTime);
      }

      if (!logDispatcher.enableCompressedDispatching && !group.isDelayed()) {
        AppendEntriesRequest appendEntriesRequest = prepareRequest(logList);
        appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
      } else {
        AppendCompressedEntriesRequest appendEntriesRequest = prepareCompressedRequest(logList);
        appendEntriesAsync(logList, appendEntriesRequest, currBatch.subList(prevIndex, logIndex));
      }

      if (logDispatcher.getConfig().isUseFollowerLoadBalance()) {
        FlowMonitorManager.INSTANCE.report(receiver.getEndpoint(), logSize);
      }
      group.getRateLimiter().acquire((int) logSize);
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
