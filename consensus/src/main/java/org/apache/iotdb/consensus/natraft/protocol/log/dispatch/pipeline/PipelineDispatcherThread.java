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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch.pipeline;

import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.AppendNodeEntryHandler;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

class PipelineDispatcherThread extends DynamicThread {

  private static final Logger logger = LoggerFactory.getLogger(PipelineDispatcherThread.class);

  protected final PipelinedLogDispatcher logDispatcher;
  protected Peer receiver;
  protected final PipelineDispatcherGroup group;
  private final BlockingQueue<DispatchTask> taskQueue;

  protected PipelineDispatcherThread(
      PipelinedLogDispatcher logDispatcher,
      Peer receiver,
      BlockingQueue<DispatchTask> taskQueue,
      PipelineDispatcherGroup group) {
    super(group.getDynamicThreadGroup());
    this.logDispatcher = logDispatcher;
    this.receiver = receiver;
    this.group = group;
    this.taskQueue = taskQueue;
  }

  @Override
  public void runInternal() {
    try {
      while (!Thread.interrupted() && !group.getDynamicThreadGroup().isStopped()) {
        DispatchTask task = taskQueue.take();

        idleToRunning();
        logger.debug("Sending {} to {}", task, receiver);

        appendEntriesAsync(task.request, task.votingEntryList);
        if (logDispatcher.getConfig().isUseFollowerLoadBalance()) {
          FlowMonitorManager.INSTANCE.report(receiver.getEndpoint(), task.logSize);
        }
        group.getRateLimiter().acquire(task.logSize);
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

  private void appendEntriesAsync(
      AppendCompressedEntriesRequest request, List<VotingEntry> currBatch) {
    AsyncMethodCallback<AppendEntryResult> handler = new AppendEntriesHandler(currBatch);
    AsyncRaftServiceClient client = logDispatcher.getMember().getClient(receiver.getEndpoint());
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
  }

  public AppendNodeEntryHandler getAppendNodeEntryHandler(VotingEntry log, Peer node) {
    AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
    handler.setDirectReceiver(node);
    handler.setVotingEntry(log);
    handler.setMember(logDispatcher.getMember());
    return handler;
  }

  public class AppendEntriesHandler implements AsyncMethodCallback<AppendEntryResult> {

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
