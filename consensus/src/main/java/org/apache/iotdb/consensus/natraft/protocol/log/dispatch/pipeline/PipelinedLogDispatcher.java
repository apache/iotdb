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

import static org.apache.iotdb.consensus.natraft.utils.NodeUtils.unionNodes;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThread;
import org.apache.iotdb.commons.concurrent.dynamic.DynamicThreadGroup;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.ILogDispatcher;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LogDispatcher serves a raft leader by queuing logs that the leader wants to send to its
 * followers and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class PipelinedLogDispatcher implements ILogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(PipelinedLogDispatcher.class);
  protected RaftMember member;
  protected RaftConfig config;
  protected List<Peer> allNodes;
  protected List<Peer> newNodes;
  protected Map<Peer, PipelineDispatcherGroup> dispatcherGroupMap = new HashMap<>();
  protected Map<Peer, Double> nodesRate = new HashMap<>();
  protected boolean queueOrdered;
  protected boolean enableCompressedDispatching;
  public int maxBindingThreadNum;
  public int minBindingThreadNum;
  public int maxBatchSize;
  private DynamicThreadGroup compressionThreadGroup;
  private final Queue<VotingEntry> entriesToCompress;
  private ExecutorService compressionPool;
  private ICompressor compressor;

  public PipelinedLogDispatcher(RaftMember member, RaftConfig config) {
    this.member = member;
    this.config = config;
    this.queueOrdered = !(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance());
    this.enableCompressedDispatching = config.isEnableCompressedDispatching();
    this.minBindingThreadNum = config.getMinDispatcherBindingThreadNum();
    this.maxBindingThreadNum = config.getMaxDispatcherBindingThreadNum();
    this.allNodes = member.getAllNodes();
    this.newNodes = member.getNewNodes();
    this.entriesToCompress = new ArrayDeque<>(config.getMaxNumOfLogsInMem());

    this.compressionPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            config.getMinDispatcherBindingThreadNum(), member.getName() + "-DispatcherCompressor");
    this.compressor = ICompressor.getCompressor(config.getDispatchingCompressionType());
    this.compressionThreadGroup = new DynamicThreadGroup(member.getName() + "-DispatcherCompressor",
        r -> compressionPool.submit(r), () -> new PipelineCompressor(compressionThreadGroup),
        config.getMinDispatcherBindingThreadNum(), config.getMaxDispatcherBindingThreadNum());
    this.compressionThreadGroup.init();
    createDispatcherGroups(unionNodes(allNodes, newNodes));
    maxBatchSize = config.getLogNumInBatch();
  }

  public void updateRateLimiter() {
    logger.info("{}: TEndPoint rates: {}", member.getName(), nodesRate);
    for (Entry<Peer, Double> nodeDoubleEntry : nodesRate.entrySet()) {
      Peer peer = nodeDoubleEntry.getKey();
      Double rate = nodeDoubleEntry.getValue();
      dispatcherGroupMap.get(peer).updateRate(rate);
    }
  }

  void createDispatcherGroup(Peer node) {
    dispatcherGroupMap.computeIfAbsent(
        node, n -> new PipelineDispatcherGroup(n, this, maxBindingThreadNum, minBindingThreadNum));
  }

  void createDispatcherGroups(Collection<Peer> peers) {
    for (Peer node : peers) {
      if (!node.equals(member.getThisNode())) {
        createDispatcherGroup(node);
      }
    }
    updateRateLimiter();
  }

  public void offer(VotingEntry request) {
    synchronized (entriesToCompress) {
      entriesToCompress.offer(request);
      entriesToCompress.notifyAll();
    }
  }

  protected void offer(DispatchTask dispatchTask) {
    for (Entry<Peer, PipelineDispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      PipelineDispatcherGroup group = entry.getValue();
      group.add(dispatchTask);
    }
  }

  public void applyNewNodes() {
    allNodes = newNodes;
    newNodes = null;

    List<Peer> nodesToRemove = new ArrayList<>();
    for (Entry<Peer, PipelineDispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      if (!allNodes.contains(entry.getKey())) {
        nodesToRemove.add(entry.getKey());
      }
    }
    for (Peer peer : nodesToRemove) {
      PipelineDispatcherGroup removed = dispatcherGroupMap.remove(peer);
      removed.close();
    }
  }

  public Map<Peer, Double> getNodesRate() {
    return nodesRate;
  }

  public void setNewNodes(List<Peer> newNodes) {
    this.newNodes = newNodes;
    for (Peer newNode : newNodes) {
      if (!allNodes.contains(newNode)) {
        createDispatcherGroup(newNode);
      }
    }
  }

  public RaftConfig getConfig() {
    return config;
  }

  public RaftMember getMember() {
    return member;
  }

  public void stop() {
    dispatcherGroupMap.forEach((p, g) -> g.close());
    compressionThreadGroup.cancelAll();
  }

  public void wakeUp() {
    for (PipelineDispatcherGroup group : dispatcherGroupMap.values()) {
      group.wakeUp();
    }
  }

  public void sortPeers(List<Peer> peers) {
    peers.sort(Comparator.comparing(dispatcherGroupMap::get));
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    for (Entry<Peer, PipelineDispatcherGroup> entry : dispatcherGroupMap.entrySet()) {
      stringBuilder.append(entry.getKey()).append("->").append(entry.getValue()).append(";");
    }
    return "LogDispatcher{" + stringBuilder + "}";
  }

  public class PipelineCompressor extends DynamicThread {

    protected List<VotingEntry> currBatch = new ArrayList<>();
    protected PublicBAOS batchLogBuffer = new PublicBAOS(64 * 1024);
    protected AtomicReference<byte[]> compressionBuffer =
        new AtomicReference<>(new byte[64 * 1024]);

    public PipelineCompressor(DynamicThreadGroup threadGroup) {
      super(threadGroup);
    }

    @Override
    public void runInternal() {
      try {
        while (!Thread.interrupted()) {
          currBatch.clear();
          if (fetchLogsSyncLoop()) {
            idleToRunning();
            compressLogs();
            runningToIdle();
          }
          if (shouldExit()) {
            return;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private void compressLogs() {
      if (currBatch.isEmpty()) {
        return;
      }

      DispatchTask dispatchTask = new DispatchTask();
      int logIndex = 0;
      logger.debug(
          "send logs from index {} to {}",
          currBatch.get(0).getEntry().getCurrLogIndex(),
          currBatch.get(currBatch.size() - 1).getEntry().getCurrLogIndex());
      while (logIndex < currBatch.size()) {
        long logSize = 0;
        long logSizeLimit = getConfig().getThriftMaxFrameSize();
        List<ByteBuffer> logList = new ArrayList<>();

        for (; logIndex < currBatch.size(); logIndex++) {
          VotingEntry entry = currBatch.get(logIndex);
          ByteBuffer serialized = entry.getEntry().serialize();

          long curSize = serialized.remaining();
          if (logSizeLimit - curSize - logSize <= IoTDBConstant.LEFT_SIZE_IN_REQUEST) {
            break;
          }
          logSize += curSize;
          logList.add(serialized);
          dispatchTask.votingEntryList.add(entry);

          Statistic.LOG_DISPATCHER_FROM_CREATE_TO_SENDING.calOperationCostTimeFromStart(
              entry.getEntry().createTime);
        }

        dispatchTask.request = prepareCompressedRequest(logList);
        dispatchTask.logSize = (int) logSize;

        offer(dispatchTask);
        dispatchTask = new DispatchTask();
      }
    }

    protected AppendCompressedEntriesRequest prepareCompressedRequest(List<ByteBuffer> logList) {
      AppendCompressedEntriesRequest request = new AppendCompressedEntriesRequest();

      request.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
      request.setLeader(member.getThisNode().getEndpoint());
      request.setLeaderId(member.getThisNode().getNodeId());
      request.setLeaderCommit(member.getLogManager().getCommitLogIndex());
      request.setTerm(member.getStatus().getTerm().get());
      long startTime = Statistic.RAFT_SENDER_COMPRESS_LOG.getOperationStartTime();
      request.setEntryBytes(
          LogUtils.compressEntries(
              logList, compressor, request, batchLogBuffer, compressionBuffer));
      Statistic.RAFT_SENDER_COMPRESS_LOG.calOperationCostTimeFromStart(startTime);
      request.setCompressionType((byte) compressor.getType().ordinal());
      return request;
    }

    private boolean fetchLogsSyncLoop() throws InterruptedException {
      if (!LogUtils.drainTo(entriesToCompress, currBatch, maxBatchSize)) {
        synchronized (entriesToCompress) {
          if (getMember().isLeader()) {
            entriesToCompress.wait(1000);
          } else {
            entriesToCompress.wait(5000);
          }
        }
        return false;
      }
      return true;
    }
  }
}
