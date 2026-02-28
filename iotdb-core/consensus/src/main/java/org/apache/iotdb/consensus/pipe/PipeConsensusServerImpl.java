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

package org.apache.iotdb.consensus.pipe;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ComparableConsensusRequest;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.config.PipeConsensusConfig.ReplicateMode;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ReplicateProgressManager;
import org.apache.iotdb.consensus.pipe.metric.PipeConsensusServerMetrics;
import org.apache.iotdb.consensus.pipe.thrift.TCheckConsensusPipeCompletedReq;
import org.apache.iotdb.consensus.pipe.thrift.TCheckConsensusPipeCompletedResp;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToCreateConsensusPipeReq;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToCreateConsensusPipeResp;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToDropConsensusPipeReq;
import org.apache.iotdb.consensus.pipe.thrift.TNotifyPeerToDropConsensusPipeResp;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveReq;
import org.apache.iotdb.consensus.pipe.thrift.TSetActiveResp;
import org.apache.iotdb.consensus.pipe.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.pipe.thrift.TWaitReleaseAllRegionRelatedResourceResp;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.RpcUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** PipeConsensusServerImpl is a consensus server implementation for pipe consensus. */
public class PipeConsensusServerImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusServerImpl.class);
  private static final long CHECK_TRANSMISSION_COMPLETION_INTERVAL_IN_MILLISECONDS = 2_000L;
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final Lock stateMachineLock = new ReentrantLock();
  private final PipeConsensusPeerManager peerManager;
  private final AtomicBoolean active;
  private final AtomicBoolean isStarted;
  private final String consensusGroupId;
  private final ReplicateProgressManager replicateProgressManager;
  private final IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager;
  private final PipeConsensusServerMetrics pipeConsensusServerMetrics;
  private final ReplicateMode replicateMode;

  private ProgressIndex cachedProgressIndex = MinimumProgressIndex.INSTANCE;

  public PipeConsensusServerImpl(
      Peer thisNode,
      IStateMachine stateMachine,
      List<Peer> peers,
      PipeConsensusConfig config,
      IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager)
      throws IOException {
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.peerManager = new PipeConsensusPeerManager(peers);
    this.active = new AtomicBoolean(true);
    this.isStarted = new AtomicBoolean(false);
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.replicateProgressManager = config.getPipe().getProgressIndexManager();
    this.syncClientManager = syncClientManager;
    this.pipeConsensusServerMetrics = new PipeConsensusServerMetrics(this);
    this.replicateMode = config.getReplicateMode();

    // Consensus pipe creation is fully delegated to ConfigNode to avoid deadlocks between
    // DataNode RPC handlers and ConfigNode's PipeTaskCoordinatorLock. ConfigNode proactively
    // creates consensus pipes at key lifecycle points:
    //   1. New DataRegion creation: via CreatePipeProcedureV2 in CreateRegionGroupsProcedure
    //   2. Region migration addPeer: via CREATE_CONSENSUS_PIPES state in AddRegionPeerProcedure
  }

  public synchronized void start() throws IOException {
    stateMachine.start();
    MetricService.getInstance().addMetricSet(this.pipeConsensusServerMetrics);
    isStarted.set(true);
  }

  public synchronized void stop() {
    MetricService.getInstance().removeMetricSet(this.pipeConsensusServerMetrics);
    stateMachine.stop();
    isStarted.set(false);
  }

  public synchronized void clear() {
    MetricService.getInstance().removeMetricSet(this.pipeConsensusServerMetrics);
    peerManager.clear();
    stateMachine.stop();
    isStarted.set(false);
    active.set(false);
  }

  /**
   * Detect inconsistencies between expected and existed consensus pipes. Actual remediation
   * (create/drop/update) is handled by ConfigNode; this method only logs warnings.
   */
  public synchronized void checkConsensusPipe(Map<ConsensusPipeName, PipeStatus> existedPipes) {
    final PipeStatus expectedStatus = isStarted.get() ? PipeStatus.RUNNING : PipeStatus.STOPPED;
    final Map<ConsensusPipeName, Peer> expectedPipes =
        peerManager.getOtherPeers(thisNode).stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    peer -> new ConsensusPipeName(thisNode, peer), peer -> peer));

    existedPipes.forEach(
        (existedName, existedStatus) -> {
          if (!expectedPipes.containsKey(existedName)) {
            LOGGER.warn(
                "{} unexpected consensus pipe [{}] exists, should be dropped by ConfigNode",
                consensusGroupId,
                existedName);
          } else if (!expectedStatus.equals(existedStatus)) {
            LOGGER.warn(
                "{} consensus pipe [{}] status mismatch: expected={}, actual={}",
                consensusGroupId,
                existedName,
                expectedStatus,
                existedStatus);
          }
        });

    expectedPipes.forEach(
        (expectedName, expectedPeer) -> {
          if (!existedPipes.containsKey(expectedName)) {
            LOGGER.warn(
                "{} consensus pipe [{}] missing, should be created by ConfigNode",
                consensusGroupId,
                expectedName);
          }
        });
  }

  public TSStatus write(IConsensusRequest request) {
    stateMachineLock.lock();
    try {
      long consensusWriteStartTime = System.nanoTime();
      long getStateMachineLockTime = System.nanoTime();
      // statistic the time of acquiring stateMachine lock
      pipeConsensusServerMetrics.recordGetStateMachineLockTime(
          getStateMachineLockTime - consensusWriteStartTime);
      long writeToStateMachineStartTime = System.nanoTime();
      if (request instanceof ComparableConsensusRequest) {
        ((ComparableConsensusRequest) request)
            .setProgressIndex(replicateProgressManager.assignProgressIndex(thisNode.getGroupId()));
      }
      TSStatus result = stateMachine.write(request);
      long writeToStateMachineEndTime = System.nanoTime();
      PERFORMANCE_OVERVIEW_METRICS.recordEngineCost(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
      // statistic the time of writing request into stateMachine
      pipeConsensusServerMetrics.recordUserWriteStateMachineTime(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
      return result;
    } finally {
      stateMachineLock.unlock();
    }
  }

  public TSStatus writeOnFollowerReplica(IConsensusRequest request) {
    stateMachineLock.lock();
    try {
      long consensusWriteStartTime = System.nanoTime();
      long getStateMachineLockTime = System.nanoTime();
      // statistic the time of acquiring stateMachine lock
      pipeConsensusServerMetrics.recordGetStateMachineLockTime(
          getStateMachineLockTime - consensusWriteStartTime);

      long writeToStateMachineStartTime = System.nanoTime();
      TSStatus result = stateMachine.write(request);
      long writeToStateMachineEndTime = System.nanoTime();

      PERFORMANCE_OVERVIEW_METRICS.recordEngineCost(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
      // statistic the time of writing request into stateMachine
      pipeConsensusServerMetrics.recordReplicaWriteStateMachineTime(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
      return result;
    } finally {
      stateMachineLock.unlock();
    }
  }

  public DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  public void setRemotePeerActive(Peer peer, boolean isActive, boolean isForDeletionPurpose)
      throws ConsensusGroupModifyPeerException {
    try (SyncPipeConsensusServiceClient client =
        syncClientManager.borrowClient(peer.getEndpoint())) {
      try {
        TSetActiveResp res =
            client.setActive(
                new TSetActiveReq(
                    peer.getGroupId().convertToTConsensusGroupId(),
                    isActive,
                    isForDeletionPurpose));
        if (!RpcUtils.SUCCESS_STATUS.equals(res.getStatus())) {
          throw new ConsensusGroupModifyPeerException(
              String.format(
                  "error when set peer %s to active %s. result status: %s",
                  peer, isActive, res.getStatus()));
        }
      } catch (Exception e) {
        throw new ConsensusGroupModifyPeerException(
            String.format("error when set peer %s to active %s", peer, isActive), e);
      }
    } catch (ClientManagerException e) {
      if (isForDeletionPurpose) {
        // for remove peer, if target peer is already down, we can skip this step.
        LOGGER.warn(
            "target peer may be down, error when set peer {} to active {}", peer, isActive, e);
      } else {
        // for add peer, if target peer is down, we need to throw exception to identify the failure
        // of this addPeerProcedure.
        throw new ConsensusGroupModifyPeerException(e);
      }
    }
  }

  public void notifyPeersToCreateConsensusPipes(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try (SyncPipeConsensusServiceClient client =
          syncClientManager.borrowClient(peer.getEndpoint())) {
        TNotifyPeerToCreateConsensusPipeResp resp =
            client.notifyPeerToCreateConsensusPipe(
                new TNotifyPeerToCreateConsensusPipeReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId(),
                    targetPeer.getEndpoint(),
                    targetPeer.getNodeId()));
        if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when notify peer %s to create consensus pipe", peer));
        }
      } catch (Exception e) {
        LOGGER.warn(
            "{} cannot notify peer {} to create consensus pipe, may because that peer is unknown currently, please manually check!",
            thisNode,
            peer,
            e);
      }
    }

    try {
      // This node which acts as coordinator will transfer complete historical snapshot to new
      // target.
      createConsensusPipeToTargetPeer(targetPeer);
    } catch (Exception e) {
      LOGGER.warn(
          "{} cannot create consensus pipe to {}, may because target peer is unknown currently, please manually check!",
          thisNode,
          targetPeer,
          e);
      throw new ConsensusGroupModifyPeerException(e);
    }
  }

  public synchronized void createConsensusPipeToTargetPeer(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    KillPoint.setKillPoint(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE);
    // Pipe creation is delegated to ConfigNode; only update local peer list.
    peerManager.addPeer(targetPeer);
  }

  public void notifyPeersToDropConsensusPipe(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try (SyncPipeConsensusServiceClient client =
          syncClientManager.borrowClient(peer.getEndpoint())) {
        TNotifyPeerToDropConsensusPipeResp resp =
            client.notifyPeerToDropConsensusPipe(
                new TNotifyPeerToDropConsensusPipeReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId(),
                    targetPeer.getEndpoint(),
                    targetPeer.getNodeId()));
        if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when notify peer %s to drop consensus pipe", peer));
        }
      } catch (Exception e) {
        LOGGER.warn(
            "{} cannot notify peer {} to drop consensus pipe, may because that peer is unknown currently, please manually check!",
            thisNode,
            peer,
            e);
      }
    }

    try {
      dropConsensusPipeToTargetPeer(targetPeer);
    } catch (Exception e) {
      LOGGER.warn(
          "{} cannot drop consensus pipe to {}, may because target peer is unknown currently, please manually check!",
          thisNode,
          targetPeer,
          e);
      throw new ConsensusGroupModifyPeerException(e);
    }
  }

  public synchronized void dropConsensusPipeToTargetPeer(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    // Pipe drop is delegated to ConfigNode; only update local peer list.
    peerManager.removePeer(targetPeer);
  }

  /** Wait for the user written data up to firstCheck to be replicated */
  public void waitPeersToTargetPeerTransmissionCompleted(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    boolean isTransmissionCompleted = false;
    boolean isFirstCheckForCurrentPeer = true;
    boolean isFirstCheckForOtherPeers = true;

    try {
      while (!isTransmissionCompleted) {
        Thread.sleep(CHECK_TRANSMISSION_COMPLETION_INTERVAL_IN_MILLISECONDS);

        if (isConsensusPipesTransmissionCompleted(
            Collections.singletonList(new ConsensusPipeName(thisNode, targetPeer).toString()),
            isFirstCheckForCurrentPeer)) {
          final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);

          isTransmissionCompleted = true;
          for (Peer peer : otherPeers) {
            if (!peer.equals(targetPeer)) {
              isTransmissionCompleted &=
                  isRemotePeerConsensusPipesTransmissionCompleted(
                      peer,
                      Collections.singletonList(new ConsensusPipeName(peer, targetPeer).toString()),
                      isFirstCheckForOtherPeers);
            }
          }
          isFirstCheckForOtherPeers = false;
        }
        isFirstCheckForCurrentPeer = false;
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} is interrupted when waiting for transfer completed", thisNode, e);
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format("%s is interrupted when waiting for transfer completed", thisNode), e);
    }
  }

  /** Wait for the user written data up to firstCheck to be replicated */
  public void waitTargetPeerToPeersTransmissionCompleted(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    boolean isTransmissionCompleted = false;
    boolean isFirstCheck = true;

    try {
      while (!isTransmissionCompleted) {
        Thread.sleep(CHECK_TRANSMISSION_COMPLETION_INTERVAL_IN_MILLISECONDS);

        final List<String> consensusPipeNames =
            peerManager.getPeers().stream()
                .filter(peer -> !peer.equals(targetPeer))
                .map(peer -> new ConsensusPipeName(targetPeer, peer).toString())
                .collect(Collectors.toList());
        isTransmissionCompleted =
            isRemotePeerConsensusPipesTransmissionCompleted(
                targetPeer, consensusPipeNames, isFirstCheck);

        isFirstCheck = false;
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} is interrupted when waiting for transfer completed", thisNode, e);
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format("%s is interrupted when waiting for transfer completed", thisNode), e);
    }
  }

  private boolean isRemotePeerConsensusPipesTransmissionCompleted(
      Peer targetPeer, List<String> consensusPipeNames, boolean refreshCachedProgressIndex) {
    try (SyncPipeConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      TCheckConsensusPipeCompletedResp resp =
          client.checkConsensusPipeCompleted(
              new TCheckConsensusPipeCompletedReq(
                  thisNode.getGroupId().convertToTConsensusGroupId(),
                  consensusPipeNames,
                  refreshCachedProgressIndex));
      if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
        LOGGER.warn(
            "{} cannot check consensus pipes transmission completed to peer {}",
            thisNode,
            targetPeer);
        throw new ConsensusGroupModifyPeerException(
            String.format(
                "error when check consensus pipes transmission completed to peer %s", targetPeer));
      }
      return resp.isCompleted;
    } catch (Exception e) {
      LOGGER.warn("{} cannot check consensus pipes transmission completed", thisNode, e);
      return true;
    }
  }

  public boolean isConsensusPipesTransmissionCompleted(List<String> consensusPipeNames) {
    return consensusPipeNames.stream()
        .noneMatch(
            pipeName ->
                replicateProgressManager.getSyncLagForSpecificConsensusPipe(
                        thisNode.getGroupId(), new ConsensusPipeName(pipeName))
                    > 0);
  }

  public synchronized boolean isConsensusPipesTransmissionCompleted(
      List<String> consensusPipeNames, boolean refreshCachedProgressIndex) {
    if (refreshCachedProgressIndex) {
      cachedProgressIndex =
          cachedProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
              replicateProgressManager.getMaxAssignedProgressIndex(thisNode.getGroupId()));
    }

    try {
      return consensusPipeNames.stream()
          .noneMatch(
              name ->
                  cachedProgressIndex.isAfter(
                      replicateProgressManager.getProgressIndex(new ConsensusPipeName(name))));
    } catch (PipeException e) {
      LOGGER.info(e.getMessage());
      return false;
    }
  }

  public void waitReleaseAllRegionRelatedResource(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    long checkIntervalInMs = 10_000L;
    try (SyncPipeConsensusServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      while (true) {
        TWaitReleaseAllRegionRelatedResourceResp res =
            client.waitReleaseAllRegionRelatedResource(
                new TWaitReleaseAllRegionRelatedResourceReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId()));
        if (res.releaseAllResource) {
          LOGGER.info("[WAIT RELEASE] {} has released all region related resource", targetPeer);
          return;
        }
        LOGGER.info("[WAIT RELEASE] {} is still releasing all region related resource", targetPeer);
        Thread.sleep(checkIntervalInMs);
      }
    } catch (ClientManagerException | TException e) {
      // in case of target peer is down or can not serve, we simply skip it.
      LOGGER.warn(
          String.format(
              "error when waiting %s to release all region related resource. %s",
              targetPeer, e.getMessage()),
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format(
              "thread interrupted when waiting %s to release all region related resource. %s",
              targetPeer, e.getMessage()),
          e);
    }
  }

  public boolean hasReleaseAllRegionRelatedResource(ConsensusGroupId groupId) {
    return stateMachine.hasReleaseAllRegionRelatedResource(groupId);
  }

  public boolean isReadOnly() {
    return stateMachine.isReadOnly();
  }

  public boolean isActive() {
    return active.get();
  }

  public void setActive(boolean active) {
    LOGGER.info("set {} active status to {}", this.thisNode, active);
    this.active.set(active);
  }

  public boolean containsPeer(Peer peer) {
    return peerManager.contains(peer);
  }

  public List<Peer> getPeers() {
    return peerManager.getPeers();
  }

  public String getConsensusGroupId() {
    return consensusGroupId;
  }

  public long getReplicateMode() {
    return (replicateMode == ReplicateMode.BATCH) ? 2 : 1;
  }

  public Peer getThisNodePeer() {
    return thisNode;
  }
}
