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
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeManager;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ProgressIndexManager;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final long RETRY_WAIT_TIME_IN_MS = 500;
  private static final long MAX_RETRY_TIMES = 20;
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final Lock stateMachineLock = new ReentrantLock();
  private final PipeConsensusPeerManager peerManager;
  private final AtomicBoolean active;
  private final AtomicBoolean isStarted;
  private final String consensusGroupId;
  private final ConsensusPipeManager consensusPipeManager;
  private final ProgressIndexManager progressIndexManager;
  private final IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager;
  private final PipeConsensusServerMetrics pipeConsensusServerMetrics;
  private final ReplicateMode replicateMode;

  private ProgressIndex cachedProgressIndex = MinimumProgressIndex.INSTANCE;

  public PipeConsensusServerImpl(
      Peer thisNode,
      IStateMachine stateMachine,
      String storageDir,
      List<Peer> peers,
      PipeConsensusConfig config,
      ConsensusPipeManager consensusPipeManager,
      IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager)
      throws IOException {
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.peerManager = new PipeConsensusPeerManager(storageDir, peers);
    this.active = new AtomicBoolean(true);
    this.isStarted = new AtomicBoolean(false);
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.consensusPipeManager = consensusPipeManager;
    this.progressIndexManager = config.getPipe().getProgressIndexManager();
    this.syncClientManager = syncClientManager;
    this.pipeConsensusServerMetrics = new PipeConsensusServerMetrics(this);
    this.replicateMode = config.getReplicateMode();

    if (peers.isEmpty()) {
      peerManager.recover();
    } else {
      // create consensus pipes
      Set<Peer> deepCopyPeersWithoutSelf =
          peers.stream().filter(peer -> !peer.equals(thisNode)).collect(Collectors.toSet());
      final List<Peer> successfulPipes = createConsensusPipes(deepCopyPeersWithoutSelf);
      if (successfulPipes.size() < deepCopyPeersWithoutSelf.size()) {
        // roll back
        updateConsensusPipesStatus(successfulPipes, PipeStatus.DROPPED);
        throw new IOException(String.format("%s cannot create all consensus pipes", thisNode));
      }

      // persist peers' info
      try {
        peerManager.persistAll();
      } catch (Exception e) {
        // roll back
        LOGGER.warn("{} cannot persist all peers", thisNode, e);
        peerManager.deleteAllFiles();
        updateConsensusPipesStatus(successfulPipes, PipeStatus.DROPPED);
        throw e;
      }
    }
  }

  @SuppressWarnings("java:S2276")
  public synchronized void start(boolean startConsensusPipes) throws IOException {
    stateMachine.start();
    MetricService.getInstance().addMetricSet(this.pipeConsensusServerMetrics);

    if (startConsensusPipes) {
      // start all consensus pipes
      final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
      List<Peer> failedPipes =
          updateConsensusPipesStatus(new ArrayList<>(otherPeers), PipeStatus.RUNNING);
      // considering procedure can easily time out, keep trying updateConsensusPipesStatus until all
      // consensus pipes are started gracefully or exceed the maximum number of attempts.
      // NOTE: start pipe procedure is idempotent guaranteed.
      try {
        for (int i = 0; i < MAX_RETRY_TIMES && !failedPipes.isEmpty(); i++) {
          failedPipes = updateConsensusPipesStatus(failedPipes, PipeStatus.RUNNING);
          Thread.sleep(RETRY_WAIT_TIME_IN_MS);
        }
      } catch (InterruptedException e) {
        LOGGER.warn(
            "PipeConsensusImpl-peer{}: pipeConsensusImpl thread get interrupted when start consensus pipe. May because IoTDB process is killed.",
            thisNode);
        throw new IOException(String.format("%s cannot start all consensus pipes", thisNode));
      }
      // if there still are some consensus pipes failed to start, throw an exception.
      if (!failedPipes.isEmpty()) {
        // roll back
        List<Peer> successfulPipes = new ArrayList<>(otherPeers);
        successfulPipes.removeAll(failedPipes);
        updateConsensusPipesStatus(successfulPipes, PipeStatus.STOPPED);
        throw new IOException(String.format("%s cannot start all consensus pipes", thisNode));
      }
    }
    isStarted.set(true);
  }

  public synchronized void stop() {
    // stop all consensus pipes
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    final List<Peer> failedPipes =
        updateConsensusPipesStatus(new ArrayList<>(otherPeers), PipeStatus.STOPPED);
    if (!failedPipes.isEmpty()) {
      // do not roll back, because it will stop anyway
      LOGGER.warn("{} cannot stop all consensus pipes", thisNode);
    }
    MetricService.getInstance().removeMetricSet(this.pipeConsensusServerMetrics);
    stateMachine.stop();
    isStarted.set(false);
  }

  public synchronized void clear() throws IOException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    final List<Peer> failedPipes =
        updateConsensusPipesStatus(new ArrayList<>(otherPeers), PipeStatus.DROPPED);
    if (!failedPipes.isEmpty()) {
      // do not roll back, because it will clear anyway
      LOGGER.warn("{} cannot drop all consensus pipes", thisNode);
    }

    MetricService.getInstance().removeMetricSet(this.pipeConsensusServerMetrics);
    peerManager.clear();
    stateMachine.stop();
    isStarted.set(false);
    active.set(false);
  }

  private List<Peer> createConsensusPipes(Set<Peer> peers) {
    return peers.stream()
        .filter(
            peer -> {
              try {
                if (!peers.equals(thisNode)) {
                  consensusPipeManager.createConsensusPipe(thisNode, peer);
                }
                return true;
              } catch (Exception e) {
                LOGGER.warn(
                    "{}: cannot create consensus pipe between {} and {}",
                    e.getMessage(),
                    thisNode,
                    peer,
                    e);
                return false;
              }
            })
        .collect(Collectors.toList());
  }

  /**
   * update given consensus pipes' status, returns the peer corresponding to the pipe that failed to
   * update
   */
  private List<Peer> updateConsensusPipesStatus(List<Peer> peers, PipeStatus status) {
    return peers.stream()
        .filter(
            peer -> {
              try {
                if (!peer.equals(thisNode)) {
                  consensusPipeManager.updateConsensusPipe(
                      new ConsensusPipeName(thisNode, peer), status);
                }
                return false;
              } catch (Exception e) {
                LOGGER.warn(
                    "{}: cannot update consensus pipe between {} and {} to status {}",
                    e.getMessage(),
                    thisNode,
                    peer,
                    status);
                return true;
              }
            })
        .collect(Collectors.toList());
  }

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
            try {
              LOGGER.warn("{} drop consensus pipe [{}]", consensusGroupId, existedName);
              consensusPipeManager.updateConsensusPipe(existedName, PipeStatus.DROPPED);
            } catch (Exception e) {
              LOGGER.warn("{} cannot drop consensus pipe [{}]", consensusGroupId, existedName, e);
            }
          } else if (!expectedStatus.equals(existedStatus)) {
            try {
              LOGGER.warn(
                  "{} update consensus pipe [{}] to status {}",
                  consensusGroupId,
                  existedName,
                  expectedStatus);
              if (expectedStatus.equals(PipeStatus.RUNNING)) {
                // Do nothing. Because Pipe framework's metaSync will do that.
                return;
              }
              consensusPipeManager.updateConsensusPipe(existedName, expectedStatus);
            } catch (Exception e) {
              LOGGER.warn(
                  "{} cannot update consensus pipe [{}] to status {}",
                  consensusGroupId,
                  existedName,
                  expectedStatus,
                  e);
            }
          }
        });

    expectedPipes.forEach(
        (expectedName, expectedPeer) -> {
          if (!existedPipes.containsKey(expectedName)) {
            try {
              LOGGER.warn(
                  "{} create and update consensus pipe [{}] to status {}",
                  consensusGroupId,
                  expectedName,
                  expectedStatus);
              consensusPipeManager.createConsensusPipe(thisNode, expectedPeer);
              consensusPipeManager.updateConsensusPipe(expectedName, expectedStatus);
            } catch (Exception e) {
              LOGGER.warn(
                  "{} cannot create and update consensus pipe [{}] to status {}",
                  consensusGroupId,
                  expectedName,
                  expectedStatus,
                  e);
            }
          }
        });
  }

  public TSStatus write(IConsensusRequest request) {
    try {
      long consensusWriteStartTime = System.nanoTime();
      stateMachineLock.lock();
      long getStateMachineLockTime = System.nanoTime();
      // statistic the time of acquiring stateMachine lock
      pipeConsensusServerMetrics.recordGetStateMachineLockTime(
          getStateMachineLockTime - consensusWriteStartTime);
      long writeToStateMachineStartTime = System.nanoTime();
      if (request instanceof ComparableConsensusRequest) {
        ((ComparableConsensusRequest) request)
            .setProgressIndex(progressIndexManager.assignProgressIndex(thisNode.getGroupId()));
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
    try {
      long consensusWriteStartTime = System.nanoTime();
      stateMachineLock.lock();
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
      throw new ConsensusGroupModifyPeerException(e);
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
      createConsensusPipeToTargetPeer(targetPeer, false);
    } catch (Exception e) {
      LOGGER.warn(
          "{} cannot create consensus pipe to {}, may because target peer is unknown currently, please manually check!",
          thisNode,
          targetPeer,
          e);
      throw new ConsensusGroupModifyPeerException(e);
    }
  }

  public synchronized void createConsensusPipeToTargetPeer(
      Peer targetPeer, boolean needManuallyStart) throws ConsensusGroupModifyPeerException {
    try {
      KillPoint.setKillPoint(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE);
      consensusPipeManager.createConsensusPipe(thisNode, targetPeer, needManuallyStart);
      peerManager.addAndPersist(targetPeer);
    } catch (IOException e) {
      LOGGER.warn("{} cannot persist peer {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot persist peer %s", thisNode, targetPeer), e);
    } catch (Exception e) {
      LOGGER.warn("{} cannot create consensus pipe to {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot create consensus pipe to %s", thisNode, targetPeer), e);
    }
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
    try {
      consensusPipeManager.dropConsensusPipe(thisNode, targetPeer);
      peerManager.removeAndPersist(targetPeer);
    } catch (IOException e) {
      LOGGER.warn("{} cannot persist peer {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot persist peer %s", thisNode, targetPeer), e);
    } catch (Exception e) {
      LOGGER.warn("{} cannot drop consensus pipe to {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot drop consensus pipe to %s", thisNode, targetPeer), e);
    }
  }

  public void startOtherConsensusPipesToTargetPeer(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try {
        consensusPipeManager.updateConsensusPipe(
            new ConsensusPipeName(peer, targetPeer), PipeStatus.RUNNING);
      } catch (Exception e) {
        // just warn but not throw exceptions. Because there may exist unknown nodes in consensus
        // group
        LOGGER.warn("{} cannot start consensus pipe to {}", peer, targetPeer, e);
      }
    }
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
      Peer targetPeer, List<String> consensusPipeNames, boolean refreshCachedProgressIndex)
      throws ConsensusGroupModifyPeerException {
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
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot check consensus pipes transmission completed", thisNode), e);
    }
  }

  public synchronized boolean isConsensusPipesTransmissionCompleted(
      List<String> consensusPipeNames, boolean refreshCachedProgressIndex) {
    if (refreshCachedProgressIndex) {
      cachedProgressIndex =
          cachedProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
              progressIndexManager.getMaxAssignedProgressIndex(thisNode.getGroupId()));
    }

    try {
      return consensusPipeNames.stream()
          .noneMatch(
              name ->
                  cachedProgressIndex.isAfter(
                      progressIndexManager.getProgressIndex(new ConsensusPipeName(name))));
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
      throw new ConsensusGroupModifyPeerException(
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
