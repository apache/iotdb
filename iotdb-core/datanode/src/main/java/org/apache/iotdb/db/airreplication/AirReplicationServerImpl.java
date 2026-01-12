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

package org.apache.iotdb.consensus.air;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncAirReplicationServiceClient;
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
import org.apache.iotdb.consensus.config.AirReplicationConfig;
import org.apache.iotdb.consensus.config.AirReplicationConfig.ReplicateMode;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationManager;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationName;
import org.apache.iotdb.consensus.air.airreplication.ReplicateProgressManager;
import org.apache.iotdb.consensus.air.metric.AirReplicationServerMetrics;
import org.apache.iotdb.consensus.air.thrift.TCheckAirReplicationCompletedReq;
import org.apache.iotdb.consensus.air.thrift.TCheckAirReplicationCompletedResp;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToCreateAirReplicationReq;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToCreateAirReplicationResp;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToDropAirReplicationReq;
import org.apache.iotdb.consensus.air.thrift.TNotifyPeerToDropAirReplicationResp;
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

/** AirReplicationServerImpl is a replication server implementation for air replication. */
public class AirReplicationServerImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(AirReplicationServerImpl.class);
  private static final long CHECK_TRANSMISSION_COMPLETION_INTERVAL_IN_MILLISECONDS = 2_000L;
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();
  private static final long RETRY_WAIT_TIME_IN_MS = 500;
  private static final long MAX_RETRY_TIMES = 20;
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final Lock stateMachineLock = new ReentrantLock();
  private final AirReplicationPeerManager peerManager;
  private final AtomicBoolean active;
  private final AtomicBoolean isStarted;
  private final String consensusGroupId;
  private final AirReplicationManager airReplicationManager;
  private final ReplicateProgressManager replicateProgressManager;
  private final IClientManager<TEndPoint, SyncAirReplicationServiceClient> syncClientManager;
  private final AirReplicationServerMetrics airReplicationServerMetrics;
  private final ReplicateMode replicateMode;

  private ProgressIndex cachedProgressIndex = MinimumProgressIndex.INSTANCE;

  public AirReplicationServerImpl(
      Peer thisNode,
      IStateMachine stateMachine,
      List<Peer> peers,
      AirReplicationConfig config,
      AirReplicationManager airReplicationManager,
      IClientManager<TEndPoint, SyncAirReplicationServiceClient> syncClientManager)
      throws IOException {
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.peerManager = new AirReplicationPeerManager(peers);
    this.active = new AtomicBoolean(true);
    this.isStarted = new AtomicBoolean(false);
    this.consensusGroupId = thisNode.getGroupId().toString();
    this.airReplicationManager = airReplicationManager;
    this.replicateProgressManager = config.getAir().getProgressIndexManager();
    this.syncClientManager = syncClientManager;
    this.airReplicationServerMetrics = new AirReplicationServerMetrics(this);
    this.replicateMode = config.getReplicateMode();

    // if peers is empty, the `resetPeerList` will automatically fetch correct peers' info from CN.
    if (!peers.isEmpty()) {
      // create air replications
      Set<Peer> deepCopyPeersWithoutSelf =
          peers.stream().filter(peer -> !peer.equals(thisNode)).collect(Collectors.toSet());
      final List<Peer> successfulAirs = createAirReplications(deepCopyPeersWithoutSelf);
      if (successfulAirs.size() < deepCopyPeersWithoutSelf.size()) {
        // roll back
        updateAirReplicationsStatus(successfulAirs, PipeStatus.DROPPED);
        throw new IOException(String.format("%s cannot create all air replications", thisNode));
      }
    }
  }

  @SuppressWarnings("java:S2276")
  public synchronized void start(boolean startConsensusPipes) throws IOException {
    stateMachine.start();
    MetricService.getInstance().addMetricSet(this.airReplicationServerMetrics);

    if (startConsensusPipes) {
      // start all air replications
      final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
      List<Peer> failedAirs =
          updateAirReplicationsStatus(new ArrayList<>(otherPeers), PipeStatus.RUNNING);
      // considering procedure can easily time out, keep trying updateAirReplicationsStatus until all
      // air replications are started gracefully or exceed the maximum number of attempts.
      // NOTE: start air procedure is idempotent guaranteed.
      try {
        for (int i = 0; i < MAX_RETRY_TIMES && !failedAirs.isEmpty(); i++) {
          failedAirs = updateAirReplicationsStatus(failedAirs, PipeStatus.RUNNING);
          Thread.sleep(RETRY_WAIT_TIME_IN_MS);
        }
      } catch (InterruptedException e) {
        LOGGER.warn(
            "AirReplicationImpl-peer{}: airReplicationImpl thread get interrupted when start air replication. May because IoTDB process is killed.",
            thisNode);
        throw new IOException(String.format("%s cannot start all air replications", thisNode));
      }
      // if there still are some air replications failed to start, throw an exception.
      if (!failedAirs.isEmpty()) {
        // roll back
        List<Peer> successfulAirs = new ArrayList<>(otherPeers);
        successfulAirs.removeAll(failedAirs);
        updateAirReplicationsStatus(successfulAirs, PipeStatus.STOPPED);
        throw new IOException(String.format("%s cannot start all air replications", thisNode));
      }
    }
    isStarted.set(true);
  }

  public synchronized void stop() {
    // stop all air replications
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    final List<Peer> failedAirs =
        updateAirReplicationsStatus(new ArrayList<>(otherPeers), PipeStatus.STOPPED);
    if (!failedAirs.isEmpty()) {
      // do not roll back, because it will stop anyway
      LOGGER.warn("{} cannot stop all air replications", thisNode);
    }
    MetricService.getInstance().removeMetricSet(this.airReplicationServerMetrics);
    stateMachine.stop();
    isStarted.set(false);
  }

  public synchronized void clear() {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    final List<Peer> failedAirs =
        updateAirReplicationsStatus(new ArrayList<>(otherPeers), PipeStatus.DROPPED);
    if (!failedAirs.isEmpty()) {
      // do not roll back, because it will clear anyway
      LOGGER.warn("{} cannot drop all air replications", thisNode);
    }

    MetricService.getInstance().removeMetricSet(this.airReplicationServerMetrics);
    peerManager.clear();
    stateMachine.stop();
    isStarted.set(false);
    active.set(false);
  }

  private List<Peer> createAirReplications(Set<Peer> peers) {
    return peers.stream()
        .filter(
            peer -> {
              try {
                if (!peers.equals(thisNode)) {
                  airReplicationManager.createAirReplication(thisNode, peer);
                }
                return true;
              } catch (Exception e) {
                LOGGER.warn(
                    "{}: cannot create air replication between {} and {}",
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
   * update given air replications' status, returns the peer corresponding to the air that failed to
   * update
   */
  private List<Peer> updateAirReplicationsStatus(List<Peer> peers, PipeStatus status) {
    return peers.stream()
        .filter(
            peer -> {
              try {
                if (!peer.equals(thisNode)) {
                  airReplicationManager.updateAirReplication(
                      new AirReplicationName(thisNode, peer), status);
                }
                return false;
              } catch (Exception e) {
                LOGGER.warn(
                    "{}: cannot update air replication between {} and {} to status {}",
                    e.getMessage(),
                    thisNode,
                    peer,
                    status);
                return true;
              }
            })
        .collect(Collectors.toList());
  }

  public synchronized void checkAirReplication(Map<AirReplicationName, PipeStatus> existedAirs) {
    final PipeStatus expectedStatus = isStarted.get() ? PipeStatus.RUNNING : PipeStatus.STOPPED;
    final Map<AirReplicationName, Peer> expectedAirs =
        peerManager.getOtherPeers(thisNode).stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    peer -> new AirReplicationName(thisNode, peer), peer -> peer));

    existedAirs.forEach(
        (existedName, existedStatus) -> {
          if (!expectedAirs.containsKey(existedName)) {
            try {
              LOGGER.warn("{} drop air replication [{}]", consensusGroupId, existedName);
              airReplicationManager.updateAirReplication(existedName, PipeStatus.DROPPED);
            } catch (Exception e) {
              LOGGER.warn("{} cannot drop air replication [{}]", consensusGroupId, existedName, e);
            }
          } else if (!expectedStatus.equals(existedStatus)) {
            try {
              LOGGER.warn(
                  "{} update air replication [{}] to status {}",
                  consensusGroupId,
                  existedName,
                  expectedStatus);
              if (expectedStatus.equals(PipeStatus.RUNNING)) {
                // Do nothing. Because Air framework's metaSync will do that.
                return;
              }
              airReplicationManager.updateAirReplication(existedName, expectedStatus);
            } catch (Exception e) {
              LOGGER.warn(
                  "{} cannot update air replication [{}] to status {}",
                  consensusGroupId,
                  existedName,
                  expectedStatus,
                  e);
            }
          }
        });

    expectedAirs.forEach(
        (expectedName, expectedPeer) -> {
          if (!existedAirs.containsKey(expectedName)) {
            try {
              LOGGER.warn(
                  "{} create and update air replication [{}] to status {}",
                  consensusGroupId,
                  expectedName,
                  expectedStatus);
              airReplicationManager.createAirReplication(thisNode, expectedPeer);
              airReplicationManager.updateAirReplication(expectedName, expectedStatus);
            } catch (Exception e) {
              LOGGER.warn(
                  "{} cannot create and update air replication [{}] to status {}",
                  consensusGroupId,
                  expectedName,
                  expectedStatus,
                  e);
            }
          }
        });
  }

  public TSStatus write(IConsensusRequest request) {
    stateMachineLock.lock();
    try {
      long consensusWriteStartTime = System.nanoTime();
      long getStateMachineLockTime = System.nanoTime();
      // statistic the time of acquiring stateMachine lock
      airReplicationServerMetrics.recordGetStateMachineLockTime(
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
      airReplicationServerMetrics.recordUserWriteStateMachineTime(
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
      airReplicationServerMetrics.recordGetStateMachineLockTime(
          getStateMachineLockTime - consensusWriteStartTime);

      long writeToStateMachineStartTime = System.nanoTime();
      TSStatus result = stateMachine.write(request);
      long writeToStateMachineEndTime = System.nanoTime();

      PERFORMANCE_OVERVIEW_METRICS.recordEngineCost(
          writeToStateMachineEndTime - writeToStateMachineStartTime);
      // statistic the time of writing request into stateMachine
      airReplicationServerMetrics.recordReplicaWriteStateMachineTime(
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
    try (SyncAirReplicationServiceClient client =
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

  public void notifyPeersToCreateAirReplications(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try (SyncAirReplicationServiceClient client =
          syncClientManager.borrowClient(peer.getEndpoint())) {
        TNotifyPeerToCreateAirReplicationResp resp =
            client.notifyPeerToCreateAirReplication(
                new TNotifyPeerToCreateAirReplicationReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId(),
                    targetPeer.getEndpoint(),
                    targetPeer.getNodeId()));
        if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when notify peer %s to create air replication", peer));
        }
      } catch (Exception e) {
        LOGGER.warn(
            "{} cannot notify peer {} to create air replication, may because that peer is unknown currently, please manually check!",
            thisNode,
            peer,
            e);
      }
    }

    try {
      // This node which acts as coordinator will transfer complete historical snapshot to new
      // target.
      createAirReplicationToTargetPeer(targetPeer, false);
    } catch (Exception e) {
      LOGGER.warn(
          "{} cannot create air replication to {}, may because target peer is unknown currently, please manually check!",
          thisNode,
          targetPeer,
          e);
      throw new ConsensusGroupModifyPeerException(e);
    }
  }

  public synchronized void createAirReplicationToTargetPeer(
      Peer targetPeer, boolean needManuallyStart) throws ConsensusGroupModifyPeerException {
    try {
      KillPoint.setKillPoint(DataNodeKillPoints.ORIGINAL_ADD_PEER_DONE);
      airReplicationManager.createAirReplication(thisNode, targetPeer, needManuallyStart);
      peerManager.addPeer(targetPeer);
    } catch (Exception e) {
      LOGGER.warn("{} cannot create air replication to {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot create air replication to %s", thisNode, targetPeer), e);
    }
  }

  public void notifyPeersToDropAirReplication(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try (SyncAirReplicationServiceClient client =
          syncClientManager.borrowClient(peer.getEndpoint())) {
        TNotifyPeerToDropAirReplicationResp resp =
            client.notifyPeerToDropAirReplication(
                new TNotifyPeerToDropAirReplicationReq(
                    targetPeer.getGroupId().convertToTConsensusGroupId(),
                    targetPeer.getEndpoint(),
                    targetPeer.getNodeId()));
        if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
          throw new ConsensusGroupModifyPeerException(
              String.format("error when notify peer %s to drop air replication", peer));
        }
      } catch (Exception e) {
        LOGGER.warn(
            "{} cannot notify peer {} to drop air replication, may because that peer is unknown currently, please manually check!",
            thisNode,
            peer,
            e);
      }
    }

    try {
      dropAirReplicationToTargetPeer(targetPeer);
    } catch (Exception e) {
      LOGGER.warn(
          "{} cannot drop air replication to {}, may because target peer is unknown currently, please manually check!",
          thisNode,
          targetPeer,
          e);
      throw new ConsensusGroupModifyPeerException(e);
    }
  }

  public synchronized void dropAirReplicationToTargetPeer(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    try {
      airReplicationManager.dropAirReplication(thisNode, targetPeer);
      peerManager.removePeer(targetPeer);
    } catch (Exception e) {
      LOGGER.warn("{} cannot drop air replication to {}", thisNode, targetPeer, e);
      throw new ConsensusGroupModifyPeerException(
          String.format("%s cannot drop air replication to %s", thisNode, targetPeer), e);
    }
  }

  public void startOtherAirReplicationsToTargetPeer(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);
    for (Peer peer : otherPeers) {
      if (peer.equals(targetPeer)) {
        continue;
      }
      try {
        airReplicationManager.updateAirReplication(
            new AirReplicationName(peer, targetPeer), PipeStatus.RUNNING);
      } catch (Exception e) {
        // just warn but not throw exceptions. Because there may exist unknown nodes in replication
        // group
        LOGGER.warn("{} cannot start air replication to {}", peer, targetPeer, e);
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

        if (isAirReplicationsTransmissionCompleted(
            Collections.singletonList(new AirReplicationName(thisNode, targetPeer).toString()),
            isFirstCheckForCurrentPeer)) {
          final List<Peer> otherPeers = peerManager.getOtherPeers(thisNode);

          isTransmissionCompleted = true;
          for (Peer peer : otherPeers) {
            if (!peer.equals(targetPeer)) {
              isTransmissionCompleted &=
                  isRemotePeerAirReplicationsTransmissionCompleted(
                      peer,
                      Collections.singletonList(new AirReplicationName(peer, targetPeer).toString()),
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

        final List<String> airReplicationNames =
            peerManager.getPeers().stream()
                .filter(peer -> !peer.equals(targetPeer))
                .map(peer -> new AirReplicationName(targetPeer, peer).toString())
                .collect(Collectors.toList());
        isTransmissionCompleted =
            isRemotePeerAirReplicationsTransmissionCompleted(
                targetPeer, airReplicationNames, isFirstCheck);

        isFirstCheck = false;
      }
    } catch (InterruptedException e) {
      LOGGER.warn("{} is interrupted when waiting for transfer completed", thisNode, e);
      Thread.currentThread().interrupt();
      throw new ConsensusGroupModifyPeerException(
          String.format("%s is interrupted when waiting for transfer completed", thisNode), e);
    }
  }

  private boolean isRemotePeerAirReplicationsTransmissionCompleted(
      Peer targetPeer, List<String> airReplicationNames, boolean refreshCachedProgressIndex) {
    try (SyncAirReplicationServiceClient client =
        syncClientManager.borrowClient(targetPeer.getEndpoint())) {
      TCheckAirReplicationCompletedResp resp =
          client.checkAirReplicationCompleted(
              new TCheckAirReplicationCompletedReq(
                  thisNode.getGroupId().convertToTConsensusGroupId(),
                  airReplicationNames,
                  refreshCachedProgressIndex));
      if (!RpcUtils.SUCCESS_STATUS.equals(resp.getStatus())) {
        LOGGER.warn(
            "{} cannot check air replications transmission completed to peer {}",
            thisNode,
            targetPeer);
        throw new ConsensusGroupModifyPeerException(
            String.format(
                "error when check air replications transmission completed to peer %s", targetPeer));
      }
      return resp.isCompleted;
    } catch (Exception e) {
      LOGGER.warn("{} cannot check air replications transmission completed", thisNode, e);
      return true;
    }
  }

  public boolean isAirReplicationsTransmissionCompleted(List<String> airReplicationNames) {
    return airReplicationNames.stream()
        .noneMatch(
            airName ->
                replicateProgressManager.getSyncLagForSpecificAirReplication(
                        thisNode.getGroupId(), new AirReplicationName(airName))
                    > 0);
  }

  public synchronized boolean isAirReplicationsTransmissionCompleted(
      List<String> airReplicationNames, boolean refreshCachedProgressIndex) {
    if (refreshCachedProgressIndex) {
      cachedProgressIndex =
          cachedProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
              replicateProgressManager.getMaxAssignedProgressIndex(thisNode.getGroupId()));
    }

    try {
      return airReplicationNames.stream()
          .noneMatch(
              name ->
                  cachedProgressIndex.isAfter(
                      replicateProgressManager.getProgressIndex(new AirReplicationName(name))));
    } catch (PipeException e) {
      LOGGER.info(e.getMessage());
      return false;
    }
  }

  public void waitReleaseAllRegionRelatedResource(Peer targetPeer)
      throws ConsensusGroupModifyPeerException {
    long checkIntervalInMs = 10_000L;
    try (SyncAirReplicationServiceClient client =
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
