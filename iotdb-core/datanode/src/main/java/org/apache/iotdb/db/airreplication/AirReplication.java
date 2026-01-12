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
import org.apache.iotdb.commons.client.async.AsyncAirReplicationServiceClient;
import org.apache.iotdb.commons.client.sync.SyncAirReplicationServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.iotv2.container.IoTV2GlobalComponentContainer;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusDeleteLocalPeerKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusRemovePeerCoordinatorKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.AirReplicationConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationGuardian;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationManager;
import org.apache.iotdb.consensus.air.airreplication.AirReplicationName;
import org.apache.iotdb.consensus.air.service.AirReplicationRPCService;
import org.apache.iotdb.consensus.air.service.AirReplicationRPCServiceProcessor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class AirReplication implements IConsensus {
  private static final String REPLICATION_AIR_GUARDIAN_TASK_ID = "replication_air_guardian";
  private static final String CLASS_NAME = AirReplication.class.getSimpleName();
  private static final Logger LOGGER = LoggerFactory.getLogger(AirReplication.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, AirReplicationServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final AirReplicationRPCService rpcService;
  private final RegisterManager registerManager = new RegisterManager();
  private final Map<ConsensusGroupId, ReentrantLock> replicationGroupIdReentrantLockMap =
      new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock stateMachineMapLock = new ReentrantReadWriteLock();
  private final AirReplicationConfig config;
  private final AirReplicationManager airReplicationManager;
  private final AirReplicationGuardian airReplicationGuardian;
  private final IClientManager<TEndPoint, AsyncAirReplicationServiceClient> asyncClientManager;
  private final IClientManager<TEndPoint, SyncAirReplicationServiceClient> syncClientManager;
  private Map<ConsensusGroupId, List<Peer>> correctPeerListBeforeStart = null;

  public AirReplication(ConsensusConfig config, IStateMachine.Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getAirReplicationConfig();
    this.registry = registry;
    this.rpcService = new AirReplicationRPCService(thisNode, config.getAirReplicationConfig());
    this.airReplicationManager =
        new AirReplicationManager(
            config.getAirReplicationConfig().getAir(),
            config.getAirReplicationConfig().getReplicateMode());
    this.airReplicationGuardian =
        config.getAirReplicationConfig().getAir().getAirReplicationGuardian();
    this.asyncClientManager =
        IoTV2GlobalComponentContainer.getInstance().getGlobalAsyncClientManager();
    this.syncClientManager =
        IoTV2GlobalComponentContainer.getInstance().getGlobalSyncClientManager();
  }

  @Override
  public synchronized void start() throws IOException {
    Future<Void> recoverFuture = initAndRecover();

    rpcService.initSyncedServiceImpl(new AirReplicationRPCServiceProcessor(this, config.getAir()));
    try {
      registerManager.register(rpcService);
    } catch (StartupException e) {
      throw new IOException(e);
    }

    try {
      recoverFuture.get();
    } catch (CancellationException ce) {
      LOGGER.info("IoTV2 Recover Task is cancelled", ce);
    } catch (ExecutionException ee) {
      LOGGER.error("Exception while waiting for recover future completion", ee);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      LOGGER.warn("IoTV2 Recover Task is interrupted", ie);
    }
    // only when we recover all replication group can we launch async backend checker thread
    airReplicationGuardian.start(
        REPLICATION_AIR_GUARDIAN_TASK_ID,
        this::checkAllAirReplication,
        config.getAir().getAirReplicationGuardJobIntervalInSeconds());
  }

  private Future<Void> initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      // init
      if (!storageDir.mkdirs()) {
        LOGGER.warn("Unable to create replication dir at {}", storageDir);
        throw new IOException(String.format("Unable to create replication dir at %s", storageDir));
      }
      return CompletableFuture.completedFuture(null);
    } else {
      // asynchronously recover, retry logic is implemented at AirReplicationImpl
      return CompletableFuture.runAsync(
              () -> {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
                  for (Path path : stream) {
                    ConsensusGroupId consensusGroupId =
                        parsePeerFileName(path.getFileName().toString());
                    try {
                      AirReplicationServerImpl replication =
                          new AirReplicationServerImpl(
                              new Peer(consensusGroupId, thisNodeId, thisNode),
                              registry.apply(consensusGroupId),
                              new ArrayList<>(),
                              config,
                              airReplicationManager,
                              syncClientManager);
                      stateMachineMap.put(consensusGroupId, replication);
                      checkPeerListAndStartIfEligible(consensusGroupId, replication);
                    } catch (Exception e) {
                      LOGGER.error(
                          "Failed to recover replication from {} for {}, ignore it and continue recover other group, async backend checker thread will automatically deregister related air side effects for this failed replication group.",
                          storageDir,
                          consensusGroupId,
                          e);
                    }
                  }
                } catch (IOException e) {
                  LOGGER.error(
                      "Failed to recover replication from {} because read dir failed", storageDir, e);
                }
              })
          .exceptionally(
              e -> {
                LOGGER.error("Failed to recover replication from {}", storageDir, e);
                return null;
              });
    }
  }

  private void checkPeerListAndStartIfEligible(
      ConsensusGroupId consensusGroupId, AirReplicationServerImpl replication) throws IOException {
    BiConsumer<ConsensusGroupId, List<Peer>> resetPeerListWithoutThrow =
        (dataRegionId, peers) -> {
          try {
            resetPeerList(dataRegionId, peers);
          } catch (ConsensusGroupNotExistException ignore) {

          } catch (Exception e) {
            LOGGER.warn("Failed to reset peer list while start", e);
          }
        };

    if (correctPeerListBeforeStart != null) {
      if (correctPeerListBeforeStart.containsKey(consensusGroupId)) {
        // make peers which are in list correct
        resetPeerListWithoutThrow.accept(
            consensusGroupId, correctPeerListBeforeStart.get(consensusGroupId));
        replication.start(true);
      } else {
        // clear peers which are not in the list
        resetPeerListWithoutThrow.accept(consensusGroupId, Collections.emptyList());
      }

    } else {
      replication.start(true);
    }
  }

  @Override
  public synchronized void stop() {
    asyncClientManager.close();
    syncClientManager.close();
    registerManager.deregisterAll();
    airReplicationGuardian.stop();
    stateMachineMap.values().parallelStream().forEach(AirReplicationServerImpl::stop);
    IoTV2GlobalComponentContainer.getInstance().stopBackgroundTaskService();
  }

  private void checkAllAirReplication() {
    final Map<ConsensusGroupId, Map<AirReplicationName, PipeStatus>> existedAirs =
        airReplicationManager.getAllAirReplication().entrySet().stream()
            .filter(entry -> entry.getKey().getSenderDataNodeId() == thisNodeId)
            .collect(
                Collectors.groupingBy(
                    entry -> entry.getKey().getConsensusGroupId(),
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    stateMachineMapLock.writeLock().lock();
    try {
      stateMachineMap.forEach(
          (key, value) ->
              value.checkAirReplication(existedAirs.getOrDefault(key, ImmutableMap.of())));
      existedAirs.entrySet().stream()
          .filter(entry -> !stateMachineMap.containsKey(entry.getKey()))
          .flatMap(entry -> entry.getValue().keySet().stream())
          .forEach(
              airReplicationName -> {
                try {
                  LOGGER.warn(
                      "{} drop air replication [{}]",
                      airReplicationName.getConsensusGroupId(),
                      airReplicationName);
                  airReplicationManager.updateAirReplication(airReplicationName, PipeStatus.DROPPED);
                } catch (Exception e) {
                  LOGGER.warn(
                      "{} cannot drop air replication [{}]",
                      airReplicationName.getConsensusGroupId(),
                      airReplicationName,
                      e);
                }
              });
    } finally {
      stateMachineMapLock.writeLock().unlock();
    }
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    final AirReplicationServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.isReadOnly()) {
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    } else if (!impl.isActive()) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          "current node is not active and is not ready to receive user write.");
    } else {
      return impl.write(request);
    }
  }

  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .orElseThrow(() -> new ConsensusGroupNotExistException(groupId))
        .read(request);
  }

  private String getPeerDir(ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }

  private ConsensusGroupId parsePeerFileName(String fileName) {
    String[] items = fileName.split("_");
    return ConsensusGroupId.Factory.create(Integer.parseInt(items[0]), Integer.parseInt(items[1]));
  }

  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    final int replicationGroupSize = peers.size();
    if (replicationGroupSize == 0) {
      throw new IllegalPeerNumException(replicationGroupSize);
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, peers);
    }

    Lock lock =
        replicationGroupIdReentrantLockMap.computeIfAbsent(groupId, key -> new ReentrantLock());
    lock.lock();
    try {
      stateMachineMapLock.readLock().lock();
      try {
        if (stateMachineMap.containsKey(groupId)) {
          throw new ConsensusGroupAlreadyExistException(groupId);
        }

        final String path = getPeerDir(groupId);
        File replicationDir = new File(path);
        if (!replicationDir.exists() && !replicationDir.mkdirs()) {
          LOGGER.warn("Unable to create replication dir for group {} at {}", groupId, path);
          throw new ConsensusException(
              String.format("Unable to create replication dir for group %s", groupId));
        }

        AirReplicationServerImpl replication =
            new AirReplicationServerImpl(
                new Peer(groupId, thisNodeId, thisNode),
                registry.apply(groupId),
                peers,
                config,
                airReplicationManager,
                syncClientManager);
        stateMachineMap.put(groupId, replication);
        replication.start(false); // air will start after creating
        KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER);
      } catch (IOException e) {
        LOGGER.warn("Cannot create local peer for group {} with peers {}", groupId, peers, e);
        throw new ConsensusException(e);
      } finally {
        stateMachineMapLock.readLock().unlock();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.BEFORE_DELETE);
    Lock lock =
        replicationGroupIdReentrantLockMap.computeIfAbsent(groupId, key -> new ReentrantLock());
    lock.lock();
    try {
      stateMachineMapLock.readLock().lock();
      try {
        if (!stateMachineMap.containsKey(groupId)) {
          throw new ConsensusGroupNotExistException(groupId);
        }
        LOGGER.info("[{}] start to delete local peer for group {}", CLASS_NAME, groupId);
        final AirReplicationServerImpl replication = stateMachineMap.get(groupId);
        replication.clear();
        stateMachineMap.remove(groupId);

        FileUtils.deleteFileOrDirectory(new File(getPeerDir(groupId)));
        KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.AFTER_DELETE);
        LOGGER.info("[{}] finish deleting local peer for group {}", CLASS_NAME, groupId);
      } finally {
        stateMachineMapLock.readLock().unlock();
      }
    } finally {
      lock.unlock();
      replicationGroupIdReentrantLockMap.remove(groupId);
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    AirReplicationServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.containsPeer(peer)) {
      throw new PeerAlreadyInConsensusGroupException(groupId, peer);
    }
    try {
      // step 1: inactive new Peer to prepare for following steps
      LOGGER.info("[{}] inactivate new peer: {}", CLASS_NAME, peer);
      impl.setRemotePeerActive(peer, false, false);

      // step 2: notify all the other Peers to create air replications to newPeer
      // NOTE: For this step, all the other peers will try to transfer its user write data to target
      LOGGER.info("[{}] notify current peers to create air replications...", CLASS_NAME);
      impl.notifyPeersToCreateAirReplications(peer);
      KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_TRANSITION);

      // step 3: wait until all other Peers finish transferring
      LOGGER.info("[{}] wait until all the other peers finish transferring...", CLASS_NAME);
      impl.waitPeersToTargetPeerTransmissionCompleted(peer);

      // step 4: active new Peer to let new Peer receive client requests
      LOGGER.info("[{}] activate new peer...", CLASS_NAME);
      impl.setRemotePeerActive(peer, true, false);
      KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_DONE);
    } catch (ConsensusGroupModifyPeerException e) {
      try {
        LOGGER.warn(
            "[{}] add remote peer failed, automatic cleanup side effects...", CLASS_NAME, e);

        // roll back
        impl.notifyPeersToDropAirReplication(peer);

      } catch (ConsensusGroupModifyPeerException mpe) {
        LOGGER.error(
            "[{}] failed to cleanup side effects after failed to add remote peer", CLASS_NAME, mpe);
      }
      throw new ConsensusException(e);
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    AirReplicationServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (!impl.containsPeer(peer)) {
      throw new PeerNotInConsensusGroupException(groupId, peer.toString());
    }
    KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.INIT);

    try {
      // let other peers to drop air replications to target
      LOGGER.info("[{}] notify other peers to drop air replications...", CLASS_NAME);
      impl.notifyPeersToDropAirReplication(peer);
      KillPoint.setKillPoint(
          IoTConsensusRemovePeerCoordinatorKillPoints
              .AFTER_NOTIFY_PEERS_TO_REMOVE_REPLICATE_CHANNEL);

      // let target peer reject new write
      LOGGER.info("[{}] inactivate peer {}", CLASS_NAME, peer);
      impl.setRemotePeerActive(peer, false, true);
      KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_INACTIVE_PEER);

      // wait its air replications to complete
      LOGGER.info("[{}] wait target peer{} complete transfer...", CLASS_NAME, peer);
      impl.waitTargetPeerToPeersTransmissionCompleted(peer);

      // wait target peer to release all resource
      LOGGER.info("[{}] wait {} to release all resource...", CLASS_NAME, peer);
      impl.waitReleaseAllRegionRelatedResource(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e);
    }
    KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.FINISH);
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(
      Map<ConsensusGroupId, List<Peer>> correctPeerList) {
    LOGGER.info("Record correct peer list: {}", correctPeerList);
    this.correctPeerListBeforeStart = correctPeerList;
  }

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    AirReplicationServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    if (!correctPeers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      LOGGER.warn(
          "[RESET PEER LIST] {} Local peer is not in the correct configuration, delete it.",
          groupId);
      deleteLocalPeer(groupId);
      return;
    }

    ImmutableList<Peer> currentPeers = ImmutableList.copyOf(impl.getPeers());
    String previousPeerListStr = impl.getPeers().toString();
    // remove invalid peer
    for (Peer peer : currentPeers) {
      if (!correctPeers.contains(peer)) {
        try {
          impl.dropAirReplicationToTargetPeer(peer);
          LOGGER.info("[RESET PEER LIST] {} Remove sync channel with: {}", groupId, peer);
        } catch (ConsensusGroupModifyPeerException e) {
          LOGGER.error(
              "[RESET PEER LIST] {} Failed to remove sync channel with: {}", groupId, peer, e);
        }
      }
    }
    // add correct peer
    for (Peer peer : correctPeers) {
      if (!impl.containsPeer(peer) && peer.getNodeId() != this.thisNodeId) {
        try {
          impl.createAirReplicationToTargetPeer(peer, false);
          LOGGER.info("[RESET PEER LIST] {} Build sync channel with: {}", groupId, peer);
        } catch (ConsensusGroupModifyPeerException e) {
          LOGGER.warn(
              "[RESET PEER LIST] {} Failed to build sync channel with: {}", groupId, peer, e);
        }
      }
    }
    // show result
    String currentPeerListStr = impl.getPeers().toString();
    if (!previousPeerListStr.equals(currentPeerListStr)) {
      LOGGER.info(
          "[RESET PEER LIST] {} Local peer list has been reset: {} -> {}",
          groupId,
          previousPeerListStr,
          impl.getPeers());
    } else {
      LOGGER.info(
          "[RESET PEER LIST] {} The current peer list is correct, nothing need to be reset: {}",
          groupId,
          previousPeerListStr);
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    throw new ConsensusException(String.format("%s does not support leader transfer", CLASS_NAME));
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    if (!stateMachineMap.containsKey(groupId)) {
      throw new ConsensusGroupNotExistException(groupId);
    }
    // Do nothing here because we do not need to transfer snapshot when there are new peers
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    // TODO: check logical clock
    return 0;
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    if (!stateMachineMap.containsKey(groupId)) {
      return null;
    }
    return new Peer(groupId, thisNodeId, thisNode);
  }

  @Override
  public int getReplicationNum(ConsensusGroupId groupId) {
    AirReplicationServerImpl impl = stateMachineMap.get(groupId);
    return impl != null ? impl.getPeers().size() : 0;
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return getPeerDir(groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    // AirReplication doesn't support reload consensus config, related config can be reloaded in
    // iotdb-core layer.
  }

  public AirReplicationServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }
}
