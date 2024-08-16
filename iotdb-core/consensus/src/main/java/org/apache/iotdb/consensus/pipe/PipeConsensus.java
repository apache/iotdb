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
import org.apache.iotdb.commons.client.async.AsyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.client.container.PipeConsensusClientMgrContainer;
import org.apache.iotdb.commons.client.sync.SyncPipeConsensusServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.PipeConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeGuardian;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeManager;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.service.PipeConsensusRPCService;
import org.apache.iotdb.consensus.pipe.service.PipeConsensusRPCServiceProcessor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.iotdb.consensus.iot.IoTConsensus.getConsensusGroupIdsFromDir;

public class PipeConsensus implements IConsensus {
  private static final String CONSENSUS_PIPE_GUARDIAN_TASK_ID = "consensus_pipe_guardian";
  private static final String CLASS_NAME = PipeConsensus.class.getSimpleName();
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensus.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, PipeConsensusServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final PipeConsensusRPCService rpcService;
  private final RegisterManager registerManager = new RegisterManager();
  private final ReentrantLock stateMachineMapLock = new ReentrantLock();
  private final PipeConsensusConfig config;
  private final ConsensusPipeManager consensusPipeManager;
  private final ConsensusPipeGuardian consensusPipeGuardian;
  private final IClientManager<TEndPoint, AsyncPipeConsensusServiceClient> asyncClientManager;
  private final IClientManager<TEndPoint, SyncPipeConsensusServiceClient> syncClientManager;

  public PipeConsensus(ConsensusConfig config, IStateMachine.Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getPipeConsensusConfig();
    this.registry = registry;
    this.rpcService = new PipeConsensusRPCService(thisNode, config.getPipeConsensusConfig());
    this.consensusPipeManager =
        new ConsensusPipeManager(
            config.getPipeConsensusConfig().getPipe(),
            config.getPipeConsensusConfig().getReplicateMode());
    this.consensusPipeGuardian =
        config.getPipeConsensusConfig().getPipe().getConsensusPipeGuardian();
    this.asyncClientManager = PipeConsensusClientMgrContainer.getInstance().getAsyncClientManager();
    this.syncClientManager = PipeConsensusClientMgrContainer.getInstance().getSyncClientManager();
  }

  @Override
  public synchronized void start() throws IOException {
    initAndRecover();

    rpcService.initSyncedServiceImpl(new PipeConsensusRPCServiceProcessor(this, config.getPipe()));
    try {
      registerManager.register(rpcService);
    } catch (StartupException e) {
      throw new IOException(e);
    }

    consensusPipeGuardian.start(
        CONSENSUS_PIPE_GUARDIAN_TASK_ID,
        this::checkAllConsensusPipe,
        config.getPipe().getConsensusPipeGuardJobIntervalInSeconds());
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      // init
      if (!storageDir.mkdirs()) {
        LOGGER.warn("Unable to create consensus dir at {}", storageDir);
        throw new IOException(String.format("Unable to create consensus dir at %s", storageDir));
      }
    } else {
      // asynchronously recover, retry logic is implemented at PipeConsensusImpl
      CompletableFuture.runAsync(
              () -> {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
                  for (Path path : stream) {
                    ConsensusGroupId consensusGroupId =
                        parsePeerFileName(path.getFileName().toString());
                    PipeConsensusServerImpl consensus =
                        new PipeConsensusServerImpl(
                            new Peer(consensusGroupId, thisNodeId, thisNode),
                            registry.apply(consensusGroupId),
                            path.toString(),
                            new ArrayList<>(),
                            config,
                            consensusPipeManager,
                            syncClientManager);
                    stateMachineMap.put(consensusGroupId, consensus);
                    consensus.start(true);
                  }
                } catch (Exception e) {
                  LOGGER.error("Failed to recover consensus from {}", storageDir, e);
                }
              })
          .exceptionally(
              e -> {
                LOGGER.error("Failed to recover consensus from {}", storageDir, e);
                return null;
              });
    }
  }

  @Override
  public synchronized void stop() {
    asyncClientManager.close();
    syncClientManager.close();
    registerManager.deregisterAll();
    consensusPipeGuardian.stop();
    stateMachineMap.values().parallelStream().forEach(PipeConsensusServerImpl::stop);
  }

  private void checkAllConsensusPipe() {
    final Map<ConsensusGroupId, Map<ConsensusPipeName, PipeStatus>> existedPipes =
        consensusPipeManager.getAllConsensusPipe().entrySet().stream()
            .filter(entry -> entry.getKey().getSenderDataNodeId() == thisNodeId)
            .collect(
                Collectors.groupingBy(
                    entry -> entry.getKey().getConsensusGroupId(),
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    try {
      stateMachineMapLock.lock();
      stateMachineMap.forEach(
          (key, value) ->
              value.checkConsensusPipe(existedPipes.getOrDefault(key, ImmutableMap.of())));
      existedPipes.entrySet().stream()
          .filter(entry -> !stateMachineMap.containsKey(entry.getKey()))
          .flatMap(entry -> entry.getValue().keySet().stream())
          .forEach(
              consensusPipeName -> {
                try {
                  LOGGER.warn(
                      "{} drop consensus pipe [{}]",
                      consensusPipeName.getConsensusGroupId(),
                      consensusPipeName);
                  consensusPipeManager.updateConsensusPipe(consensusPipeName, PipeStatus.DROPPED);
                } catch (Exception e) {
                  LOGGER.warn(
                      "{} cannot drop consensus pipe [{}]",
                      consensusPipeName.getConsensusGroupId(),
                      consensusPipeName,
                      e);
                }
              });
    } finally {
      stateMachineMapLock.unlock();
    }
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    final PipeConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.isReadOnly()) {
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    } else if (!impl.isActive()) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          "peer is inactive and not ready to receive sync log request.");
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
    final int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      throw new IllegalPeerNumException(consensusGroupSize);
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, peers);
    }
    if (stateMachineMap.containsKey(groupId)) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }

    try {
      stateMachineMapLock.lock();

      final String path = getPeerDir(groupId);
      if (!new File(path).mkdirs()) {
        LOGGER.warn("Unable to create consensus dir for group {} at {}", groupId, path);
        throw new ConsensusException(
            String.format("Unable to create consensus dir for group %s", groupId));
      }

      PipeConsensusServerImpl consensus =
          new PipeConsensusServerImpl(
              new Peer(groupId, thisNodeId, thisNode),
              registry.apply(groupId),
              path,
              peers,
              config,
              consensusPipeManager,
              syncClientManager);
      stateMachineMap.put(groupId, consensus);
      consensus.start(false); // pipe will start after creating
    } catch (IOException e) {
      LOGGER.warn("Cannot create local peer for group {} with peers {}", groupId, peers, e);
      throw new ConsensusException(e);
    } finally {
      stateMachineMapLock.unlock();
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    if (!stateMachineMap.containsKey(groupId)) {
      throw new ConsensusGroupNotExistException(groupId);
    }

    try {
      stateMachineMapLock.lock();

      final PipeConsensusServerImpl consensus = stateMachineMap.get(groupId);
      consensus.clear();

      FileUtils.deleteFileOrDirectory(new File(getPeerDir(groupId)));
    } catch (IOException e) {
      LOGGER.warn("Cannot delete local peer for group {}", groupId, e);
      throw new ConsensusException(e);
    } finally {
      stateMachineMapLock.unlock();
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.containsPeer(peer)) {
      throw new PeerAlreadyInConsensusGroupException(groupId, peer);
    }
    try {
      // step 1: inactive new Peer to prepare for following steps
      LOGGER.info("[{}] inactivate new peer: {}", CLASS_NAME, peer);
      impl.setRemotePeerActive(peer, false);

      // step 2: notify all the other Peers to create consensus pipes to newPeer
      LOGGER.info("[{}] notify current peers to create consensus pipes...", CLASS_NAME);
      impl.notifyPeersToCreateConsensusPipes(peer);

      // step 3: wait until all the other Peers finish transferring
      LOGGER.info("[{}] wait until all the other peers finish transferring...", CLASS_NAME);
      impl.waitPeersToTargetPeerTransmissionCompleted(peer);

      // step 4: active new Peer
      LOGGER.info("[{}] activate new peer...", CLASS_NAME);
      impl.setRemotePeerActive(peer, true);
    } catch (ConsensusGroupModifyPeerException e) {
      try {
        LOGGER.info("[{}] add remote peer failed, automatic cleanup side effects...", CLASS_NAME);

        // roll back
        impl.notifyPeersToDropConsensusPipe(peer);

      } catch (ConsensusGroupModifyPeerException mpe) {
        LOGGER.error(
            "[{}] failed to cleanup side effects after failed to add remote peer", CLASS_NAME, mpe);
      }
      throw new ConsensusException(e);
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (!impl.containsPeer(peer)) {
      throw new PeerNotInConsensusGroupException(groupId, peer.toString());
    }

    try {
      // let other peers remove the consensus pipe to target peer
      impl.notifyPeersToDropConsensusPipe(peer);
      // let target peer reject new write
      impl.setRemotePeerActive(peer, false);
      // wait its consensus pipes to complete
      impl.waitTargetPeerToPeersTransmissionCompleted(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    PipeConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (!correctPeers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      LOGGER.warn(
          "[RESET PEER LIST] Local peer is not in the correct configuration, delete local peer {}",
          groupId);
      deleteLocalPeer(groupId);
      return;
    }
    String previousPeerListStr = impl.getPeers().toString();
    for (Peer peer : impl.getPeers()) {
      if (!correctPeers.contains(peer)) {
        try {
          impl.dropConsensusPipeToTargetPeer(peer);
        } catch (ConsensusGroupModifyPeerException e) {
          LOGGER.error(
              "[RESET PEER LIST] Failed to remove peer {}'s consensus pipe from group {}",
              peer,
              groupId,
              e);
        }
      }
    }
    LOGGER.info(
        "[RESET PEER LIST] Local peer list has been reset: {} -> {}",
        previousPeerListStr,
        impl.getPeers());
    for (Peer peer : correctPeers) {
      if (!impl.containsPeer(peer)) {
        LOGGER.warn("[RESET PEER LIST] \"Correct peer\" {} is not in local peer list", peer);
      }
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
    PipeConsensusServerImpl impl = stateMachineMap.get(groupId);
    return impl != null ? impl.getPeers().size() : 0;
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIdsWithoutStarting() {
    return getConsensusGroupIdsFromDir(storageDir, LOGGER);
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return getPeerDir(groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    // TODO: impl for hot config loading
  }

  public PipeConsensusServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  //////////////////////////// APIs provided for Test ////////////////////////////
  @TestOnly
  public int getPipeCount() {
    return this.consensusPipeManager.getAllConsensusPipe().size();
  }
}
