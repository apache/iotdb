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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.IStateMachine.Registry;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.AsyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.SyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.SyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.logdispatcher.IoTConsensusMemoryManager;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCService;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCServiceProcessor;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTConsensus implements IConsensus {

  private final Logger logger = LoggerFactory.getLogger(IoTConsensus.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, IoTConsensusServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final IoTConsensusRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private final IoTConsensusConfig config;
  private final IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager;
  private final IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager;
  private final ScheduledExecutorService retryService;

  public IoTConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getIotConsensusConfig();
    this.registry = registry;
    this.service = new IoTConsensusRPCService(thisNode, config.getIotConsensusConfig());
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncIoTConsensusServiceClient>()
            .createClientManager(
                new AsyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncIoTConsensusServiceClient>()
            .createClientManager(
                new SyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.retryService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.LOG_DISPATCHER_RETRY_EXECUTOR.getName());
    // init IoTConsensus memory manager
    IoTConsensusMemoryManager.getInstance()
        .init(
            config.getIotConsensusConfig().getReplication().getAllocateMemoryForConsensus(),
            config.getIotConsensusConfig().getReplication().getAllocateMemoryForQueue());
  }

  @Override
  public synchronized void start() throws IOException {
    initAndRecover();
    service.initAsyncedServiceImpl(new IoTConsensusRPCServiceProcessor(this));
    try {
      registerManager.register(service);
    } catch (StartupException e) {
      throw new IOException(e);
    }
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      if (!storageDir.mkdirs()) {
        throw new IOException(String.format("Unable to create consensus dir at %s", storageDir));
      }
    } else {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
        for (Path path : stream) {
          String[] items = path.getFileName().toString().split("_");
          ConsensusGroupId consensusGroupId =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
          IoTConsensusServerImpl consensus =
              new IoTConsensusServerImpl(
                  path.toString(),
                  new Peer(consensusGroupId, thisNodeId, thisNode),
                  new ArrayList<>(),
                  registry.apply(consensusGroupId),
                  retryService,
                  clientManager,
                  syncClientManager,
                  config);
          stateMachineMap.put(consensusGroupId, consensus);
          consensus.start();
        }
      }
    }
  }

  @Override
  public synchronized void stop() {
    stateMachineMap.values().parallelStream().forEach(IoTConsensusServerImpl::stop);
    clientManager.close();
    syncClientManager.close();
    registerManager.deregisterAll();
    retryService.shutdown();
    try {
      retryService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("{}: interrupted when shutting down add Executor with exception {}", this, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
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

  @SuppressWarnings("java:S2201")
  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      throw new IllegalPeerNumException(consensusGroupSize);
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, peers);
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    Optional.ofNullable(
            stateMachineMap.computeIfAbsent(
                groupId,
                k -> {
                  exist.set(false);

                  String path = buildPeerDir(storageDir, groupId);
                  File file = new File(path);
                  if (!file.mkdirs()) {
                    logger.warn("Unable to create consensus dir for group {} at {}", groupId, path);
                    return null;
                  }

                  IoTConsensusServerImpl impl =
                      new IoTConsensusServerImpl(
                          path,
                          new Peer(groupId, thisNodeId, thisNode),
                          peers,
                          registry.apply(groupId),
                          retryService,
                          clientManager,
                          syncClientManager,
                          config);
                  impl.start();
                  return impl;
                }))
        .orElseThrow(
            () ->
                new ConsensusException(
                    String.format("Unable to create consensus dir for group %s", groupId)));
    if (exist.get()) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    AtomicBoolean exist = new AtomicBoolean(false);
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          exist.set(true);
          v.stop();
          FileUtils.deleteDirectory(new File(buildPeerDir(storageDir, groupId)));
          return null;
        });
    if (!exist.get()) {
      throw new ConsensusGroupNotExistException(groupId);
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.getConfiguration().contains(peer)) {
      throw new PeerAlreadyInConsensusGroupException(groupId, peer);
    }
    try {
      // step 1: inactive new Peer to prepare for following steps
      logger.info("[IoTConsensus] inactivate new peer: {}", peer);
      impl.inactivePeer(peer);

      // step 2: notify all the other Peers to build the sync connection to newPeer
      logger.info("[IoTConsensus] notify current peers to build sync log...");
      impl.checkAndLockSafeDeletedSearchIndex();
      impl.notifyPeersToBuildSyncLogChannel(peer);

      // step 3: take snapshot
      logger.info("[IoTConsensus] start to take snapshot...");
      impl.takeSnapshot();

      // step 4: transit snapshot
      logger.info("[IoTConsensus] start to transit snapshot...");
      impl.transitSnapshot(peer);

      // step 5: let the new peer load snapshot
      logger.info("[IoTConsensus] trigger new peer to load snapshot...");
      impl.triggerSnapshotLoad(peer);

      // step 6: active new Peer
      logger.info("[IoTConsensus] activate new peer...");
      impl.activePeer(peer);

      // step 7: spot clean
      logger.info("[IoTConsensus] do spot clean...");
      doSpotClean(peer, impl);

    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  private void doSpotClean(Peer peer, IoTConsensusServerImpl impl) {
    try {
      impl.cleanupRemoteSnapshot(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      logger.warn("[IoTConsensus] failed to cleanup remote snapshot", e);
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    if (!impl.getConfiguration().contains(peer)) {
      throw new PeerNotInConsensusGroupException(groupId, peer.toString());
    }

    try {
      // let other peers remove the sync channel with target peer
      impl.notifyPeersToRemoveSyncLogChannel(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }

    try {
      // let target peer reject new write
      impl.inactivePeer(peer);
      // wait its SyncLog to complete
      impl.waitTargetPeerUntilSyncLogCompleted(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    throw new ConsensusException("IoTConsensus does not support leader transfer");
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    try {
      impl.takeSnapshot();
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    return true;
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
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  public IoTConsensusServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  public static String buildPeerDir(File storageDir, ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }
}
