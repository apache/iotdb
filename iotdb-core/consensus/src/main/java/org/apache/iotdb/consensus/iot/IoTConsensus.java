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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.IStateMachine.Registry;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
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
import java.util.concurrent.ConcurrentHashMap;
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

  public IoTConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getIoTConsensusConfig();
    this.registry = registry;
    this.service = new IoTConsensusRPCService(thisNode, config.getIoTConsensusConfig());
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncIoTConsensusServiceClient>()
            .createClientManager(
                new AsyncIoTConsensusServiceClientPoolFactory(config.getIoTConsensusConfig()));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncIoTConsensusServiceClient>()
            .createClientManager(
                new SyncIoTConsensusServiceClientPoolFactory(config.getIoTConsensusConfig()));
    // init IoTConsensus memory manager
    IoTConsensusMemoryManager.getInstance()
        .init(
            config.getIoTConsensusConfig().getReplication().getAllocateMemoryForConsensus(),
            config.getIoTConsensusConfig().getReplication().getAllocateMemoryForQueue());
  }

  @Override
  public void start() throws IOException {
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
        logger.warn("Unable to create consensus dir at {}", storageDir);
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
  public void stop() {
    clientManager.close();
    stateMachineMap.values().parallelStream().forEach(IoTConsensusServerImpl::stop);
    registerManager.deregisterAll();
  }

  @Override
  public ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest request) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }

    TSStatus status;
    if (impl.isReadOnly()) {
      status = new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
      status.setMessage("Fail to do non-query operations because system is read-only.");
    } else if (!impl.isActive()) {
      // TODO: (xingtanzjr) whether we need to define a new status to indicate the inactive status ?
      status = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT);
      status.setMessage("peer is inactive and not ready to receive sync log request.");
    } else {
      status = impl.write(request);
    }
    return ConsensusWriteResponse.newBuilder().setStatus(status).build();
  }

  @Override
  public ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest request) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusReadResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return ConsensusReadResponse.newBuilder().setDataSet(impl.read(request)).build();
  }

  @Override
  public ConsensusGenericResponse createPeer(ConsensusGroupId groupId, List<Peer> peers) {
    int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerNumException(consensusGroupSize))
          .build();
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerEndpointException(thisNode, peers))
          .build();
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    stateMachineMap.computeIfAbsent(
        groupId,
        k -> {
          exist.set(false);
          String path = buildPeerDir(storageDir, groupId);
          File file = new File(path);
          if (!file.mkdirs()) {
            logger.warn("Unable to create consensus dir for group {} at {}", groupId, path);
          }
          IoTConsensusServerImpl impl =
              new IoTConsensusServerImpl(
                  path,
                  new Peer(groupId, thisNodeId, thisNode),
                  peers,
                  registry.apply(groupId),
                  clientManager,
                  syncClientManager,
                  config);
          impl.start();
          return impl;
        });
    if (exist.get()) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupAlreadyExistException(groupId))
          .build();
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse deletePeer(ConsensusGroupId groupId) {
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
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse addPeer(ConsensusGroupId groupId, Peer peer) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
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
      logger.error("cannot execute addPeer() for {}", peer, e);
      return ConsensusGenericResponse.newBuilder()
          .setSuccess(false)
          .setException(new ConsensusException(e.getMessage()))
          .build();
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  private void doSpotClean(Peer peer, IoTConsensusServerImpl impl) {
    try {
      impl.cleanupRemoteSnapshot(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      logger.warn("[IoTConsensus] failed to cleanup remote snapshot", e);
    }
  }

  @Override
  public ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      // let other peers remove the sync channel with target peer
      impl.notifyPeersToRemoveSyncLogChannel(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      return ConsensusGenericResponse.newBuilder()
          .setSuccess(false)
          .setException(new ConsensusException(e.getMessage()))
          .build();
    }

    try {
      // let target peer reject new write
      impl.inactivePeer(peer);
      // wait its SyncLog to complete
      impl.waitTargetPeerUntilSyncLogCompleted(peer);
    } catch (ConsensusGroupModifyPeerException e) {
      // we only log warning here because sometimes the target peer may already be down
      logger.warn("cannot wait {} to complete SyncLog. error message: {}", peer, e.getMessage());
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse updatePeer(ConsensusGroupId groupId, Peer oldPeer, Peer newPeer) {
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      impl.takeSnapshot();
    } catch (ConsensusGroupModifyPeerException e) {
      return ConsensusGenericResponse.newBuilder()
          .setSuccess(false)
          .setException(new ConsensusException(e.getMessage()))
          .build();
    }
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
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
