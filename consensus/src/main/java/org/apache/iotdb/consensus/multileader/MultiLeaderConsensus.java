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

package org.apache.iotdb.consensus.multileader;

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
import org.apache.iotdb.consensus.config.MultiLeaderConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAddPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.multileader.client.AsyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.client.MultiLeaderConsensusClientPool.AsyncMultiLeaderServiceClientPoolFactory;
import org.apache.iotdb.consensus.multileader.client.MultiLeaderConsensusClientPool.SyncMultiLeaderServiceClientPoolFactory;
import org.apache.iotdb.consensus.multileader.client.SyncMultiLeaderServiceClient;
import org.apache.iotdb.consensus.multileader.service.MultiLeaderRPCService;
import org.apache.iotdb.consensus.multileader.service.MultiLeaderRPCServiceProcessor;
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

public class MultiLeaderConsensus implements IConsensus {

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderConsensus.class);

  private final TEndPoint thisNode;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, MultiLeaderServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final MultiLeaderRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private final MultiLeaderConfig config;
  private final IClientManager<TEndPoint, AsyncMultiLeaderServiceClient> clientManager;
  private final IClientManager<TEndPoint, SyncMultiLeaderServiceClient> syncClientManager;

  public MultiLeaderConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNode();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getMultiLeaderConfig();
    this.registry = registry;
    this.service = new MultiLeaderRPCService(thisNode, config.getMultiLeaderConfig());
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncMultiLeaderServiceClient>()
            .createClientManager(
                new AsyncMultiLeaderServiceClientPoolFactory(config.getMultiLeaderConfig()));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncMultiLeaderServiceClient>()
            .createClientManager(
                new SyncMultiLeaderServiceClientPoolFactory(config.getMultiLeaderConfig()));
  }

  @Override
  public void start() throws IOException {
    initAndRecover();
    service.initAsyncedServiceImpl(new MultiLeaderRPCServiceProcessor(this));
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
          MultiLeaderServerImpl consensus =
              new MultiLeaderServerImpl(
                  path.toString(),
                  new Peer(consensusGroupId, thisNode),
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
    stateMachineMap.values().parallelStream().forEach(MultiLeaderServerImpl::stop);
    registerManager.deregisterAll();
  }

  @Override
  public ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest request) {
    MultiLeaderServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }

    TSStatus status;
    if (impl.isReadOnly()) {
      status = new TSStatus(TSStatusCode.READ_ONLY_SYSTEM_ERROR.getStatusCode());
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
    MultiLeaderServerImpl impl = stateMachineMap.get(groupId);
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
    if (!peers.contains(new Peer(groupId, thisNode))) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerEndpointException(thisNode, peers))
          .build();
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    stateMachineMap.computeIfAbsent(
        groupId,
        k -> {
          exist.set(false);
          String path = buildPeerDir(groupId);
          File file = new File(path);
          if (!file.mkdirs()) {
            logger.warn("Unable to create consensus dir for group {} at {}", groupId, path);
          }
          MultiLeaderServerImpl impl =
              new MultiLeaderServerImpl(
                  path,
                  new Peer(groupId, thisNode),
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
          FileUtils.deleteDirectory(new File(buildPeerDir(groupId)));
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
    MultiLeaderServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      // step 1: inactive new Peer to prepare for following steps
      logger.info("[MultiLeaderConsensus] inactivate new peer: {}", peer);
      impl.inactivePeer(peer);

      // step 2: notify all the other Peers to build the sync connection to newPeer
      logger.info("[MultiLeaderConsensus] notify current peers to build sync log...");
      impl.notifyPeersToBuildSyncLogChannel(peer);

      // step 3: take snapshot
      logger.info("[MultiLeaderConsensus] start to take snapshot...");
      impl.takeSnapshot();

      // step 4: transit snapshot
      logger.info("[MultiLeaderConsensus] start to transit snapshot...");
      impl.transitSnapshot(peer);

      // step 5: let the new peer load snapshot
      logger.info("[MultiLeaderConsensus] trigger new peer to load snapshot...");
      impl.triggerSnapshotLoad(peer);

      // step 6: active new Peer
      logger.info("[MultiLeaderConsensus] activate new peer...");
      impl.activePeer(peer);

    } catch (ConsensusGroupAddPeerException e) {
      logger.error("cannot execute addPeer() for {}", peer, e);
      return ConsensusGenericResponse.newBuilder()
          .setSuccess(false)
          .setException(new ConsensusException(e.getMessage()))
          .build();
    }

    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer) {
    MultiLeaderServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      impl.notifyPeersToRemoveSyncLogChannel(peer);
    } catch (ConsensusGroupAddPeerException e) {
      return ConsensusGenericResponse.newBuilder()
          .setSuccess(false)
          .setException(new ConsensusException(e.getMessage()))
          .build();
    }
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
    MultiLeaderServerImpl impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      impl.takeSnapshot();
    } catch (ConsensusGroupAddPeerException e) {
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
    return new Peer(groupId, thisNode);
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  public MultiLeaderServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  private String buildPeerDir(ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }
}
