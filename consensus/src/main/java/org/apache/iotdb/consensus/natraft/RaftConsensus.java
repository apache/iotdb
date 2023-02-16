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

package org.apache.iotdb.consensus.natraft;

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
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.natraft.client.AsyncRaftServiceClient;
import org.apache.iotdb.consensus.natraft.client.RaftConsensusClientPool.AsyncRaftServiceClientPoolFactory;
import org.apache.iotdb.consensus.natraft.exception.CheckConsistencyException;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.service.RaftRPCService;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RaftConsensus implements IConsensus {

  private static final Logger logger = LoggerFactory.getLogger(RaftConsensus.class);
  private final TEndPoint thisNode;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, RaftMember> stateMachineMap = new ConcurrentHashMap<>();
  private final RaftRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private final RaftConfig config;
  private final IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;

  public RaftConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNode();
    this.storageDir = new File(config.getStorageDir());
    this.config = new RaftConfig(config);
    this.registry = registry;
    this.service = new RaftRPCService(thisNode, this.config);
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncRaftServiceClient>()
            .createClientManager(new AsyncRaftServiceClientPoolFactory(this.config));
  }

  @Override
  public void start() throws IOException {
    initAndRecover();
    service.initAsyncedServiceImpl(new RaftRPCService(thisNode, config));
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
          Path fileName = path.getFileName();
          String[] items = fileName.toString().split("_");
          ConsensusGroupId consensusGroupId =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
          RaftMember raftMember =
              new RaftMember(
                  config,
                  thisNode,
                  new ArrayList<>(),
                  consensusGroupId,
                  registry.apply(consensusGroupId),
                  clientManager);
          stateMachineMap.put(consensusGroupId, raftMember);
          raftMember.start();
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {
    clientManager.close();
    stateMachineMap.values().parallelStream().forEach(RaftMember::stop);
    registerManager.deregisterAll();
  }

  @Override
  public ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest request) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }

    TSStatus status;
    if (impl.isReadOnly()) {
      status = new TSStatus(TSStatusCode.READ_ONLY_SYSTEM_ERROR.getStatusCode());
      status.setMessage("Fail to do non-query operations because system is read-only.");
    } else {
      status = impl.processRequest(request);
    }
    return ConsensusWriteResponse.newBuilder().setStatus(status).build();
  }

  @Override
  public ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest request) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusReadResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    try {
      return ConsensusReadResponse.newBuilder().setDataSet(impl.read(request)).build();
    } catch (CheckConsistencyException e) {
      return ConsensusReadResponse.newBuilder().setException(e).build();
    }
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
          RaftMember impl =
              new RaftMember(
                  config,
                  thisNode,
                  peers.stream().map(Peer::getEndpoint).collect(Collectors.toList()),
                  groupId,
                  registry.apply(groupId),
                  clientManager);
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

  private String buildPeerDir(ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
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
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    return ConsensusGenericResponse.newBuilder().setSuccess(false).build();
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return false;
    }
    return Objects.equals(impl.getStatus().getLeader().get(), impl.getThisNode());
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return null;
    }
    return new Peer(groupId, impl.getStatus().getLeader().get());
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  public RaftMember getMember(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }
}
