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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.TRaftConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;

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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TRaftConsensus implements IConsensus {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftConsensus.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, TRaftServerImpl> stateMachineMap = new ConcurrentHashMap<>();

  private TRaftConfig config;
  private Map<ConsensusGroupId, List<Peer>> correctPeerListBeforeStart = null;

  public TRaftConsensus(ConsensusConfig config, IStateMachine.Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.registry = registry;
    this.config = config.getTRaftConfig();
  }

  @Override
  public synchronized void start() throws IOException {
    initAndRecover();
    stateMachineMap.values().forEach(TRaftServerImpl::start);
    TRaftNodeRegistry.register(thisNode, this);
  }

  @Override
  public synchronized void stop() {
    TRaftNodeRegistry.unregister(thisNode);
    stateMachineMap.values().forEach(TRaftServerImpl::stop);
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.isReadOnly()) {
      throw new ConsensusException("Current peer is read-only");
    }
    return impl.write(request);
  }

  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    return impl.read(request);
  }

  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers) throws ConsensusException {
    List<Peer> effectivePeers = peers;
    if (effectivePeers == null || effectivePeers.isEmpty()) {
      effectivePeers =
          Collections.singletonList(new Peer(groupId, thisNodeId, thisNode));
    }
    if (!effectivePeers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, effectivePeers);
    }
    if (effectivePeers.size() < 1) {
      throw new IllegalPeerNumException(effectivePeers.size());
    }
    final List<Peer> finalPeers = effectivePeers;
    AtomicBoolean alreadyExists = new AtomicBoolean(true);
    Optional.ofNullable(
            stateMachineMap.computeIfAbsent(
                groupId,
                key -> {
                  alreadyExists.set(false);
                  File peerDir = new File(buildPeerDir(groupId));
                  if (!peerDir.exists() && !peerDir.mkdirs()) {
                    LOGGER.warn("Failed to create TRaft peer dir {}", peerDir);
                    return null;
                  }
                  try {
                    return new TRaftServerImpl(
                        peerDir.getAbsolutePath(),
                        new Peer(groupId, thisNodeId, thisNode),
                        new TreeSet<>(finalPeers),
                        registry.apply(groupId),
                        config);
                  } catch (IOException e) {
                    LOGGER.error("Failed to create TRaft server for {}", groupId, e);
                    return null;
                  }
                }))
        .map(
            impl -> {
              impl.start();
              return impl;
            })
        .orElseThrow(
            () -> new ConsensusException(String.format("Failed to create local peer %s", groupId)));
    if (alreadyExists.get()) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    AtomicBoolean exist = new AtomicBoolean(false);
    stateMachineMap.computeIfPresent(
        groupId,
        (key, value) -> {
          exist.set(true);
          value.stop();
          FileUtils.deleteFileOrDirectory(new File(buildPeerDir(groupId)));
          return null;
        });
    if (!exist.get()) {
      throw new ConsensusGroupNotExistException(groupId);
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.getConfiguration().contains(peer)) {
      throw new PeerAlreadyInConsensusGroupException(groupId, peer);
    }
    try {
      impl.addPeer(peer);
    } catch (IOException e) {
      throw new ConsensusException("Failed to add peer in TRaft", e);
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (!impl.getConfiguration().contains(peer)) {
      throw new PeerNotInConsensusGroupException(groupId, peer.toString());
    }
    try {
      impl.removePeer(peer);
    } catch (IOException e) {
      throw new ConsensusException("Failed to remove peer in TRaft", e);
    }
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(Map<ConsensusGroupId, List<Peer>> correctPeerList) {
    this.correctPeerListBeforeStart = correctPeerList;
  }

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    try {
      impl.resetPeerList(correctPeers);
    } catch (IOException e) {
      throw new ConsensusException("Failed to reset peer list in TRaft", e);
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    TRaftServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    impl.transferLeader(newLeader);
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    throw new ConsensusException("TRaft does not support snapshot trigger currently");
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId)).map(TRaftServerImpl::isLeader).orElse(false);
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .map(TRaftServerImpl::getLogicalClock)
        .orElse(0L);
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .map(TRaftServerImpl::isLeaderReady)
        .orElse(false);
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId)).map(TRaftServerImpl::getLeader).orElse(null);
  }

  @Override
  public int getReplicationNum(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .map(TRaftServerImpl::getConfiguration)
        .map(List::size)
        .orElse(0);
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return buildPeerDir(groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    this.config = consensusConfig.getTRaftConfig();
    stateMachineMap.values().forEach(server -> server.reloadConsensusConfig(config));
  }

  TRaftServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists() && !storageDir.mkdirs()) {
      throw new IOException(String.format("Unable to create consensus dir at %s", storageDir));
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
      for (Path path : stream) {
        String[] items = path.getFileName().toString().split("_");
        if (items.length != 2) {
          continue;
        }
        ConsensusGroupId consensusGroupId =
            ConsensusGroupId.Factory.create(
                Integer.parseInt(items[0]), Integer.parseInt(items[1]));
        TRaftServerImpl consensus =
            new TRaftServerImpl(
                path.toString(),
                new Peer(consensusGroupId, thisNodeId, thisNode),
                new TreeSet<>(),
                registry.apply(consensusGroupId),
                config);
        stateMachineMap.put(consensusGroupId, consensus);
      }
    }
    if (correctPeerListBeforeStart != null) {
      for (Map.Entry<ConsensusGroupId, List<Peer>> entry : correctPeerListBeforeStart.entrySet()) {
        TRaftServerImpl impl = stateMachineMap.get(entry.getKey());
        if (impl == null) {
          continue;
        }
        impl.resetPeerList(entry.getValue());
      }
    }
  }

  private String buildPeerDir(ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }
}
