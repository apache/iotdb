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

package org.apache.iotdb.consensus.standalone;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.consensus.statemachine.IStateMachine.Registry;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple consensus implementation, which can be used when replicaNum is 1.
 *
 * <p>Notice: The stateMachine needs to implement WAL itself to ensure recovery after a restart
 *
 * <p>any module can use `IConsensus consensusImpl = new StandAloneConsensus(id -> new
 * EmptyStateMachine());` to perform an initialization implementation.
 */
class StandAloneConsensus implements IConsensus {

  private final TEndPoint thisNode;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, StandAloneServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();

  public StandAloneConsensus(TEndPoint thisNode, File storageDir, Registry registry) {
    this.thisNode = thisNode;
    this.storageDir = storageDir;
    this.registry = registry;
  }

  @Override
  public void start() throws IOException {
    if (!this.storageDir.exists()) {
      storageDir.mkdirs();
    } else {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
        for (Path path : stream) {
          String filename = path.getFileName().toString();
          String[] items = filename.split("_");
          TConsensusGroupType type = TConsensusGroupType.valueOf(items[0]);
          ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.createEmpty(type);
          consensusGroupId.setId(Integer.parseInt(items[1]));
          TEndPoint endPoint = new TEndPoint(items[2], Integer.parseInt(items[3]));
          stateMachineMap.put(
              consensusGroupId,
              new StandAloneServerImpl(
                  new Peer(consensusGroupId, endPoint), registry.apply(consensusGroupId)));
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {}

  @Override
  public ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest request) {
    AtomicReference<TSStatus> result = new AtomicReference<>();
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          // TODO make Statemachine thread-safe to avoid thread-safe ways like this that may affect
          // performance
          result.set(v.write(request));
          return v;
        });
    if (result.get() == null) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return ConsensusWriteResponse.newBuilder().setStatus(result.get()).build();
  }

  @Override
  public ConsensusReadResponse read(ConsensusGroupId groupId, IConsensusRequest request) {
    AtomicReference<DataSet> result = new AtomicReference<>();
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          // TODO make Statemachine thread-safe to avoid thread-safe ways like this that may affect
          // performance
          result.set(v.read(request));
          return v;
        });
    if (result.get() == null) {
      return ConsensusReadResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return ConsensusReadResponse.newBuilder().setDataSet(result.get()).build();
  }

  @Override
  public ConsensusGenericResponse addConsensusGroup(ConsensusGroupId groupId, List<Peer> peers) {
    int consensusGroupSize = peers.size();
    if (consensusGroupSize != 1) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerNumException(consensusGroupSize))
          .build();
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    stateMachineMap.computeIfAbsent(
        groupId,
        (k) -> {
          exist.set(false);
          StandAloneServerImpl impl =
              new StandAloneServerImpl(peers.get(0), registry.apply(groupId));
          impl.start();
          String groupPath =
              storageDir
                  + File.separator
                  + groupId.getType()
                  + "_"
                  + groupId.getId()
                  + "_"
                  + peers.get(0).getEndpoint().ip
                  + "_"
                  + peers.get(0).getEndpoint().port;
          File file = new File(groupPath);
          file.mkdirs();

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
  public ConsensusGenericResponse removeConsensusGroup(ConsensusGroupId groupId) {
    AtomicBoolean exist = new AtomicBoolean(false);
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          String groupPath =
              storageDir
                  + File.separator
                  + groupId.getType()
                  + "_"
                  + groupId.getId()
                  + "_"
                  + thisNode.ip
                  + "_"
                  + thisNode.port;
          File file = new File(groupPath);
          file.delete();
          exist.set(true);
          v.stop();
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
    return true;
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    if (!stateMachineMap.containsKey(groupId)) {
      return null;
    }
    return new Peer(groupId, thisNode);
  }
}
