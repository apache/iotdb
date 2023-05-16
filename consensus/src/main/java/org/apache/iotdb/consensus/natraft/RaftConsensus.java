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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
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
import org.apache.iotdb.consensus.natraft.client.SyncClientAdaptor;
import org.apache.iotdb.consensus.natraft.exception.CheckConsistencyException;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.dispatch.flowcontrol.FlowMonitorManager;
import org.apache.iotdb.consensus.natraft.service.RaftRPCService;
import org.apache.iotdb.consensus.natraft.service.RaftRPCServiceProcessor;
import org.apache.iotdb.consensus.natraft.utils.NodeReport;
import org.apache.iotdb.consensus.natraft.utils.NodeReport.RaftMemberReport;
import org.apache.iotdb.consensus.natraft.utils.StatusUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RaftConsensus implements IConsensus {

  private static final Logger logger = LoggerFactory.getLogger(RaftConsensus.class);
  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, RaftMember> stateMachineMap = new ConcurrentHashMap<>();
  private final RaftRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private final RaftConfig config;
  private final IClientManager<TEndPoint, AsyncRaftServiceClient> clientManager;
  private ScheduledExecutorService reportThread;

  public RaftConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = new RaftConfig(config);
    this.registry = registry;
    this.service = new RaftRPCService(thisNode, this.config);
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncRaftServiceClient>()
            .createClientManager(new AsyncRaftServiceClientPoolFactory(this.config));
    FlowMonitorManager.INSTANCE.setConfig(this.config);
    SyncClientAdaptor.setConfig(this.config);
    Entry.DEFAULT_SERIALIZATION_BUFFER_SIZE = this.config.getEntryDefaultSerializationBufferSize();
  }

  @Override
  public void start() throws IOException {
    initAndRecover();
    service.initAsyncedServiceImpl(new RaftRPCServiceProcessor(this));
    try {
      registerManager.register(service);
    } catch (StartupException e) {
      throw new IOException(e);
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info(Timer.Statistic.getReport());
                  try {
                    stop();
                  } catch (IOException e) {
                    logger.error("Error during exiting", e);
                  }
                }));
    reportThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("NodeReportThread");
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        reportThread, this::generateNodeReport, 5, 5, TimeUnit.SECONDS);
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      if (!storageDir.mkdirs()) {
        logger.warn("Unable to create consensus dir at {}", storageDir);
      }
    } else {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
        for (Path path : stream) {
          logger.info("Recovering a RaftMember from {}", path);
          Path fileName = path.getFileName();
          String[] items = fileName.toString().split("_");
          if (items.length != 2) {
            continue;
          }
          ConsensusGroupId consensusGroupId =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
          RaftMember raftMember =
              new RaftMember(
                  path.toString(),
                  config,
                  new Peer(consensusGroupId, thisNodeId, thisNode),
                  new ArrayList<>(),
                  null,
                  consensusGroupId,
                  registry.apply(consensusGroupId),
                  clientManager,
                  this::onMemberRemoved);
          stateMachineMap.put(consensusGroupId, raftMember);
          raftMember.start();
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {
    reportThread.shutdownNow();
    stateMachineMap.values().parallelStream().forEach(RaftMember::stop);
    FlowMonitorManager.INSTANCE.close();
    clientManager.close();
    registerManager.deregisterAll();
  }

  @Override
  public ConsensusWriteResponse write(ConsensusGroupId groupId, IConsensusRequest request) {
    if (config.isOnlyTestNetwork()) {
      request.serializeToByteBuffer();
      return ConsensusWriteResponse.newBuilder().setStatus(StatusUtils.OK).build();
    }
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusWriteResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }

    TSStatus status;
    if (impl.isReadOnly()) {
      status = new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
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

  public void onMemberRemoved(ConsensusGroupId groupId) {
    stateMachineMap.remove(groupId);
  }

  public boolean createNewMemberIfAbsent(
      ConsensusGroupId groupId, Peer thisPeer, List<Peer> peers, List<Peer> newPeers) {
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
                  path,
                  config,
                  thisPeer,
                  peers,
                  newPeers,
                  groupId,
                  registry.apply(groupId),
                  clientManager,
                  this::onMemberRemoved);
          impl.start();
          return impl;
        });
    return !exist.get();
  }

  @Override
  public ConsensusGenericResponse createPeer(ConsensusGroupId groupId, List<Peer> peers) {
    int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerNumException(consensusGroupSize))
          .build();
    }
    Peer thisPeer = new Peer(groupId, thisNodeId, thisNode);
    if (!peers.contains(thisPeer)) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new IllegalPeerEndpointException(thisNode, peers))
          .build();
    }

    if (!createNewMemberIfAbsent(groupId, thisPeer, peers, null)) {
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
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return StatusUtils.toGenericResponse(impl.addPeer(peer));
  }

  @Override
  public ConsensusGenericResponse removePeer(ConsensusGroupId groupId, Peer peer) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return StatusUtils.toGenericResponse(impl.removePeer(peer));
  }

  @Override
  public ConsensusGenericResponse updatePeer(ConsensusGroupId groupId, Peer oldPeer, Peer newPeer) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return StatusUtils.toGenericResponse(impl.updatePeer(oldPeer, newPeer));
  }

  @Override
  public ConsensusGenericResponse changePeer(ConsensusGroupId groupId, List<Peer> newPeers) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return StatusUtils.toGenericResponse(impl.changeConfig(newPeers));
  }

  @Override
  public ConsensusGenericResponse transferLeader(ConsensusGroupId groupId, Peer newLeader) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    return StatusUtils.toGenericResponse(impl.transferLeader(newLeader));
  }

  @Override
  public ConsensusGenericResponse triggerSnapshot(ConsensusGroupId groupId) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return ConsensusGenericResponse.newBuilder()
          .setException(new ConsensusGroupNotExistException(groupId))
          .build();
    }
    impl.triggerSnapshot();
    return ConsensusGenericResponse.newBuilder().setSuccess(true).build();
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return false;
    }
    return Objects.equals(impl.getStatus().getLeader(), impl.getThisNode());
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    RaftMember impl = stateMachineMap.get(groupId);
    if (impl == null) {
      return null;
    }
    return impl.getStatus().getLeader();
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  public RaftMember getMember(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  public int getThisNodeId() {
    return thisNodeId;
  }

  public TEndPoint getThisNode() {
    return thisNode;
  }

  private void generateNodeReport() {
    if (logger.isInfoEnabled()) {
      try {
        NodeReport report = new NodeReport(thisNode);
        List<RaftMemberReport> reports = new ArrayList<>();
        for (RaftMember value : stateMachineMap.values()) {
          RaftMemberReport raftMemberReport = value.genMemberReport();
          if (raftMemberReport.getPrevLastLogIndex() != raftMemberReport.getLastLogIndex()) {
            reports.add(raftMemberReport);
          }
        }
        if (!reports.isEmpty()) {
          report.setMemberReports(reports);
          logger.info(report.toString());
        }
      } catch (Exception e) {
        logger.error("exception occurred when generating node report", e);
      }
    }
  }
}
