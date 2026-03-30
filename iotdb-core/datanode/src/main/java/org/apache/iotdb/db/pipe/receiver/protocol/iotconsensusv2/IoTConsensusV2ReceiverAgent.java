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

package org.apache.iotdb.db.pipe.receiver.protocol.iotconsensusv2;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestVersion;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeReceiver;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.external.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IoTConsensusV2ReceiverAgent implements ConsensusPipeReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTConsensusV2ReceiverAgent.class);

  private static final Map<
          Byte,
          TriFunction<IoTConsensusV2, ConsensusGroupId, ConsensusPipeName, IoTConsensusV2Receiver>>
      RECEIVER_CONSTRUCTORS = new HashMap<>();

  private static final long WAIT_INITIALIZE_RECEIVER_INTERVAL_IN_MS = 100;

  private final int thisNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  /**
   * For each consensus Pipe task, there is an independent receiver. So for every replica, it has
   * (n-1) receivers, n is the num of replicas. 1 DataNode --has--> 1 IoTConsensusV2ReceiverAgent &
   * n replicas 1 IoTConsensusV2ReceiverAgent --manages--> n replicas' receivers 1 replica --has-->
   * (n-1) receivers
   */
  private final Map<
          ConsensusGroupId, Map<ConsensusPipeName, AtomicReference<IoTConsensusV2Receiver>>>
      replicaReceiverMap = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock receiverLifeCircleLock = new ReentrantReadWriteLock();
  private final Set<Integer> staleRegions = new CopyOnWriteArraySet<>();

  private IoTConsensusV2 iotConsensusV2;

  public IoTConsensusV2ReceiverAgent() {
    RECEIVER_CONSTRUCTORS.put(
        IoTConsensusV2RequestVersion.VERSION_1.getVersion(), IoTConsensusV2Receiver::new);
  }

  public void initConsensusInRuntime() {
    IConsensus consensus = DataRegionConsensusImpl.getInstance();
    // If DataRegion uses IoTConsensusV2
    if (consensus instanceof IoTConsensusV2) {
      this.iotConsensusV2 = (IoTConsensusV2) consensus;
    }
    // If DataRegion uses other consensus such as IoTConsensus
    else {
      this.iotConsensusV2 = null;
    }
  }

  public static TIoTConsensusV2TransferResp closedResp(String consensusInfo, TCommitId tCommitId) {
    final TSStatus status =
        new TSStatus(
            RpcUtils.getStatus(
                TSStatusCode.IOT_CONSENSUS_V2_CLOSE_ERROR,
                "IoTConsensusV2 receiver received a request after it was closed."));
    LOGGER.info(
        "IoTConsensusV2-{}: receive on-the-fly no.{} event after data region was deleted, discard it",
        consensusInfo,
        tCommitId);
    return new TIoTConsensusV2TransferResp(status);
  }

  @Override
  public TIoTConsensusV2TransferResp receive(TIoTConsensusV2TransferReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      final ConsensusGroupId consensusGroupId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      final IoTConsensusV2Receiver receiver =
          getReceiver(consensusGroupId, req.getDataNodeId(), reqVersion);

      if (receiver == null) {
        return closedResp(consensusGroupId.toString(), req.getCommitId());
      }
      return receiver.receive(req);
    } else {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.IOT_CONSENSUS_V2_VERSION_ERROR,
              String.format("Unknown IoTConsensusV2RequestVersion %s.", reqVersion));
      LOGGER.warn(
          "IoTConsensusV2: Unknown IoTConsensusV2RequestVersion, response status = {}.", status);
      return new TIoTConsensusV2TransferResp(status);
    }
  }

  private IoTConsensusV2Receiver getReceiver(
      ConsensusGroupId consensusGroupId, int leaderDataNodeId, byte reqVersion) {
    // Try to not block concurrent execution of receive() while ensuring sequential execution of
    // creating receiver and releasing receiver by using writeReadLock.
    receiverLifeCircleLock.readLock().lock();
    try {
      // If data region is stale, return null.
      if (staleRegions.contains(consensusGroupId.getId())) {
        return null;
      }
      // 1. Route to given consensusGroup's receiver map
      Map<ConsensusPipeName, AtomicReference<IoTConsensusV2Receiver>> consensusPipe2ReceiverMap =
          replicaReceiverMap.computeIfAbsent(consensusGroupId, key -> new ConcurrentHashMap<>());
      // 2. Route to given consensusPipeTask's receiver
      ConsensusPipeName consensusPipeName =
          new ConsensusPipeName(consensusGroupId, leaderDataNodeId, thisNodeId);
      AtomicBoolean isFirstGetReceiver = new AtomicBoolean(false);
      AtomicReference<IoTConsensusV2Receiver> receiverReference =
          consensusPipe2ReceiverMap.computeIfAbsent(
              consensusPipeName,
              key -> {
                isFirstGetReceiver.set(true);
                return new AtomicReference<>(null);
              });

      if (receiverReference.get() == null) {
        return internalSetAndGetReceiver(
            consensusGroupId, consensusPipeName, reqVersion, isFirstGetReceiver);
      }

      final byte receiverThreadLocalVersion = receiverReference.get().getVersion().getVersion();
      if (receiverThreadLocalVersion != reqVersion) {
        LOGGER.warn(
            "The iotConsensusV2 request version {} is different from the sender request version {},"
                + " the receiver will be reset to the sender request version.",
            receiverThreadLocalVersion,
            reqVersion);
        receiverReference.set(null);
        return internalSetAndGetReceiver(
            consensusGroupId, consensusPipeName, reqVersion, isFirstGetReceiver);
      }
      return receiverReference.get();
    } finally {
      receiverLifeCircleLock.readLock().unlock();
    }
  }

  private IoTConsensusV2Receiver internalSetAndGetReceiver(
      ConsensusGroupId consensusGroupId,
      ConsensusPipeName consensusPipeName,
      byte reqVersion,
      AtomicBoolean isFirstGetReceiver) {
    // 1. Route to given consensusGroup's receiver map
    Map<ConsensusPipeName, AtomicReference<IoTConsensusV2Receiver>> consensusPipe2ReciverMap =
        replicaReceiverMap.get(consensusGroupId);
    // 2. Route to given consensusPipeTask's receiver
    AtomicReference<IoTConsensusV2Receiver> receiverReference =
        consensusPipe2ReciverMap.get(consensusPipeName);

    if (isFirstGetReceiver.get()) {
      if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
        receiverReference.set(
            RECEIVER_CONSTRUCTORS
                .get(reqVersion)
                .apply(iotConsensusV2, consensusGroupId, consensusPipeName));
        LOGGER.info("Receiver-{} is ready", consensusPipeName);
      } else {
        throw new UnsupportedOperationException(
            String.format("Unsupported iotConsensusV2 request version %d", reqVersion));
      }
    } else {
      waitUntilReceiverGetInitiated(receiverReference);
    }
    return receiverReference.get();
  }

  private void waitUntilReceiverGetInitiated(
      AtomicReference<IoTConsensusV2Receiver> receiverReference) {
    try {
      while (receiverReference.get() == null) {
        Thread.sleep(WAIT_INITIALIZE_RECEIVER_INTERVAL_IN_MS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "IoTConsensusV2Receiver thread is interrupted when waiting for receiver get initiated, may because system exit.",
          e);
    }
  }

  /** Release all receivers of given data region when this region is deleted */
  @Override
  public final void releaseReceiverResource(DataRegionId dataRegionId) {
    receiverLifeCircleLock.writeLock().lock();
    try {
      // Mark this region as stale first, indicating that this region can not create new receivers
      // since it has been deleted.
      staleRegions.add(dataRegionId.getId());
      // 1. Route to given consensusGroup's receiver map
      Map<ConsensusPipeName, AtomicReference<IoTConsensusV2Receiver>> consensusPipe2ReciverMap =
          this.replicaReceiverMap.getOrDefault(
              ConsensusGroupId.Factory.create(
                  TConsensusGroupType.DataRegion.getValue(), dataRegionId.getId()),
              new ConcurrentHashMap<>());
      // 2. Release all related receivers
      consensusPipe2ReciverMap.entrySet().stream()
          .filter(entry -> entry.getKey().getReceiverDataNodeId() == thisNodeId)
          .forEach(
              receiverEntry -> {
                ConsensusPipeName consensusPipeName = receiverEntry.getKey();
                AtomicReference<IoTConsensusV2Receiver> receiverReference =
                    receiverEntry.getValue();
                if (receiverReference != null && receiverReference.get() != null) {
                  receiverReference.get().handleExit();
                  receiverReference.set(null);
                }
              });
      // 3. Release replica map
      this.replicaReceiverMap.remove(dataRegionId);
      // 4. GC receiver map
      consensusPipe2ReciverMap.clear();
      LOGGER.info("All Receivers related to {} are released.", dataRegionId);
    } finally {
      receiverLifeCircleLock.writeLock().unlock();
    }
  }

  public final void closeReceiverExecutor() {
    this.replicaReceiverMap.forEach(
        (consensusGroupId, receiverMap) -> {
          receiverMap.forEach(
              (consensusPipeName, receiverReference) -> {
                if (receiverReference != null) {
                  receiverReference.get().closeExecutor();
                  LOGGER.info("Receivers-{}' executor is closed.", consensusPipeName);
                }
              });
        });
  }
}
