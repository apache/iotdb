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

package org.apache.iotdb.db.pipe.receiver.protocol.pipeconsensus;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeReceiver;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class PipeConsensusReceiverAgent implements ConsensusPipeReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiverAgent.class);

  private static final Map<Byte, BiFunction<PipeConsensus, ConsensusGroupId, PipeConsensusReceiver>>
      RECEIVER_CONSTRUCTORS = new HashMap<>();

  private final Map<ConsensusGroupId, AtomicReference<PipeConsensusReceiver>> replicaReceiverMap =
      new ConcurrentHashMap<>();

  private final PipeConsensus pipeConsensus;

  public PipeConsensusReceiverAgent() {
    IConsensus consensus = DataRegionConsensusImpl.getInstance();
    // If DataRegion uses PipeConsensus
    if (consensus instanceof PipeConsensus) {
      this.pipeConsensus = (PipeConsensus) consensus;
    }
    // If DataRegion uses other consensus such as IoTConsensus
    else {
      this.pipeConsensus = null;
    }

    RECEIVER_CONSTRUCTORS.put(
        PipeConsensusRequestVersion.VERSION_1.getVersion(), PipeConsensusReceiver::new);
  }

  @Override
  public TPipeConsensusTransferResp receive(TPipeConsensusTransferReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      final ConsensusGroupId consensusGroupId =
          ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getConsensusGroupId());
      return getReceiver(consensusGroupId, reqVersion).receive(req);
    } else {
      final TSStatus status =
          RpcUtils.getStatus(
              TSStatusCode.PIPE_CONSENSUS_VERSION_ERROR,
              String.format("Unknown PipeConsensusRequestVersion %s.", reqVersion));
      LOGGER.warn(
          "PipeConsensus: Unknown PipeConsensusRequestVersion, response status = {}.", status);
      return new TPipeConsensusTransferResp(status);
    }
  }

  private PipeConsensusReceiver getReceiver(ConsensusGroupId consensusGroupId, byte reqVersion) {
    AtomicReference<PipeConsensusReceiver> receiverReference =
        replicaReceiverMap.computeIfAbsent(consensusGroupId, key -> new AtomicReference<>(null));

    if (receiverReference.get() == null) {
      return internalSetAndGetReceiver(consensusGroupId, reqVersion);
    }

    final byte receiverThreadLocalVersion = receiverReference.get().getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The pipeConsensus request version {} is different from the sender request version {},"
              + " the receiver will be reset to the sender request version.",
          receiverThreadLocalVersion,
          reqVersion);
      receiverReference.set(null);
      return internalSetAndGetReceiver(consensusGroupId, reqVersion);
    }

    return receiverReference.get();
  }

  private PipeConsensusReceiver internalSetAndGetReceiver(
      ConsensusGroupId consensusGroupId, byte reqVersion) {
    AtomicReference<PipeConsensusReceiver> receiverReference =
        replicaReceiverMap.get(consensusGroupId);

    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      receiverReference.set(
          RECEIVER_CONSTRUCTORS.get(reqVersion).apply(pipeConsensus, consensusGroupId));
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipeConsensus request version %d", reqVersion));
    }
    return receiverReference.get();
  }

  @TestOnly
  public void resetReceiver() {
    // changed to reset given receiver
  }

  @TestOnly
  public long getSyncCommitIndex() {
    return 0;
    //    if (receiverReference.get() == null) {
    //      return internalSetAndGetReceiver(PipeConsensusRequestVersion.VERSION_1.getVersion())
    //          .getOnSyncedCommitIndex();
    //    }
    //    return receiverReference.get().getOnSyncedCommitIndex();
  }

  @TestOnly
  public int getRebootTimes() {
    return 0;
    //    if (receiverReference.get() == null) {
    //      return internalSetAndGetReceiver(PipeConsensusRequestVersion.VERSION_1.getVersion())
    //          .getConnectorRebootTimes();
    //    }
    //    return receiverReference.get().getConnectorRebootTimes();
  }
}
