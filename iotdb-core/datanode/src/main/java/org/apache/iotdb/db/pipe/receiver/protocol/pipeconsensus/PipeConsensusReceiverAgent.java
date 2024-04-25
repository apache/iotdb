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
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.PipeConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class PipeConsensusReceiverAgent extends PipeConsensusServerImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusReceiverAgent.class);

  private final ThreadLocal<PipeConsensusReceiver> receiverThreadLocal = new ThreadLocal<>();

  private static final Map<Byte, Supplier<PipeConsensusReceiver>> RECEIVER_CONSTRUCTORS =
      new HashMap<>();

  public PipeConsensusReceiverAgent() {
    RECEIVER_CONSTRUCTORS.put(
        PipeConsensusRequestVersion.VERSION_1.getVersion(), PipeConsensusReceiver::new);
  }

  public TPipeConsensusTransferResp receive(
      TPipeConsensusTransferReq req) {
    final byte reqVersion = req.getVersion();
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      return getReceiver(reqVersion).receive(req);
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

  private PipeConsensusReceiver getReceiver(byte reqVersion) {
    if (receiverThreadLocal.get() == null) {
      return internalSetAndGetReceiver(reqVersion);
    }

    final byte receiverThreadLocalVersion = receiverThreadLocal.get().getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The pipeConsensus request version {} is different from the sender request version {},"
              + " the receiver will be reset to the sender request version.",
          receiverThreadLocalVersion,
          reqVersion);
      receiverThreadLocal.remove();
      return internalSetAndGetReceiver(reqVersion);
    }

    return receiverThreadLocal.get();
  }

  private PipeConsensusReceiver internalSetAndGetReceiver(byte reqVersion) {
    if (RECEIVER_CONSTRUCTORS.containsKey(reqVersion)) {
      receiverThreadLocal.set(RECEIVER_CONSTRUCTORS.get(reqVersion).get());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipeConsensus request version %d", reqVersion));
    }
    return receiverThreadLocal.get();
  }
}
