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

package org.apache.iotdb.consensus.raft.service;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.PartitionedMeta;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.thrift.TAdminReq;
import org.apache.iotdb.consensus.raft.thrift.TAdminRes;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteReq;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteRes;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaReq;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaRes;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.concurrent.CompletableFuture;

/**
 * What the RPC processor needs from the node: hand a kernel message to a group and get its
 * response, plus the non-kernel RPCs (forwarded writes, admin, snapshots, status). Implemented by
 * the {@code RaftConsensus} facade.
 */
public interface RaftRpcDispatcher {

  /** The group's response, or null if the group does not exist on this node. */
  CompletableFuture<Message> handle(ConsensusGroupId gid, int from, Message msg);

  default org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq decodeAppend(
      ConsensusGroupId gid, org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq request)
      throws java.io.IOException {
    return org.apache.iotdb.consensus.raft.transport.ThriftCodec.decode(request, metaCodec(gid));
  }

  TGroupStatusRes groupStatus(ConsensusGroupId gid);

  default TGroupStatusRes readIndexStatus(ConsensusGroupId gid)
      throws org.apache.iotdb.consensus.exception.ConsensusException {
    throw new org.apache.iotdb.consensus.exception.ConsensusException("read barrier unsupported");
  }

  /**
   * The group's entry-metadata codec for RPC payloads ({@code TRaftLogEntry.profileMeta}). The
   * default ({@link PartitionedMeta#CODEC}) keeps the original behaviour.
   */
  default EntryMetaCodec metaCodec(ConsensusGroupId gid) {
    return PartitionedMeta.CODEC;
  }

  default TForwardWriteRes forwardWrite(TForwardWriteReq req) {
    return new TForwardWriteRes(RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
  }

  default TAdminRes admin(TAdminReq req) {
    return new TAdminRes(RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
  }

  default TInstallSnapshotMetaRes installSnapshotMeta(TInstallSnapshotMetaReq req) {
    return new TInstallSnapshotMetaRes(0, RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
  }

  default TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req) {
    return new TSendSnapshotFragmentRes(RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION), 0);
  }

  default TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req) {
    return new TTriggerSnapshotLoadRes(RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
  }

  default TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(
      TCleanupTransferredSnapshotReq req) {
    return new TCleanupTransferredSnapshotRes(
        RpcUtils.getStatus(TSStatusCode.UNSUPPORTED_OPERATION));
  }
}
