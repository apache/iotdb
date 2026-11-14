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
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.thrift.RaftConsensusIService;
import org.apache.iotdb.consensus.raft.thrift.TAdminReq;
import org.apache.iotdb.consensus.raft.thrift.TAdminRes;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesRes;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.raft.thrift.TCleanupTransferredSnapshotRes;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteReq;
import org.apache.iotdb.consensus.raft.thrift.TForwardWriteRes;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusReq;
import org.apache.iotdb.consensus.raft.thrift.TGroupStatusRes;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaReq;
import org.apache.iotdb.consensus.raft.thrift.TInstallSnapshotMetaRes;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermReq;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermRes;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteReq;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteRes;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.raft.thrift.TSendSnapshotFragmentRes;
import org.apache.iotdb.consensus.raft.thrift.TTimeoutNowReq;
import org.apache.iotdb.consensus.raft.thrift.TTimeoutNowRes;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.raft.thrift.TTriggerSnapshotLoadRes;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncReq;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncRes;
import org.apache.iotdb.consensus.raft.transport.ThriftCodec;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thrift {@code Iface}: decodes, hands the message to the group, waits for the kernel's Reply (the
 * worker blocks at most {@code rpcTimeoutMs}) and encodes it back.
 */
public final class RaftRPCServiceProcessor implements RaftConsensusIService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftRPCServiceProcessor.class);

  private final RaftRpcDispatcher dispatcher;
  private final long rpcTimeoutMs;

  public RaftRPCServiceProcessor(RaftRpcDispatcher dispatcher, long rpcTimeoutMs) {
    this.dispatcher = dispatcher;
    this.rpcTimeoutMs = rpcTimeoutMs;
  }

  private static ConsensusGroupId gid(org.apache.iotdb.common.rpc.thrift.TConsensusGroupId tgid) {
    return ConsensusGroupId.Factory.createFromTConsensusGroupId(tgid);
  }

  private Message dispatch(
      org.apache.iotdb.common.rpc.thrift.TConsensusGroupId tgid, int from, Message msg)
      throws TException {
    return dispatch(gid(tgid), from, msg);
  }

  private Message dispatch(ConsensusGroupId gid, int from, Message msg) throws TException {
    CompletableFuture<Message> f = dispatcher.handle(gid, from, msg);
    if (f == null) {
      throw new TException("unknown consensus group " + gid);
    }
    try {
      return f.get(rpcTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TException(e);
    } catch (Exception e) {
      throw new TException("no reply from " + gid + " within " + rpcTimeoutMs + " ms: " + e);
    }
  }

  @Override
  public TAppendEntriesRes appendEntries(TAppendEntriesReq req) throws TException {
    ConsensusGroupId gid = gid(req.getGroupId());
    Message res;
    try {
      res = dispatch(gid, req.getLeaderId(), dispatcher.decodeAppend(gid, req));
    } catch (java.io.IOException | IllegalArgumentException error) {
      throw new TException("Invalid replication file or append request", error);
    }
    if (!(res instanceof AppendEntriesRes)) {
      throw new TException("unexpected reply " + res);
    }
    return ThriftCodec.encode((AppendEntriesRes) res);
  }

  @Override
  public TRequestVoteRes requestVote(TRequestVoteReq req) throws TException {
    Message res = dispatch(req.getGroupId(), req.getCandidateId(), ThriftCodec.decode(req));
    if (!(res instanceof RequestVoteRes)) {
      throw new TException("unexpected reply " + res);
    }
    return ThriftCodec.encode((RequestVoteRes) res);
  }

  @Override
  public TTimeoutNowRes timeoutNow(TTimeoutNowReq req) {
    ConsensusGroupId gid = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId());
    CompletableFuture<Message> f =
        dispatcher.handle(gid, req.getLeaderId(), ThriftCodec.decode(req));
    return new TTimeoutNowRes(
        RpcUtils.getStatus(
            f == null ? TSStatusCode.CONSENSUS_GROUP_NOT_EXIST : TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TForwardWriteRes forwardWrite(TForwardWriteReq req) {
    return dispatcher.forwardWrite(req);
  }

  @Override
  public TAdminRes admin(TAdminReq req) {
    return dispatcher.admin(req);
  }

  @Override
  public TInstallSnapshotMetaRes installSnapshotMeta(TInstallSnapshotMetaReq req) {
    return dispatcher.installSnapshotMeta(req);
  }

  @Override
  public TSendSnapshotFragmentRes sendSnapshotFragment(TSendSnapshotFragmentReq req) {
    return dispatcher.sendSnapshotFragment(req);
  }

  @Override
  public TTriggerSnapshotLoadRes triggerSnapshotLoad(TTriggerSnapshotLoadReq req) {
    return dispatcher.triggerSnapshotLoad(req);
  }

  @Override
  public TCleanupTransferredSnapshotRes cleanupTransferredSnapshot(
      TCleanupTransferredSnapshotReq req) {
    return dispatcher.cleanupTransferredSnapshot(req);
  }

  @Override
  public TRecoverTermRes recoverTerm(TRecoverTermReq req) throws TException {
    ConsensusGroupId gid = gid(req.getGroupId());
    Message res = dispatch(gid, req.getLeaderId(), ThriftCodec.decode(req));
    if (!(res instanceof RecoverTermRes)) {
      throw new TException("unexpected reply " + res);
    }
    return ThriftCodec.encode((RecoverTermRes) res, dispatcher.metaCodec(gid));
  }

  @Override
  public TUpdateSyncRes updateSync(TUpdateSyncReq req) throws TException {
    Message res = dispatch(req.getGroupId(), req.getLeaderId(), ThriftCodec.decode(req));
    if (!(res instanceof UpdateSyncRes)) {
      throw new TException("unexpected reply " + res);
    }
    return ThriftCodec.encode((UpdateSyncRes) res);
  }

  @Override
  public TGroupStatusRes getGroupStatus(TGroupStatusReq req) throws TException {
    TGroupStatusRes res;
    try {
      ConsensusGroupId gid = ConsensusGroupId.Factory.createFromTConsensusGroupId(req.getGroupId());
      res = req.isReadBarrier() ? dispatcher.readIndexStatus(gid) : dispatcher.groupStatus(gid);
    } catch (org.apache.iotdb.consensus.exception.ConsensusException e) {
      throw new TException(e);
    }
    if (res == null) {
      throw new TException("unknown consensus group " + req.getGroupId());
    }
    return res;
  }

  public void handleClientExit() {
    LOGGER.debug("raft rpc client exited");
  }
}
