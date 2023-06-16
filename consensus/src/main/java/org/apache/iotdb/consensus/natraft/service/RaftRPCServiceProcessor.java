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

package org.apache.iotdb.consensus.natraft.service;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId.Factory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.natraft.RaftConsensus;
import org.apache.iotdb.consensus.natraft.exception.UnknownLogTypeException;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.LogParser;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.ConfigChangeEntry;
import org.apache.iotdb.consensus.natraft.utils.IOUtils;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedSingleEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;
import org.apache.iotdb.consensus.raft.thrift.ElectionRequest;
import org.apache.iotdb.consensus.raft.thrift.ExecuteReq;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatRequest;
import org.apache.iotdb.consensus.raft.thrift.HeartBeatResponse;
import org.apache.iotdb.consensus.raft.thrift.NoMemberException;
import org.apache.iotdb.consensus.raft.thrift.RaftService;
import org.apache.iotdb.consensus.raft.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.consensus.raft.thrift.SendSnapshotRequest;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class RaftRPCServiceProcessor implements RaftService.AsyncIface {

  private final Logger logger = LoggerFactory.getLogger(RaftRPCServiceProcessor.class);

  private final RaftConsensus consensus;

  public RaftRPCServiceProcessor(RaftConsensus consensus) {
    this.consensus = consensus;
  }

  public void handleClientExit() {}

  private RaftMember getMember(TConsensusGroupId groupId) throws TException {
    RaftMember member = consensus.getMember(Factory.createFromTConsensusGroupId(groupId));
    if (member == null) {
      throw new NoMemberException("No such member of: " + groupId);
    }
    return member;
  }

  private ConfigChangeEntry findFirstConfigChangeEntry(AppendEntriesRequest request)
      throws UnknownLogTypeException {
    ConfigChangeEntry configChangeEntry = null;
    for (ByteBuffer entryBuffer : request.entries) {
      Entry entry = LogParser.getINSTANCE().parse(entryBuffer, null);
      if (entry instanceof ConfigChangeEntry) {
        configChangeEntry = (ConfigChangeEntry) entry;
        break;
      }
    }
    return configChangeEntry;
  }

  /**
   * Get the associated member or create it using the last config change entry in the request (if
   * any).
   */
  private RaftMember getMemberOrCreate(TConsensusGroupId tgroupId, AppendEntriesRequest request)
      throws TException {
    ConsensusGroupId groupId = Factory.createFromTConsensusGroupId(tgroupId);
    RaftMember member = consensus.getMember(groupId);
    if (member == null) {
      try {
        ConfigChangeEntry lastConfigChangeEntry = findFirstConfigChangeEntry(request);
        if (lastConfigChangeEntry != null) {
          Peer thisPeer = new Peer(groupId, consensus.getThisNodeId(), consensus.getThisNode());
          consensus.createNewMemberIfAbsent(
              groupId,
              thisPeer,
              lastConfigChangeEntry.getOldPeers(),
              lastConfigChangeEntry.getNewPeers());
          return consensus.getMember(groupId);
        }
      } catch (UnknownLogTypeException e) {
        throw new TException(e.getMessage());
      }
      throw new NoMemberException("No such member of: " + tgroupId);
    }
    return member;
  }

  @Override
  public void sendHeartbeat(
      HeartBeatRequest request, AsyncMethodCallback<HeartBeatResponse> resultHandler)
      throws TException {
    RaftMember member = getMember(request.groupId);
    resultHandler.onComplete(member.processHeartbeatRequest(request));
  }

  @Override
  public void startElection(ElectionRequest request, AsyncMethodCallback<Long> resultHandler)
      throws TException {
    RaftMember member = getMember(request.groupId);
    logger.info(
        "Member for election request: {}, request groupId {}",
        member.getRaftGroupId(),
        request.getGroupId());
    resultHandler.onComplete(member.processElectionRequest(request));
  }

  @Override
  public void appendEntries(
      AppendEntriesRequest request, AsyncMethodCallback<AppendEntryResult> resultHandler)
      throws TException {
    long startTime = Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.getOperationStartTime();
    RaftMember member = getMemberOrCreate(request.groupId, request);
    try {
      resultHandler.onComplete(member.appendEntries(request));
    } catch (UnknownLogTypeException e) {
      throw new TException(e);
    }
    Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.calOperationCostTimeFromStart(startTime);
  }

  @Override
  public void appendCompressedEntries(
      AppendCompressedEntriesRequest request, AsyncMethodCallback<AppendEntryResult> resultHandler)
      throws TException {
    long startTime = Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.getOperationStartTime();
    AppendEntriesRequest decompressedRequest = new AppendEntriesRequest();
    decompressedRequest
        .setTerm(request.getTerm())
        .setLeader(request.leader)
        .setLeaderCommit(request.leaderCommit)
        .setGroupId(request.groupId)
        .setLeaderId(request.leaderId);

    try {
      long compressionStartTime = Statistic.RAFT_RECEIVER_DECOMPRESS_ENTRY.getOperationStartTime();
      List<ByteBuffer> buffers =
          LogUtils.decompressEntries(
              request.entryBytes,
              IUnCompressor.getUnCompressor(CompressionType.values()[request.compressionType]),
              request.uncompressedSize);
      decompressedRequest.setEntries(buffers);
      Statistic.RAFT_RECEIVER_DECOMPRESS_ENTRY.calOperationCostTimeFromStart(compressionStartTime);

      RaftMember member = getMemberOrCreate(request.groupId, decompressedRequest);
      resultHandler.onComplete(member.appendEntries(decompressedRequest));
    } catch (UnknownLogTypeException | IOException e) {
      throw new TException(e);
    }
    Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.calOperationCostTimeFromStart(startTime);
  }

  @Override
  public void appendCompressedSingleEntries(
      AppendCompressedSingleEntriesRequest request,
      AsyncMethodCallback<AppendEntryResult> resultHandler)
      throws TException {
    long startTime = Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.getOperationStartTime();
    AppendEntriesRequest decompressedRequest = new AppendEntriesRequest();
    decompressedRequest
        .setTerm(request.getTerm())
        .setLeader(request.leader)
        .setLeaderCommit(request.leaderCommit)
        .setGroupId(request.groupId)
        .setLeaderId(request.leaderId);

    try {
      long compressionStartTime = Statistic.RAFT_RECEIVER_DECOMPRESS_ENTRY.getOperationStartTime();
      List<ByteBuffer> buffers =
          LogUtils.decompressEntries(
              request.entries, request.compressionTypes, request.uncompressedSizes);
      decompressedRequest.setEntries(buffers);
      Statistic.RAFT_RECEIVER_DECOMPRESS_ENTRY.calOperationCostTimeFromStart(compressionStartTime);

      RaftMember member = getMemberOrCreate(request.groupId, decompressedRequest);
      resultHandler.onComplete(member.appendEntries(decompressedRequest));
    } catch (UnknownLogTypeException | IOException e) {
      throw new TException(e);
    }
    Statistic.RAFT_RECEIVER_APPEND_ENTRY_FULL.calOperationCostTimeFromStart(startTime);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback<TSStatus> resultHandler)
      throws TException {
    RaftMember member = getMember(request.groupId);
    resultHandler.onComplete(member.installSnapshot(request.snapshotBytes, request.source));
  }

  @Override
  public void matchTerm(
      long index, long term, TConsensusGroupId groupId, AsyncMethodCallback<Boolean> resultHandler)
      throws TException {
    RaftMember member = getMember(groupId);
    resultHandler.onComplete(member.matchLog(index, term));
  }

  @Override
  public void executeRequest(ExecuteReq request, AsyncMethodCallback<TSStatus> resultHandler)
      throws TException {
    RaftMember member = getMember(request.groupId);
    resultHandler.onComplete(
        member
            .executeForwardedRequest(new ByteBufferConsensusRequest(request.requestBytes))
            .getStatus());
  }

  @Override
  public void ping(AsyncMethodCallback<Void> resultHandler) {
    resultHandler.onComplete(null);
  }

  @Override
  public void requestCommitIndex(
      TConsensusGroupId groupId, AsyncMethodCallback<RequestCommitIndexResponse> resultHandler)
      throws TException {
    RaftMember member = getMember(groupId);
    resultHandler.onComplete(member.requestCommitIndex());
  }

  @Override
  public void forceElection(TConsensusGroupId groupId, AsyncMethodCallback<TSStatus> resultHandler)
      throws TException {
    RaftMember member = getMember(groupId);
    resultHandler.onComplete(member.forceElection());
  }

  @Override
  public void readFile(
      String filePath, long offset, int length, AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(IOUtils.readFile(filePath, offset, length));
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }
}
