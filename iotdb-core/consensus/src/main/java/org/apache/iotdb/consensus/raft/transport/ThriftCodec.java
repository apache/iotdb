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

package org.apache.iotdb.consensus.raft.transport;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.PartitionedMeta;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.AppendExt;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermReq;
import org.apache.iotdb.consensus.raft.core.msg.RecoverTermRes;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.core.msg.TimeoutNowReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncReq;
import org.apache.iotdb.consensus.raft.core.msg.UpdateSyncRes;
import org.apache.iotdb.consensus.raft.thrift.TAckStatus;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesRes;
import org.apache.iotdb.consensus.raft.thrift.TEntryAck;
import org.apache.iotdb.consensus.raft.thrift.TEntryType;
import org.apache.iotdb.consensus.raft.thrift.TRaftLogEntry;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermReq;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermRes;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteReq;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteRes;
import org.apache.iotdb.consensus.raft.thrift.TTimeoutNowReq;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncReq;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncRes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Kernel POJO messages <-> Thrift structs. Protocol-specific fields travel as optional fields and
 * are left unset when the kernel value is unset, so Raft / TRaft messages keep their original wire
 * format.
 *
 * <p>Entry metadata: partitioned (TRaft) metadata always travels in the dedicated fields 5-9; any
 * other non-empty metadata is encoded by the group's {@link EntryMetaCodec} into {@code
 * profileMeta}. The overloads without a codec use {@link EntryMetaCodec#EMPTY}, i.e. exactly the
 * original behaviour (only partitioned metadata travels).
 */
public final class ThriftCodec {

  private ThriftCodec() {}

  // ------------------------------------------------------------------ entries

  /** Same as {@code encode(e, EntryMetaCodec.EMPTY)}: only partitioned metadata travels. */
  public static TRaftLogEntry encode(LogEntry e) {
    return encode(e, EntryMetaCodec.EMPTY);
  }

  public static TRaftLogEntry encode(LogEntry e, EntryMetaCodec codec) {
    TRaftLogEntry t = new TRaftLogEntry();
    t.setIndex(e.index);
    t.setTerm(e.term);
    t.setType(TEntryType.findByValue(e.type.code));
    t.setPayload(e.payload());
    if (e.meta instanceof EntryMeta.Partitioned) {
      EntryMeta.Partitioned m = (EntryMeta.Partitioned) e.meta;
      if (!m.isBarrier()) {
        t.setTimestamp(m.ts());
        t.setTimestampMax(m.tsMax());
      }
      t.setPartition(m.rho());
      t.setPrevSize(m.prevSize());
      t.setFlags((byte) (m.isBarrier() ? PartitionedMeta.FLAG_BARRIER : 0));
    } else if (e.meta != EntryMeta.EMPTY) {
      int size = codec.size(e.meta);
      if (size > 0) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        codec.encode(e.meta, buf);
        t.setProfileMeta(buf.array());
      }
    }
    return t;
  }

  /** Same as {@code decode(t, EntryMetaCodec.EMPTY)}: {@code profileMeta} is ignored. */
  public static LogEntry decode(TRaftLogEntry t) {
    return decode(t, EntryMetaCodec.EMPTY);
  }

  public static LogEntry decode(TRaftLogEntry t, EntryMetaCodec codec) {
    EntryMeta meta = EntryMeta.EMPTY;
    if (t.isSetPartition()) {
      boolean barrier = t.isSetFlags() && (t.getFlags() & PartitionedMeta.FLAG_BARRIER) != 0;
      long ts = t.isSetTimestamp() ? t.getTimestamp() : Long.MIN_VALUE;
      long tsMax = t.isSetTimestampMax() ? t.getTimestampMax() : ts;
      meta =
          new PartitionedMeta(
              ts,
              tsMax,
              t.getPartition(),
              t.isSetPrevSize() ? t.getPrevSize() : 0,
              (byte) (barrier ? PartitionedMeta.FLAG_BARRIER : 0));
    } else if (t.isSetProfileMeta()) {
      ByteBuffer m = t.bufferForProfileMeta();
      meta = codec.decode(m, m.remaining());
    }
    ByteBuffer payload = t.bufferForPayload();
    return new LogEntry(
        t.getIndex(),
        t.getTerm(),
        EntryType.fromCode(t.getType().getValue()),
        meta,
        payload == null ? ByteBuffer.allocate(0) : payload.slice());
  }

  private static List<TRaftLogEntry> encodeEntries(List<LogEntry> entries, EntryMetaCodec codec) {
    List<TRaftLogEntry> es = new ArrayList<>(entries.size());
    for (LogEntry e : entries) {
      es.add(encode(e, codec));
    }
    return es;
  }

  private static List<LogEntry> decodeEntries(List<TRaftLogEntry> entries, EntryMetaCodec codec) {
    List<LogEntry> es = new ArrayList<>();
    if (entries != null) {
      for (TRaftLogEntry e : entries) {
        es.add(decode(e, codec));
      }
    }
    return es;
  }

  // ------------------------------------------------------------- AppendEntries

  /** Same as {@code encode(gid, r, EntryMetaCodec.EMPTY)}. */
  public static TAppendEntriesReq encode(TConsensusGroupId gid, AppendEntriesReq r) {
    return encode(gid, r, EntryMetaCodec.EMPTY);
  }

  public static TAppendEntriesReq encode(
      TConsensusGroupId gid, AppendEntriesReq r, EntryMetaCodec codec) {
    TAppendEntriesReq t = new TAppendEntriesReq();
    t.setGroupId(gid);
    t.setLeaderId(r.leaderId);
    t.setTerm(r.term);
    t.setPrevLogIndex(r.prevLogIndex);
    t.setPrevLogTerm(r.prevLogTerm);
    t.setEntries(encodeEntries(r.entries, codec));
    t.setLeaderCommit(r.leaderCommit);
    t.setLeaderCommitTerm(r.leaderCommitTerm);
    if (r.speculative) {
      t.setSpeculative(true);
    }
    if (r.fence) {
      t.setFence(true);
    }
    t.setMsgId(r.msgId);
    if (r.schedRound != 0) {
      t.setSchedRound(r.schedRound);
    }
    AppendExt x = r.ext;
    if (x != null) {
      if (x.commitBase != AppendExt.UNSET) {
        t.setCommitBase(x.commitBase);
      }
      if (x.commitBits != null) {
        t.setCommitBits(x.commitBits);
      }
      if (x.truncateFrom != AppendExt.UNSET) {
        t.setTruncateFrom(x.truncateFrom);
      }
      if (x.syncTerm != AppendExt.UNSET) {
        t.setSyncTerm(x.syncTerm);
      }
      if (x.recovery) {
        t.setRecovery(true);
      }
    }
    return t;
  }

  /** Same as {@code decode(t, EntryMetaCodec.EMPTY)}. */
  public static AppendEntriesReq decode(TAppendEntriesReq t) {
    return decode(t, EntryMetaCodec.EMPTY);
  }

  public static AppendEntriesReq decode(TAppendEntriesReq t, EntryMetaCodec codec) {
    if (t.isSetLogFile()) {
      throw new IllegalArgumentException(
          "Log file must be validated by its LSM group before decoding");
    }
    return new AppendEntriesReq(
        t.getTerm(),
        t.getLeaderId(),
        t.getPrevLogIndex(),
        t.getPrevLogTerm(),
        decodeEntries(t.getEntries(), codec),
        t.getLeaderCommit(),
        t.getLeaderCommitTerm(),
        t.isSetSpeculative() && t.isSpeculative(),
        t.isSetFence() && t.isFence(),
        t.isSetMsgId() ? t.getMsgId() : 0,
        t.isSetSchedRound() ? t.getSchedRound() : 0,
        decodeExt(t));
  }

  /** The extension carried by fields 14-18; null if none of them is set. */
  private static AppendExt decodeExt(TAppendEntriesReq t) {
    boolean recovery = t.isSetRecovery() && t.isRecovery();
    if (!t.isSetCommitBase()
        && !t.isSetCommitBits()
        && !t.isSetTruncateFrom()
        && !t.isSetSyncTerm()
        && !recovery) {
      return null;
    }
    return new AppendExt(
        t.isSetCommitBase() ? t.getCommitBase() : AppendExt.UNSET,
        t.isSetCommitBits() ? t.getCommitBits() : null,
        t.isSetTruncateFrom() ? t.getTruncateFrom() : AppendExt.UNSET,
        t.isSetSyncTerm() ? t.getSyncTerm() : AppendExt.UNSET,
        recovery);
  }

  public static TEntryAck encode(EntryAck a) {
    TEntryAck t = new TEntryAck();
    t.setIndex(a.index);
    t.setEntryTerm(a.entryTerm);
    t.setStatus(TAckStatus.valueOf(a.status.name()));
    if (a.reportedPartition != EntryAck.UNSET) {
      t.setReportedPartition(a.reportedPartition);
    }
    if (a.hintIndex != EntryAck.UNSET) {
      t.setHintIndex(a.hintIndex);
    }
    if (a.hintTerm != EntryAck.UNSET) {
      t.setHintTerm(a.hintTerm);
    }
    if (a.workingPartition != EntryAck.UNSET) {
      t.setWorkingPartition(a.workingPartition);
    }
    return t;
  }

  public static EntryAck decode(TEntryAck t) {
    return new EntryAck(
        t.getIndex(),
        t.getEntryTerm(),
        AckStatus.valueOf(t.getStatus().name()),
        t.isSetReportedPartition() ? t.getReportedPartition() : EntryAck.UNSET,
        t.isSetHintIndex() ? t.getHintIndex() : EntryAck.UNSET,
        t.isSetHintTerm() ? t.getHintTerm() : EntryAck.UNSET,
        t.isSetWorkingPartition() ? t.getWorkingPartition() : EntryAck.UNSET);
  }

  public static TAppendEntriesRes encode(AppendEntriesRes r) {
    TAppendEntriesRes t = new TAppendEntriesRes();
    t.setTerm(r.term);
    List<TEntryAck> acks = new ArrayList<>(r.acks.size());
    for (EntryAck a : r.acks) {
      acks.add(encode(a));
    }
    t.setAcks(acks);
    if (r.appliedIndex != AppendEntriesRes.UNSET) {
      t.setAppliedIndex(r.appliedIndex);
    }
    if (r.contiguousEnd != AppendEntriesRes.UNSET) {
      t.setContiguousEnd(r.contiguousEnd);
    }
    if (r.workingPartition >= 0) {
      t.setWorkingPartition(r.workingPartition);
    }
    if (r.truncatedFrom != AppendEntriesRes.UNSET) {
      t.setTruncatedFrom(r.truncatedFrom);
    }
    if (r.syncNumber != AppendEntriesRes.UNSET) {
      t.setSyncNumber(r.syncNumber);
    }
    if (r.logFirstIndex != AppendEntriesRes.UNSET) {
      t.setLogFirstIndex(r.logFirstIndex);
    }
    return t;
  }

  public static AppendEntriesRes decode(TAppendEntriesRes t, long msgId) {
    List<EntryAck> acks = new ArrayList<>();
    if (t.getAcks() != null) {
      for (TEntryAck a : t.getAcks()) {
        acks.add(decode(a));
      }
    }
    return new AppendEntriesRes(
        t.getTerm(),
        acks,
        t.isSetAppliedIndex() ? t.getAppliedIndex() : AppendEntriesRes.UNSET,
        t.isSetContiguousEnd() ? t.getContiguousEnd() : AppendEntriesRes.UNSET,
        t.isSetWorkingPartition() ? t.getWorkingPartition() : -1,
        t.isSetTruncatedFrom() ? t.getTruncatedFrom() : AppendEntriesRes.UNSET,
        msgId,
        t.isSetSyncNumber() ? t.getSyncNumber() : AppendEntriesRes.UNSET,
        t.isSetLogFirstIndex() ? t.getLogFirstIndex() : AppendEntriesRes.UNSET);
  }

  // --------------------------------------------------------------- RequestVote

  public static TRequestVoteReq encode(TConsensusGroupId gid, RequestVoteReq r) {
    TRequestVoteReq t = new TRequestVoteReq();
    t.setGroupId(gid);
    t.setCandidateId(r.candidateId);
    t.setTerm(r.term);
    t.setLastLogIndex(r.key.lastIndex);
    t.setLastLogTerm(r.key.lastTerm);
    t.setRhoHat(r.key.rhoHat);
    t.setSigmaHat(r.key.sigmaHat);
    t.setCommittedTermHat(r.key.committedTerm);
    t.setCommittedRhoHat(r.key.committedRho);
    if (r.preVote) {
      t.setPreVote(true);
    }
    if (r.key.syncNumber != 0) {
      t.setSyncNumber(r.key.syncNumber); // unset decodes as 0, the Raft / TRaft value
    }
    return t;
  }

  public static RequestVoteReq decode(TRequestVoteReq t) {
    VoteKey key =
        new VoteKey(
            t.getLastLogTerm(),
            t.getLastLogIndex(),
            t.isSetRhoHat() ? t.getRhoHat() : 0,
            t.isSetSigmaHat() ? t.getSigmaHat() : 0,
            t.isSetCommittedTermHat() ? t.getCommittedTermHat() : 0,
            t.isSetCommittedRhoHat() ? t.getCommittedRhoHat() : -1,
            t.isSetSyncNumber() ? t.getSyncNumber() : 0);
    return new RequestVoteReq(
        t.getTerm(), t.getCandidateId(), key, t.isSetPreVote() && t.isPreVote());
  }

  public static TRequestVoteRes encode(RequestVoteRes r) {
    TRequestVoteRes t = new TRequestVoteRes();
    t.setTerm(r.term);
    t.setGranted(r.granted);
    if (r.preVote) {
      t.setPreVote(true);
    }
    return t;
  }

  public static RequestVoteRes decode(TRequestVoteRes t) {
    return new RequestVoteRes(t.getTerm(), t.isGranted(), t.isSetPreVote() && t.isPreVote());
  }

  // ---------------------------------------------------------------- TimeoutNow

  public static TTimeoutNowReq encode(TConsensusGroupId gid, TimeoutNowReq r) {
    TTimeoutNowReq t = new TTimeoutNowReq();
    t.setGroupId(gid);
    t.setLeaderId(r.leaderId);
    t.setTerm(r.term);
    return t;
  }

  public static TimeoutNowReq decode(TTimeoutNowReq t) {
    return new TimeoutNowReq(t.getTerm(), t.getLeaderId());
  }

  // --------------------------------------------------------------- RecoverTerm

  public static TRecoverTermReq encode(TConsensusGroupId gid, RecoverTermReq r) {
    return new TRecoverTermReq(
        gid, r.leaderId, r.term, r.syncTerm, r.fromIndex, r.maxEntries, r.maxBytes);
  }

  public static RecoverTermReq decode(TRecoverTermReq t) {
    return new RecoverTermReq(
        t.getTerm(),
        t.getLeaderId(),
        t.getSyncTerm(),
        t.getFromIndex(),
        t.getMaxEntries(),
        t.getMaxBytes());
  }

  public static TRecoverTermRes encode(RecoverTermRes r, EntryMetaCodec codec) {
    TRecoverTermRes t = new TRecoverTermRes(r.term, r.granted, r.syncNumber, r.contiguousEnd);
    if (r.logFirstIndex != RecoverTermRes.UNSET) {
      t.setLogFirstIndex(r.logFirstIndex);
    }
    if (r.promiseBallot != RecoverTermRes.UNSET) {
      t.setPromiseBallot(r.promiseBallot);
    }
    if (r.promiseEnd != RecoverTermRes.UNSET) {
      t.setPromiseEnd(r.promiseEnd);
    }
    if (!r.entries.isEmpty()) {
      t.setEntries(encodeEntries(r.entries, codec));
    }
    if (r.committedIndexes.length > 0) {
      List<Long> committed = new ArrayList<>(r.committedIndexes.length);
      for (long i : r.committedIndexes) {
        committed.add(i);
      }
      t.setCommittedIndexes(committed);
    }
    if (r.more) {
      t.setMore(true);
    }
    if (r.nextIndex != RecoverTermRes.UNSET) {
      t.setNextIndex(r.nextIndex);
    }
    return t;
  }

  public static RecoverTermRes decode(TRecoverTermRes t, EntryMetaCodec codec) {
    long[] committed = new long[t.isSetCommittedIndexes() ? t.getCommittedIndexesSize() : 0];
    if (committed.length > 0) {
      int k = 0;
      for (Long i : t.getCommittedIndexes()) {
        committed[k++] = i;
      }
    }
    return new RecoverTermRes(
        t.getTerm(),
        t.isGranted(),
        t.getSyncNumber(),
        t.getContiguousEnd(),
        t.isSetLogFirstIndex() ? t.getLogFirstIndex() : RecoverTermRes.UNSET,
        t.isSetPromiseBallot() ? t.getPromiseBallot() : RecoverTermRes.UNSET,
        t.isSetPromiseEnd() ? t.getPromiseEnd() : RecoverTermRes.UNSET,
        decodeEntries(t.getEntries(), codec),
        committed,
        t.isSetMore() && t.isMore(),
        t.isSetNextIndex() ? t.getNextIndex() : RecoverTermRes.UNSET);
  }

  // ---------------------------------------------------------------- UpdateSync

  public static TUpdateSyncReq encode(TConsensusGroupId gid, UpdateSyncReq r) {
    return new TUpdateSyncReq(gid, r.leaderId, r.term, r.newSync);
  }

  public static UpdateSyncReq decode(TUpdateSyncReq t) {
    return new UpdateSyncReq(t.getTerm(), t.getLeaderId(), t.getNewSync());
  }

  public static TUpdateSyncRes encode(UpdateSyncRes r) {
    TUpdateSyncRes t = new TUpdateSyncRes(r.term, r.granted, r.syncNumber);
    if (r.contiguousEnd != UpdateSyncRes.UNSET) {
      t.setContiguousEnd(r.contiguousEnd);
    }
    return t;
  }

  public static UpdateSyncRes decode(TUpdateSyncRes t) {
    return new UpdateSyncRes(
        t.getTerm(),
        t.isGranted(),
        t.getSyncNumber(),
        t.isSetContiguousEnd() ? t.getContiguousEnd() : UpdateSyncRes.UNSET);
  }
}
