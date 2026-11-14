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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
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
import org.apache.iotdb.consensus.raft.core.profile.RaftProfile;
import org.apache.iotdb.consensus.raft.server.WriteForwarder;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesReq;
import org.apache.iotdb.consensus.raft.thrift.TAppendEntriesRes;
import org.apache.iotdb.consensus.raft.thrift.TRaftLogEntry;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermReq;
import org.apache.iotdb.consensus.raft.thrift.TRecoverTermRes;
import org.apache.iotdb.consensus.raft.thrift.TRequestVoteReq;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncReq;
import org.apache.iotdb.consensus.raft.thrift.TUpdateSyncRes;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ThriftCodecTest {

  private static final TConsensusGroupId GID =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 5);

  private static LogEntry entry(long index, long term, String data, EntryMeta meta) {
    return new LogEntry(
        index, term, EntryType.DATA, meta, ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void appendEntriesRoundTripThroughWireBytes() throws TException {
    LogEntry raft = entry(3, 2, "plain", EntryMeta.EMPTY);
    LogEntry traft = entry(4, 2, "timed", new PartitionedMeta(100, 120, 7, 3, (byte) 0));
    LogEntry barrier =
        new LogEntry(
            5, 2, EntryType.BARRIER, PartitionedMeta.barrier(8, 2), ByteBuffer.allocate(0));
    AppendEntriesReq req =
        new AppendEntriesReq(
            2, 1, 2, 2, Arrays.asList(raft, traft, barrier), 1, 2, true, false, 77, 9);
    TAppendEntriesReq t = ThriftCodec.encode(GID, req);
    byte[] wire = new TSerializer(new TCompactProtocol.Factory()).serialize(t);
    TAppendEntriesReq back = new TAppendEntriesReq();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(back, wire);
    AppendEntriesReq d = ThriftCodec.decode(back);
    assertEquals(2, d.term);
    assertEquals(1, d.leaderId);
    assertEquals(2, d.prevLogIndex);
    assertTrue(d.speculative);
    assertFalse(d.fence);
    assertEquals(77, d.msgId);
    assertEquals(9, d.schedRound);
    assertEquals(3, d.entries.size());
    assertSame(EntryMeta.EMPTY, d.entries.get(0).meta);
    assertEquals("plain", d.entries.get(0).payloadAsString());
    PartitionedMeta m = (PartitionedMeta) d.entries.get(1).meta;
    assertEquals(100, m.ts);
    assertEquals(120, m.tsMax);
    assertEquals(7, m.rho);
    assertEquals(3, m.prevSize);
    assertFalse(m.isBarrier());
    PartitionedMeta b = (PartitionedMeta) d.entries.get(2).meta;
    assertTrue(b.isBarrier());
    assertEquals(8, b.rho);
    assertEquals(EntryType.BARRIER, d.entries.get(2).type);
    TRaftLogEntry tb = back.getEntries().get(2);
    assertFalse(tb.isSetTimestamp());
  }

  @Test
  public void responseKeepsUnsetOptionalFields() throws TException {
    AppendEntriesRes res =
        new AppendEntriesRes(
            3,
            Arrays.asList(
                EntryAck.ack(1, 3),
                EntryAck.received(4, 3),
                EntryAck.nack(6, 3, 2),
                EntryAck.nack(7, 3, EntryAck.UNSET),
                EntryAck.reject(2, 1, 3, 2),
                EntryAck.needFence(9, 1),
                EntryAck.fenceAck(0, 1, 4)),
            1,
            1,
            -1,
            AppendEntriesRes.UNSET,
            5);
    TAppendEntriesRes t = ThriftCodec.encode(res);
    byte[] wire = new TSerializer(new TCompactProtocol.Factory()).serialize(t);
    TAppendEntriesRes back = new TAppendEntriesRes();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(back, wire);
    AppendEntriesRes d = ThriftCodec.decode(back, 5);
    assertEquals(7, d.acks.size());
    assertEquals(AckStatus.RECEIVED, d.acks.get(1).status);
    assertEquals(2, d.acks.get(2).reportedPartition);
    assertEquals(EntryAck.UNSET, d.acks.get(3).reportedPartition);
    assertEquals(3, d.acks.get(4).hintIndex);
    assertEquals(2, d.acks.get(4).hintTerm);
    assertEquals(AckStatus.NEED_FENCE, d.acks.get(5).status);
    assertTrue(d.acks.get(6).isFenceAck());
    assertEquals(4, d.acks.get(6).workingPartition);
    assertFalse(d.hasTruncatedFrom());
    assertEquals(1, d.appliedIndex);
    assertEquals(-1, d.workingPartition);
    assertEquals(5, d.msgId);
  }

  @Test
  public void voteMessagesRoundTrip() {
    RequestVoteReq rv = new RequestVoteReq(4, 2, VoteKey.triple(3, 9, 2, 5), true);
    RequestVoteReq d = ThriftCodec.decode(ThriftCodec.encode(GID, rv));
    assertEquals(4, d.term);
    assertEquals(2, d.candidateId);
    assertTrue(d.preVote);
    assertEquals(3, d.key.lastTerm);
    assertEquals(9, d.key.lastIndex);
    assertEquals(2, d.key.rhoHat);
    assertEquals(5, d.key.sigmaHat);
    RequestVoteRes r = ThriftCodec.decode(ThriftCodec.encode(new RequestVoteRes(4, true, true)));
    assertTrue(r.granted);
    assertTrue(r.preVote);
    assertFalse(
        ThriftCodec.decode(ThriftCodec.encode(new RequestVoteRes(4, false, false))).preVote);
  }

  // ------------------------------------------------------------ ParallelRaft fields

  /** Serialize with the compact protocol and read the bytes back into {@code fresh}. */
  @SuppressWarnings("rawtypes")
  private static <T extends TBase> T wire(TBase original, T fresh) throws TException {
    byte[] bytes = new TSerializer(new TCompactProtocol.Factory()).serialize(original);
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(fresh, bytes);
    return fresh;
  }

  @SuppressWarnings("rawtypes")
  private static byte[] bytes(TBase t) throws TException {
    return new TSerializer(new TCompactProtocol.Factory()).serialize(t);
  }

  private static AppendEntriesReq raftAndTRaftRequest() {
    LogEntry raft = entry(3, 2, "plain", EntryMeta.EMPTY);
    LogEntry traft = entry(4, 2, "timed", new PartitionedMeta(100, 120, 7, 3, (byte) 0));
    LogEntry barrier =
        new LogEntry(
            5, 2, EntryType.BARRIER, PartitionedMeta.barrier(8, 2), ByteBuffer.allocate(0));
    return new AppendEntriesReq(
        2, 1, 2, 2, Arrays.asList(raft, traft, barrier), 1, 2, true, false, 77, 9);
  }

  @Test
  public void raftAndTRaftMessagesKeepTheirWireFormat() throws TException {
    AppendEntriesReq req = raftAndTRaftRequest();
    TAppendEntriesReq viaOld = ThriftCodec.encode(GID, req);
    for (EntryMetaCodec codec :
        Arrays.asList(EntryMetaCodec.EMPTY, PartitionedMeta.CODEC, DatedTestMeta.CODEC)) {
      TAppendEntriesReq t = ThriftCodec.encode(GID, req, codec);
      assertArrayEquals(bytes(viaOld), bytes(t)); // the codec never matters for Raft / TRaft
    }
    assertFalse(viaOld.isSetCommitBase());
    assertFalse(viaOld.isSetCommitBits());
    assertFalse(viaOld.isSetTruncateFrom());
    assertFalse(viaOld.isSetSyncTerm());
    assertFalse(viaOld.isSetRecovery());
    for (TRaftLogEntry e : viaOld.getEntries()) {
      assertFalse(e.isSetProfileMeta());
    }
    assertNull(ThriftCodec.decode(wire(viaOld, new TAppendEntriesReq()), DatedTestMeta.CODEC).ext);

    AppendEntriesRes res =
        new AppendEntriesRes(3, Collections.singletonList(EntryAck.ack(1, 3)), 1, 1, -1, 0, 5);
    TAppendEntriesRes tr = ThriftCodec.encode(res);
    assertFalse(tr.isSetSyncNumber());
    assertFalse(tr.isSetLogFirstIndex());
    AppendEntriesRes back = ThriftCodec.decode(wire(tr, new TAppendEntriesRes()), 5);
    assertFalse(back.hasSyncNumber());
    assertFalse(back.hasLogFirstIndex());

    TRequestVoteReq rv =
        ThriftCodec.encode(GID, new RequestVoteReq(4, 2, VoteKey.raft(3, 9), false));
    assertFalse(rv.isSetSyncNumber());
    assertEquals(0, ThriftCodec.decode(rv).key.syncNumber);
    TRequestVoteReq triple =
        ThriftCodec.encode(GID, new RequestVoteReq(4, 2, VoteKey.triple(3, 9, 2, 5), true));
    assertFalse(triple.isSetSyncNumber());
  }

  // ------------------------------------------------------------ HEAD wire bytes

  // Raft / TRaft messages as HEAD (23716d8a5e) put them on the wire (compact protocol), encoded
  // once by the HEAD codec with the thrift classes generated from the HEAD IDL. Whatever the group
  // codec, the same messages must keep these bytes.
  private static final String HEAD_AE_ENTRIES =
      "1c1502150a001502160416041604193c1606160415021805706c61696e00160816041502180574696d656416c80116f001160e1506130000160a160415081800361015041301001602160411269a01161200";
  private static final String HEAD_AE_HEARTBEAT = "1c1502150a001502160616011600190c16081606361800";
  private static final String HEAD_AE_FENCE =
      "1c1502150a0015021606160a1604190c1608160621161a160400";
  private static final String HEAD_RES_ACKS =
      "1606197c1602160615020016081606150400160c16061506160400160e1606150600160416021508260616040016011600150a26121602001600160215024608001602160200";
  private static final String HEAD_RES_TRAFT = "1608191c160c160815040026061604160800";
  private static final String HEAD_RV_RAFT = "1c1502150a001504160816121606160016001600160100";
  private static final String HEAD_RV_TRIPLE = "1c1502150a0015041608161216061604160a160016011100";
  private static final String HEAD_RV_PAIR = "1c1502150a001506160c161216061604160a1606160200";
  private static final String HEAD_RVR_GRANTED = "1608111100";
  private static final String HEAD_RVR_DENIED = "16081200";
  private static final String HEAD_TIMEOUT_NOW = "1c1502150a001504160a00";
  private static final String HEAD_FW_TIMED = "1c1502150a001803010203160a161200";
  private static final String HEAD_FW_OPAQUE = "1c1502150a0018030102033100";

  @SuppressWarnings("rawtypes")
  private static String hex(TBase t) throws TException {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes(t)) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  private static <T extends TBase> T fromHex(String hex, T fresh) throws TException {
    byte[] raw = new byte[hex.length() / 2];
    for (int i = 0; i < raw.length; i++) {
      raw[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
    }
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(fresh, raw);
    return fresh;
  }

  @Test
  public void raftAndTRaftMessagesKeepTheHeadWireBytes() throws TException {
    AppendEntriesReq full = raftAndTRaftRequest();
    assertEquals(HEAD_AE_ENTRIES, hex(ThriftCodec.encode(GID, full)));
    EntryMetaCodec[] codecs = {
      EntryMetaCodec.EMPTY,
      PartitionedMeta.CODEC,
      DatedTestMeta.CODEC,
      new RaftProfile().metaCodec()
    };
    AppendEntriesReq heartbeat =
        new AppendEntriesReq(3, 1, -1, 0, Collections.emptyList(), 4, 3, false, false, 12, 0);
    AppendEntriesReq fence =
        new AppendEntriesReq(3, 1, 5, 2, Collections.emptyList(), 4, 3, false, true, 13, 2);
    for (EntryMetaCodec codec : codecs) {
      assertEquals(HEAD_AE_ENTRIES, hex(ThriftCodec.encode(GID, full, codec)));
      assertEquals(HEAD_AE_HEARTBEAT, hex(ThriftCodec.encode(GID, heartbeat, codec)));
      assertEquals(HEAD_AE_FENCE, hex(ThriftCodec.encode(GID, fence, codec)));
    }
    AppendEntriesRes acks =
        new AppendEntriesRes(
            3,
            Arrays.asList(
                EntryAck.ack(1, 3),
                EntryAck.received(4, 3),
                EntryAck.nack(6, 3, 2),
                EntryAck.nack(7, 3, EntryAck.UNSET),
                EntryAck.reject(2, 1, 3, 2),
                EntryAck.needFence(9, 1),
                EntryAck.fenceAck(0, 1, 4)),
            1,
            1,
            -1,
            AppendEntriesRes.UNSET,
            5);
    assertEquals(HEAD_RES_ACKS, hex(ThriftCodec.encode(acks)));
    AppendEntriesRes traft =
        new AppendEntriesRes(
            4,
            Collections.singletonList(EntryAck.received(6, 4)),
            AppendEntriesRes.UNSET,
            3,
            2,
            4,
            9);
    assertEquals(HEAD_RES_TRAFT, hex(ThriftCodec.encode(traft)));
    assertEquals(
        HEAD_RV_RAFT,
        hex(ThriftCodec.encode(GID, new RequestVoteReq(4, 2, VoteKey.raft(3, 9), false))));
    assertEquals(
        HEAD_RV_TRIPLE,
        hex(ThriftCodec.encode(GID, new RequestVoteReq(4, 2, VoteKey.triple(3, 9, 2, 5), true))));
    assertEquals(
        HEAD_RV_PAIR,
        hex(
            ThriftCodec.encode(
                GID, new RequestVoteReq(6, 3, new VoteKey(3, 9, 2, 5, 3, 1), false))));
    assertEquals(HEAD_RVR_GRANTED, hex(ThriftCodec.encode(new RequestVoteRes(4, true, true))));
    assertEquals(HEAD_RVR_DENIED, hex(ThriftCodec.encode(new RequestVoteRes(4, false, false))));
    assertEquals(HEAD_TIMEOUT_NOW, hex(ThriftCodec.encode(GID, new TimeoutNowReq(5, 2))));
    byte[] payload = {1, 2, 3};
    assertEquals(
        HEAD_FW_TIMED,
        hex(WriteForwarder.request(GID, ByteBuffer.wrap(payload), RequestMeta.timed(5, 9))));
    assertEquals(
        HEAD_FW_OPAQUE,
        hex(WriteForwarder.request(GID, ByteBuffer.wrap(payload), RequestMeta.OPAQUE)));

    // and what HEAD put on the wire still decodes as it did: no extension, no sync fields
    AppendEntriesReq d =
        ThriftCodec.decode(fromHex(HEAD_AE_ENTRIES, new TAppendEntriesReq()), DatedTestMeta.CODEC);
    assertNull(d.ext);
    assertEquals(77, d.msgId);
    assertSame(EntryMeta.EMPTY, d.entries.get(0).meta);
    assertEquals(7, ((PartitionedMeta) d.entries.get(1).meta).rho);
    assertTrue(((PartitionedMeta) d.entries.get(2).meta).isBarrier());
    assertNull(
        ThriftCodec.decode(fromHex(HEAD_AE_FENCE, new TAppendEntriesReq()), DatedTestMeta.CODEC)
            .ext);
    AppendEntriesRes r = ThriftCodec.decode(fromHex(HEAD_RES_ACKS, new TAppendEntriesRes()), 5);
    assertEquals(7, r.acks.size());
    assertFalse(r.hasSyncNumber());
    assertFalse(r.hasLogFirstIndex());
    RequestVoteReq v = ThriftCodec.decode(fromHex(HEAD_RV_TRIPLE, new TRequestVoteReq()));
    assertEquals(0, v.key.syncNumber);
    assertEquals(2, v.key.rhoHat);
  }

  @Test
  public void profileMetaTravelsThroughTheGroupCodec() throws TException {
    LogEntry dated = entry(6, 3, "dated", new DatedTestMeta(5, 42));
    TRaftLogEntry t = wire(ThriftCodec.encode(dated, DatedTestMeta.CODEC), new TRaftLogEntry());
    assertTrue(t.isSetProfileMeta());
    assertFalse(t.isSetPartition());
    assertFalse(t.isSetTimestamp());
    LogEntry d = ThriftCodec.decode(t, DatedTestMeta.CODEC);
    DatedTestMeta m = (DatedTestMeta) d.meta;
    assertEquals(5, m.date);
    assertEquals(42, m.tag);
    assertEquals(6, d.index);
    assertEquals(3, d.term);
    assertEquals("dated", d.payloadAsString());
    // without the codec (original behaviour) the profile meta is neither written nor read
    assertFalse(ThriftCodec.encode(dated).isSetProfileMeta());
    assertSame(EntryMeta.EMPTY, ThriftCodec.decode(t).meta);
    assertSame(EntryMeta.EMPTY, ThriftCodec.decode(t, PartitionedMeta.CODEC).meta);
    // partitioned metadata keeps fields 5-9 whatever the codec; empty metadata sets neither
    LogEntry timed = entry(7, 3, "timed", new PartitionedMeta(100, 120, 7, 3, (byte) 0));
    TRaftLogEntry tt = ThriftCodec.encode(timed, DatedTestMeta.CODEC);
    assertTrue(tt.isSetPartition());
    assertFalse(tt.isSetProfileMeta());
    assertEquals(7, ((PartitionedMeta) ThriftCodec.decode(tt, DatedTestMeta.CODEC).meta).rho);
    TRaftLogEntry te =
        ThriftCodec.encode(entry(8, 3, "plain", EntryMeta.EMPTY), DatedTestMeta.CODEC);
    assertFalse(te.isSetProfileMeta());
    assertFalse(te.isSetPartition());

    // the same through a whole AppendEntries
    AppendEntriesReq req =
        new AppendEntriesReq(3, 1, -1, 0, Arrays.asList(dated, timed), 4, 3, false, false, 11, 0);
    AppendEntriesReq back =
        ThriftCodec.decode(
            wire(ThriftCodec.encode(GID, req, DatedTestMeta.CODEC), new TAppendEntriesReq()),
            DatedTestMeta.CODEC);
    assertEquals(42, ((DatedTestMeta) back.entries.get(0).meta).tag);
    assertTrue(back.entries.get(1).meta instanceof PartitionedMeta);
  }

  @Test
  public void appendExtRoundTrip() throws TException {
    // commit information on a heartbeat: bitmap over (5, 5 + B]
    byte[] bits = AppendExt.bitsOf(5, new java.util.TreeSet<>(Arrays.asList(6L, 8L, 13L)), 64);
    AppendEntriesReq hb =
        new AppendEntriesReq(4, 1, -1, 0, null, 5, 4, false, false, 21, 0)
            .withExt(AppendExt.commit(5, bits));
    assertEquals("CN", hb.kind());
    TAppendEntriesReq t = wire(ThriftCodec.encode(GID, hb), new TAppendEntriesReq());
    assertEquals(5, t.getCommitBase());
    assertFalse(t.isSetTruncateFrom());
    assertFalse(t.isSetSyncTerm());
    assertFalse(t.isSetRecovery());
    AppendEntriesReq d = ThriftCodec.decode(t, DatedTestMeta.CODEC);
    assertEquals(hb.ext, d.ext);
    assertEquals(Arrays.asList(6L, 8L, 13L), d.ext.committedInBits());
    assertTrue(d.ext.bitSet(8));
    assertFalse(d.ext.bitSet(7));
    assertEquals(21, d.msgId);

    // a recovery push with entries and a truncation point
    LogEntry pushed = entry(10, 2, "p", new DatedTestMeta(4, 1));
    AppendEntriesReq push =
        new AppendEntriesReq(
                4, 1, -1, 0, Collections.singletonList(pushed), 5, 4, false, false, 22, 0)
            .withExt(AppendExt.push(12, 2));
    assertEquals("PUSH", push.kind());
    AppendEntriesReq pd =
        ThriftCodec.decode(
            wire(ThriftCodec.encode(GID, push, DatedTestMeta.CODEC), new TAppendEntriesReq()),
            DatedTestMeta.CODEC);
    assertEquals("PUSH", pd.kind());
    assertEquals(push.ext, pd.ext);
    assertTrue(pd.ext.recovery);
    assertEquals(12, pd.ext.truncateFrom);
    assertEquals(2, pd.ext.syncTerm);
    assertFalse(pd.ext.hasCommit());
    assertNull(pd.ext.commitBits);
    assertEquals(4, ((DatedTestMeta) pd.entries.get(0).meta).date);

    // a push without a truncation point, carrying commit information as well
    AppendExt both = AppendExt.push(AppendExt.UNSET, 2).withCommit(9, new byte[] {1});
    AppendEntriesReq bd =
        ThriftCodec.decode(
            wire(ThriftCodec.encode(GID, push.withExt(both)), new TAppendEntriesReq()));
    assertEquals(both, bd.ext);
    assertFalse(bd.ext.hasTruncateFrom());
    assertTrue(bd.ext.bitSet(10));
  }

  @Test
  public void appendEntriesResSyncFieldsRoundTrip() throws TException {
    AppendEntriesRes res =
        new AppendEntriesRes(
            5,
            Arrays.asList(EntryAck.ack(7, 5), EntryAck.ack(9, 5)),
            AppendEntriesRes.UNSET,
            6,
            -1,
            AppendEntriesRes.UNSET,
            33,
            4,
            2);
    TAppendEntriesRes t = wire(ThriftCodec.encode(res), new TAppendEntriesRes());
    assertEquals(4, t.getSyncNumber());
    assertEquals(2, t.getLogFirstIndex());
    AppendEntriesRes d = ThriftCodec.decode(t, 33);
    assertEquals(4, d.syncNumber);
    assertEquals(2, d.logFirstIndex);
    assertEquals(6, d.contiguousEnd);
    assertEquals(2, d.acks.size());
    assertFalse(d.hasAppliedIndex());
    assertEquals(33, d.msgId);
    AppendEntriesRes one = d.withAcks(Collections.singletonList(d.acks.get(1)));
    assertEquals(9, one.acks.get(0).index);
    assertEquals(4, one.syncNumber);
    assertEquals(2, one.logFirstIndex);
    assertEquals(6, one.contiguousEnd);
    assertEquals(33, one.msgId);
  }

  @Test
  public void voteSyncNumberRoundTrip() throws TException {
    RequestVoteReq rv = new RequestVoteReq(6, 2, VoteKey.sync(5, 4, 17), true);
    TRequestVoteReq t = wire(ThriftCodec.encode(GID, rv), new TRequestVoteReq());
    assertEquals(5, t.getSyncNumber());
    RequestVoteReq d = ThriftCodec.decode(t);
    assertEquals(5, d.key.syncNumber);
    assertEquals(4, d.key.lastTerm);
    assertEquals(17, d.key.lastIndex);
    assertTrue(d.preVote);
    assertEquals(0, VoteKey.compareSync(d.key, VoteKey.sync(5, 9, 99)));
    assertTrue(VoteKey.compareSync(d.key, VoteKey.sync(4, 9, 99)) > 0);
  }

  @Test
  public void recoverTermMessagesRoundTrip() throws TException {
    RecoverTermReq req = new RecoverTermReq(7, 1, 5, 10, 128, 1L << 24);
    TRecoverTermReq treq = wire(ThriftCodec.encode(GID, req), new TRecoverTermReq());
    assertEquals(GID, treq.getGroupId());
    RecoverTermReq dreq = ThriftCodec.decode(treq);
    assertEquals(7, dreq.term);
    assertEquals(1, dreq.leaderId);
    assertEquals(5, dreq.syncTerm);
    assertEquals(10, dreq.fromIndex);
    assertEquals(128, dreq.maxEntries);
    assertEquals(1L << 24, dreq.maxBytes);

    List<LogEntry> page =
        Arrays.asList(
            entry(10, 5, "a", new DatedTestMeta(5, 1)),
            entry(12, 5, "c", new DatedTestMeta(6, 3)),
            new LogEntry(13, 5, EntryType.NOOP, new DatedTestMeta(6, 0), ByteBuffer.allocate(0)));
    RecoverTermRes res =
        new RecoverTermRes(7, true, 5, 9, 2, 6, 14, page, new long[] {10, 12}, true, 14);
    TRecoverTermRes tres =
        wire(ThriftCodec.encode(res, DatedTestMeta.CODEC), new TRecoverTermRes());
    RecoverTermRes d = ThriftCodec.decode(tres, DatedTestMeta.CODEC);
    assertEquals(7, d.term);
    assertTrue(d.granted);
    assertEquals(5, d.syncNumber);
    assertEquals(9, d.contiguousEnd);
    assertEquals(2, d.logFirstIndex);
    assertTrue(d.hasPromise());
    assertEquals(6, d.promiseBallot);
    assertEquals(14, d.promiseEnd);
    assertEquals(3, d.entries.size());
    assertEquals("c", d.entries.get(1).payloadAsString());
    assertEquals(6, ((DatedTestMeta) d.entries.get(1).meta).date);
    assertEquals(EntryType.NOOP, d.entries.get(2).type);
    assertArrayEquals(new long[] {10, 12}, d.committedIndexes);
    assertTrue(d.isCommitted(12));
    assertFalse(d.isCommitted(13));
    assertTrue(d.more);
    assertEquals(14, d.nextIndex);

    // a voter below the sync term answers only its sync number and frontier
    RecoverTermRes low =
        new RecoverTermRes(
            7,
            true,
            3,
            4,
            RecoverTermRes.UNSET,
            RecoverTermRes.UNSET,
            RecoverTermRes.UNSET,
            null,
            null,
            false,
            RecoverTermRes.UNSET);
    TRecoverTermRes tlow =
        wire(ThriftCodec.encode(low, DatedTestMeta.CODEC), new TRecoverTermRes());
    assertFalse(tlow.isSetLogFirstIndex());
    assertFalse(tlow.isSetPromiseBallot());
    assertFalse(tlow.isSetPromiseEnd());
    assertFalse(tlow.isSetEntries());
    assertFalse(tlow.isSetCommittedIndexes());
    assertFalse(tlow.isSetMore());
    assertFalse(tlow.isSetNextIndex());
    RecoverTermRes dlow = ThriftCodec.decode(tlow, DatedTestMeta.CODEC);
    assertEquals(3, dlow.syncNumber);
    assertEquals(4, dlow.contiguousEnd);
    assertFalse(dlow.hasPromise());
    assertFalse(dlow.hasLogFirstIndex());
    assertEquals(RecoverTermRes.UNSET, dlow.promiseEnd);
    assertTrue(dlow.entries.isEmpty());
    assertEquals(0, dlow.committedIndexes.length);
    assertFalse(dlow.more);
    assertEquals(RecoverTermRes.UNSET, dlow.nextIndex);
  }

  @Test
  public void updateSyncMessagesRoundTrip() throws TException {
    UpdateSyncReq req = new UpdateSyncReq(7, 1, 7);
    UpdateSyncReq dreq =
        ThriftCodec.decode(wire(ThriftCodec.encode(GID, req), new TUpdateSyncReq()));
    assertEquals(7, dreq.term);
    assertEquals(1, dreq.leaderId);
    assertEquals(7, dreq.newSync);

    UpdateSyncRes res = new UpdateSyncRes(7, true, 7, 20);
    UpdateSyncRes d = ThriftCodec.decode(wire(ThriftCodec.encode(res), new TUpdateSyncRes()));
    assertEquals(7, d.term);
    assertTrue(d.granted);
    assertEquals(7, d.syncNumber);
    assertEquals(20, d.contiguousEnd);
    UpdateSyncRes noCe = new UpdateSyncRes(7, false, 5, UpdateSyncRes.UNSET);
    TUpdateSyncRes t = ThriftCodec.encode(noCe);
    assertFalse(t.isSetContiguousEnd());
    UpdateSyncRes dn = ThriftCodec.decode(wire(t, new TUpdateSyncRes()));
    assertFalse(dn.granted);
    assertEquals(5, dn.syncNumber);
    assertFalse(dn.hasContiguousEnd());
  }
}
