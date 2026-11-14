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

package org.apache.iotdb.consensus.raft.core.msg;

import org.apache.iotdb.consensus.raft.core.MetaToken;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.profile.Capabilities;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.RaftProfile;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The kernel message / value extensions for out-of-order profiles (ParallelRaft): their defaults
 * leave Raft / TRaft values, kinds and briefs exactly as before.
 */
public class MessageExtensionsTest {

  private static LogEntry entry(long index, long term, String data) {
    return new LogEntry(
        index,
        term,
        EntryType.DATA,
        EntryMeta.EMPTY,
        ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void appendExtFactoriesAndCombinators() {
    AppendExt c = AppendExt.commit(5, new byte[] {3});
    assertTrue(c.hasCommit());
    assertFalse(c.recovery);
    assertFalse(c.hasTruncateFrom());
    assertFalse(c.hasSyncTerm());
    AppendExt p = AppendExt.push(12, 4);
    assertTrue(p.recovery);
    assertEquals(12, p.truncateFrom);
    assertEquals(4, p.syncTerm);
    assertFalse(p.hasCommit());
    assertNull(p.commitBits);
    assertTrue(AppendExt.push(AppendExt.UNSET, 4).recovery);
    AppendExt both = c.withPush(9, 3);
    assertEquals(new AppendExt(5, new byte[] {3}, 9, 3, true), both);
    assertEquals(both.hashCode(), new AppendExt(5, new byte[] {3}, 9, 3, true).hashCode());
    assertEquals(AppendExt.UNSET, both.withTruncateFrom(AppendExt.UNSET).truncateFrom);
    assertEquals(7, both.withSyncTerm(7).syncTerm);
    assertFalse(both.withRecovery(false).recovery);
    assertEquals(p.withCommit(5, new byte[] {3}), c.withPush(12, 4));
    assertNotEquals(c, AppendExt.commit(5, new byte[] {1}));
    assertTrue(
        new AppendExt(AppendExt.UNSET, null, AppendExt.UNSET, AppendExt.UNSET, false).isEmpty());
    assertFalse(c.isEmpty());
  }

  @Test
  public void commitBitmapCoversTheRangeAfterTheBase() {
    TreeSet<Long> committed = new TreeSet<>(Arrays.asList(3L, 5L, 6L, 9L, 30L, 40L));
    byte[] bits = AppendExt.bitsOf(4, committed, 16); // covers (4, 20]
    AppendExt x = AppendExt.commit(4, bits);
    assertEquals(Arrays.asList(5L, 6L, 9L), x.committedInBits());
    assertEquals(1, bits.length); // only up to the highest set bit (index 9 = bit 4)
    assertTrue(x.bitSet(5));
    assertTrue(x.bitSet(9));
    assertFalse(x.bitSet(4)); // the base itself is not covered
    assertFalse(x.bitSet(3));
    assertFalse(x.bitSet(7));
    assertFalse(x.bitSet(30)); // beyond the encoded bytes
    assertArrayEquals(new byte[] {(byte) 0b10011}, bits);
    assertNull(AppendExt.bitsOf(40, committed, 16)); // nothing in (40, 56]
    assertNull(AppendExt.bitsOf(4, committed, 0));
    assertNull(AppendExt.bitsOf(4, new TreeSet<>(), 16));
    assertEquals(
        Arrays.asList(5L, 6L, 9L, 30L),
        AppendExt.commit(4, AppendExt.bitsOf(4, committed, 26)).committedInBits());
    assertTrue(AppendExt.push(1, 1).committedInBits().isEmpty());
    assertFalse(AppendExt.push(1, 1).bitSet(2));
  }

  @Test
  public void appendEntriesReqExtIsOptionalAndPreserved() {
    AppendEntriesReq ae =
        new AppendEntriesReq(
            3, 1, 4, 2, Collections.singletonList(entry(5, 3, "x")), 4, 2, false, false, 9, 0);
    assertNull(ae.ext);
    assertEquals("AE", ae.kind());
    assertEquals("AE(e5(t3)) term=3", ae.brief());
    AppendEntriesReq cn = new AppendEntriesReq(3, 1, -1, 0, null, 4, 2, false, false, 9, 0);
    assertEquals("CN", cn.kind());
    assertEquals("CommitNotify(commit=4,t=3)", cn.brief());
    assertFalse(ae.isPush());

    AppendExt push = AppendExt.push(6, 2);
    AppendEntriesReq p = ae.withExt(push);
    assertEquals("PUSH", p.kind());
    assertTrue(p.isPush());
    assertTrue(p.brief().startsWith("Push(e5(t3)) term=3 ext{"));
    assertSame(push, p.withMsgId(10).ext);
    assertEquals(10, p.withMsgId(10).msgId);
    assertSame(push, p.withEntries(Collections.emptyList(), false).ext);
    assertEquals(
        "PUSH", p.withEntries(Collections.emptyList(), false).kind()); // truncate-only push
    assertNull(p.withExt(null).ext);
    assertEquals("AE", p.withExt(null).kind());
    AppendEntriesReq withCommit = cn.withExt(AppendExt.commit(4, new byte[] {1}));
    assertEquals("CN", withCommit.kind());
    assertEquals(cn.msgId, withCommit.msgId);
    AppendEntriesReq fence = new AppendEntriesReq(3, 1, 4, 2, null, 4, 2, false, true, 9, 0);
    assertEquals("FENCE", fence.withExt(push).kind());
  }

  @Test
  public void appendEntriesResNewFieldsDefaultToUnset() {
    AppendEntriesRes old =
        new AppendEntriesRes(
            3,
            Arrays.asList(EntryAck.ack(1, 3), EntryAck.ack(2, 3)),
            1,
            2,
            -1,
            AppendEntriesRes.UNSET,
            7);
    assertFalse(old.hasSyncNumber());
    assertFalse(old.hasLogFirstIndex());
    assertEquals("RESP[ACK(e1,t3) ACK(e2,t3)] term=3 applied=1", old.brief());
    AppendEntriesRes split = old.withAcks(Collections.singletonList(old.acks.get(1)));
    assertEquals(1, split.acks.size());
    assertEquals(2, split.acks.get(0).index);
    assertEquals(old.appliedIndex, split.appliedIndex);
    assertEquals(old.contiguousEnd, split.contiguousEnd);
    assertEquals(old.workingPartition, split.workingPartition);
    assertEquals(old.truncatedFrom, split.truncatedFrom);
    assertEquals(old.msgId, split.msgId);
    assertFalse(split.hasSyncNumber());

    AppendEntriesRes full =
        new AppendEntriesRes(
            3, Collections.singletonList(EntryAck.ack(1, 3)), 1, 2, -1, 0, 7, 3, 1);
    assertEquals(3, full.syncNumber);
    assertEquals(1, full.logFirstIndex);
    assertTrue(full.brief().endsWith(" sync=3 firstIndex=1"));
    assertEquals(3, full.withAcks(Collections.emptyList()).syncNumber);
    assertEquals(1, full.withAcks(Collections.emptyList()).logFirstIndex);
  }

  @Test
  public void voteKeySyncNumber() {
    assertEquals(0, VoteKey.raft(3, 9).syncNumber);
    assertEquals(0, VoteKey.triple(3, 9, 2, 5).syncNumber);
    assertEquals(0, new VoteKey(1, 2, 3, 4, 5, 6).syncNumber);
    VoteKey s = VoteKey.sync(4, 3, 9);
    assertEquals(4, s.syncNumber);
    assertEquals(3, s.lastTerm);
    assertEquals(9, s.lastIndex);
    assertTrue(VoteKey.compareSync(s, VoteKey.sync(3, 8, 100)) > 0);
    assertTrue(VoteKey.compareSync(VoteKey.sync(3, 8, 100), s) < 0);
    assertEquals(0, VoteKey.compareSync(s, VoteKey.sync(4, 0, -1))); // logs are not compared
    assertEquals("(3, 0, 0)", s.toString()); // the TRaft rendering is unchanged
    assertEquals("(sync=4, 3, 9)", s.syncString());
  }

  @Test
  public void recoveryMessagesKindsAndIndexes() {
    RecoverTermReq rt = new RecoverTermReq(5, 1, 4, 10, 64, 1024);
    assertEquals("RT", rt.kind());
    assertEquals(10, rt.matchIndex());
    assertTrue(rt.brief().contains("sync=4"));
    RecoverTermRes empty =
        new RecoverTermRes(
            5,
            true,
            3,
            7,
            RecoverTermRes.UNSET,
            RecoverTermRes.UNSET,
            RecoverTermRes.UNSET,
            null,
            null,
            false,
            RecoverTermRes.UNSET);
    assertEquals("RTR", empty.kind());
    assertEquals(-1, empty.matchIndex());
    assertTrue(empty.entries.isEmpty());
    assertEquals(0, empty.committedIndexes.length);
    assertFalse(empty.hasPromise());
    RecoverTermRes page = empty.withEntries(Arrays.asList(entry(11, 4, "a"), entry(12, 4, "b")));
    assertEquals(11, page.matchIndex());
    assertEquals(2, page.entries.size());
    assertEquals(3, page.syncNumber);
    assertEquals(7, page.contiguousEnd);
    UpdateSyncReq us = new UpdateSyncReq(5, 1, 5);
    assertEquals("US", us.kind());
    assertEquals(-1, us.matchIndex());
    UpdateSyncRes usr = new UpdateSyncRes(5, true, 5, UpdateSyncRes.UNSET);
    assertEquals("USR", usr.kind());
    assertFalse(usr.hasContiguousEnd());
    assertEquals("UpdateSyncResp(GRANT,sync=5) t=5", usr.brief());
  }

  @Test
  public void requestMetaExtAndPersistMetaProfileMeta() {
    RequestMeta timed = RequestMeta.timed(5, 9);
    assertNull(timed.ext);
    assertEquals("ts=5..9", timed.toString());
    assertNull(RequestMeta.OPAQUE.ext);
    RequestMeta keyed = timed.withExt(new byte[] {1, 2});
    assertArrayEquals(new byte[] {1, 2}, keyed.ext);
    assertEquals(5, keyed.ts);
    assertEquals(9, keyed.tsMax);
    assertFalse(keyed.barrier);
    assertTrue(RequestMeta.OPAQUE.withExt(new byte[] {3}).isBarrier());
    assertNull(keyed.withExt(null).ext);

    Action.PersistMeta old = new Action.PersistMeta(3, 2, new MetaToken(7));
    assertEquals(0, old.profileMeta.length);
    assertEquals("PersistMeta(term=3,vote=2," + new MetaToken(7) + ")", old.toString());
    Action.PersistMeta withState = new Action.PersistMeta(3, 2, new byte[] {4}, new MetaToken(8));
    assertArrayEquals(new byte[] {4}, withState.profileMeta);
    assertEquals(0, new Action.PersistMeta(3, 2, null, new MetaToken(9)).profileMeta.length);
  }

  @Test
  public void raftKeepsTheDefaults() {
    assertTrue(Capabilities.NONE.isNone());
    assertFalse(Capabilities.NONE.outOfOrderCommit);
    assertFalse(Capabilities.NONE.ackBeyondHoles);
    assertFalse(Capabilities.NONE.datedOverwrite);
    assertFalse(Capabilities.NONE.leaderRecovery);
    assertEquals(0, Capabilities.NONE.commitBitmapBits);
    assertFalse(new Capabilities(true, false, false, false, 64).isNone());
    for (ProtocolProfile p : Arrays.<ProtocolProfile>asList(new RaftProfile())) {
      assertEquals(0, p.encodeProfileMeta().length);
      p.restoreProfileMeta(new byte[] {1, 2, 3}); // ignored
      assertEquals(0, p.encodeProfileMeta().length);
    }
  }
}
