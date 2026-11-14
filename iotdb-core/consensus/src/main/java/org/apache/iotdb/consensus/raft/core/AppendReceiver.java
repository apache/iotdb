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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.AppendExt;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.replication.AcceptGate;

import java.util.ArrayList;
import java.util.List;

/**
 * Follower side of AppendEntries, shared by Raft and TRaft: term check, adopt, prefix / fence
 * check, per-slot term conflict (suffix repair), idempotent duplicates, the protocol's accept gate,
 * two-phase persistence, Ack burst / RECEIVED, identity-checked commit learning and apply.
 *
 * <p>Under the capability {@code ackBeyondHoles} (ParallelRaft) a request first passes the gate's
 * request-level check and, for a recovery push, its promise and truncation; entries then take the
 * gate-first path (every entry answered on its own, dated overwrite) and every durable entry is
 * acked, also beyond a hole. With an apply policy, commit information goes to it instead of the
 * kernel's prefix learning. With every capability off and no apply policy (Raft / TRaft) the
 * original paths run unchanged.
 */
final class AppendReceiver {

  /** State of one AppendEntries between Persist and Durable. */
  static final class Pending {
    final int from;
    final ReplyHandle handle;
    final AppendEntriesReq req;
    final List<EntryAck> acks = new ArrayList<>();
    final List<LogEntry> stored = new ArrayList<>();

    /**
     * Duplicates still being persisted by an earlier write: the prefix-checked and ackBeyondHoles
     * paths answer this request after a FIFO write barrier, even if the first RPC lost its reply.
     */
    final List<LogEntry> pendingDups = new ArrayList<>();

    /** For each stored entry, the log's sequence number of the put that stored it. */
    final List<Long> storedSeq = new ArrayList<>();

    long truncatedFrom = -1;
    long oldEnd;
    long maxStored = -1;
    boolean fenceMatch;

    Pending(int from, ReplyHandle handle, AppendEntriesReq req) {
      this.from = from;
      this.handle = handle;
      this.req = req;
    }
  }

  private final RaftCore core;

  AppendReceiver(RaftCore core) {
    this.core = core;
  }

  void onAppendEntries(AppendEntriesReq req, int from, ReplyHandle h) {
    RaftState st = core.st;
    LogIndex log = core.log;
    AcceptGate gate = core.gate;
    boolean prefix = gate.prefixCheck();

    // 1. stale leader
    if (req.term < st.currentTerm) {
      List<EntryAck> acks = new ArrayList<>();
      if (!req.entries.isEmpty()) {
        for (LogEntry e : req.entries) {
          acks.add(
              prefix
                  ? EntryAck.reject(
                      e.index, e.term, log.contiguousEnd() + 1, termAtOr0(log.contiguousEnd()))
                  : EntryAck.nack(e.index, e.term, EntryAck.UNSET));
        }
      } else if (!core.policy.replyToHeartbeat && !req.isPush()) {
        return; // Python: a stale CommitNotify is ignored silently
      } else {
        acks.add(
            EntryAck.reject(
                req.prevLogIndex,
                req.prevLogTerm,
                log.contiguousEnd() + 1,
                termAtOr0(log.contiguousEnd())));
      }
      core.emit(new Action.Reply(h, response(acks, -1)));
      return;
    }
    // 2. newer leader: adopt (durably) and resume this very message afterwards
    if (req.term > st.currentTerm) {
      core.adoptTerm(req.term, new Event.Inbound(from, req, h));
      return;
    }
    if (st.role == Role.LEADER) {
      return; // same term, two leaders: impossible under Election Safety; ignore defensively
    }
    st.role = Role.FOLLOWER;
    if (st.leaderId != from) {
      st.leaderId = from;
      core.emit(new Action.RoleChanged(Role.FOLLOWER, from, st.currentTerm));
    }
    core.armElectionTimer();

    Pending pa = new Pending(from, h, req);
    pa.oldEnd = log.contiguousEnd();

    // 2b. out-of-order protocols: request-level gate, recovery push promise and truncation. The
    // kernel refuses ackBeyondHoles together with a prefix check or a leader fence, so step 3
    // never answers such a request (which would leave this in-memory cut unpersisted).
    if (core.caps.ackBeyondHoles && !admitRequest(req, from, h, pa)) {
      return;
    }

    // 3. prefix (Raft) / fence probe (TRaft)
    boolean fenceProbe = !prefix && req.fence && core.policy.leaderFence;
    if (prefix || fenceProbe) {
      long prev = req.prevLogIndex;
      if (prev >= log.firstIndex()) {
        boolean matched = log.has(prev) && log.termAt(prev) == req.prevLogTerm;
        if (fenceProbe && prev > log.contiguousEnd()) {
          matched = false; // the match point must lie inside the contiguous frontier
        }
        if (!matched) {
          long hint = log.contiguousEnd() + 1;
          core.record(
              ListenerEvent.of(fenceProbe ? "fence_reject" : "prefix_reject", core.self)
                  .with("prev", prev)
                  .with("hint", hint)
                  .build());
          core.emit(
              new Action.Reply(
                  h,
                  response(
                      java.util.Collections.singletonList(
                          EntryAck.reject(
                              prev, req.prevLogTerm, hint, termAtOr0(log.contiguousEnd()))),
                      -1)));
          return;
        }
      }
      if (fenceProbe) {
        if (log.lastIndex() > prev) {
          log.removeFrom(prev + 1);
          core.onLogTruncated(prev + 1);
          pa.truncatedFrom = prev + 1;
          core.record(ListenerEvent.of("fence_truncate", core.self).with("from", prev + 1).build());
        }
        st.fencedTerm = req.term;
        pa.fenceMatch = true;
        core.record(
            ListenerEvent.of("fence_match", core.self)
                .with("prev", prev)
                .with("term", req.term)
                .build());
        if (pa.truncatedFrom >= 0) {
          core.persist(new ArrayList<>(), pa.truncatedFrom, pa);
        } else {
          finish(pa);
        }
        return;
      }
    } else if (!prefix
        && core.policy.leaderFence
        && !req.entries.isEmpty()
        && st.fencedTerm != req.term) {
      core.record(ListenerEvent.of("need_fence", core.self).with("term", req.term).build());
      core.emit(
          new Action.Reply(
              h,
              response(
                  java.util.Collections.singletonList(
                      EntryAck.needFence(log.contiguousEnd() + 1, termAtOr0(log.contiguousEnd()))),
                  -1)));
      return;
    }

    // 4. entries (the out-of-order protocols take the gate-first path instead)
    List<LogEntry> entries = req.entries;
    if (core.caps.ackBeyondHoles) {
      storeGateFirst(req, pa);
      entries = java.util.Collections.emptyList();
    }
    for (LogEntry e : entries) {
      gate.observe(e);
      if (e.index < log.firstIndex()) {
        // a stale retransmission below the compaction point: the slot was applied here, hence
        // committed, and the leader of this term holds the same committed entry there. A durable
        // duplicate (it is in the snapshot): ACK, as the out-of-order path answers it.
        core.record(
            ListenerEvent.of("dup_receive", core.self)
                .with("index", e.index)
                .with("status", AckStatus.ACK.name())
                .with("reason", "compacted")
                .build());
        pa.acks.add(EntryAck.ack(e.index, e.term));
        recordAckEmit(e.index, e.term);
        continue;
      }
      if (log.has(e.index) && log.termAt(e.index) != e.term) {
        if (core.mut.has("ignore_term_conflict")) {
          // BUG (C-6): conflict treated as intra-partition acceptance
          pa.acks.add(EntryAck.ack(e.index, e.term));
          core.learnCommit(Math.min(req.leaderCommit, e.index));
          continue;
        }
        long oldTerm = log.termAt(e.index);
        Invariants.check(
            core, e.index > st.lastApplied, "INV-S1", "suffix repair of applied slot " + e.index);
        log.removeFrom(e.index);
        core.onLogTruncated(e.index);
        gate.refresh();
        pa.oldEnd = Math.min(pa.oldEnd, e.index - 1); // the burst starts where the repair cut
        pa.truncatedFrom = pa.truncatedFrom < 0 ? e.index : Math.min(pa.truncatedFrom, e.index);
        core.record(
            ListenerEvent.of("suffix_repair", core.self)
                .with("slot", e.index)
                .with("old_term", oldTerm)
                .with("new_term", e.term)
                .with("applied", st.lastApplied)
                .build());
      }
      if (log.has(e.index)) {
        // duplicate delivery: idempotent, respond as before (C-7). Either answer claims a durable
        // copy (ACK: the contiguous frontier through it). A reply that waits for this request's own
        // Persist (it stored or cut something before this entry) goes out once every earlier write
        // is durable too (the log writer is FIFO): the frontier in the log counts. A reply sent at
        // once counts the durable frontier only. A copy still being written cannot be acknowledged
        // immediately; the prefix path below defers this RPC behind its own write barrier.
        boolean waitsForPersist = !pa.stored.isEmpty() || pa.truncatedFrom >= 0;
        AckStatus s;
        if (waitsForPersist) {
          s = e.index <= log.contiguousEnd() ? AckStatus.ACK : AckStatus.RECEIVED;
        } else if (log.isDurable(e.index)) {
          s = e.index <= log.durableContiguousEnd() ? AckStatus.ACK : AckStatus.RECEIVED;
        } else {
          s = null;
        }
        core.record(
            ListenerEvent.of("dup_receive", core.self)
                .with("index", e.index)
                .with("status", s == null ? "PENDING" : s.name())
                .build());
        if (s == AckStatus.ACK) {
          pa.acks.add(EntryAck.ack(e.index, e.term));
          recordAckEmit(e.index, e.term);
        } else if (s == AckStatus.RECEIVED) {
          pa.acks.add(EntryAck.received(e.index, e.term));
        } else if (prefix) {
          // The original RPC may have timed out before fsync. Its eventual reply cannot release
          // this retransmission's in-flight batch, so this RPC must wait and acknowledge for
          // itself.
          pa.pendingDups.add(e);
        }
        core.learnCommit(Math.min(req.leaderCommit, e.index));
        gate.refresh(); // no-op for a correct implementation; C-8b mutation trap
        continue;
      }
      AcceptGate.Decision d = gate.check(e, req, log);
      if (!d.accepted()) {
        core.record(
            ListenerEvent.of("nack", core.self)
                .with("index", e.index)
                .with("reported", d.reportedPartition)
                .build());
        pa.acks.add(EntryAck.nack(e.index, e.term, d.reportedPartition));
        continue;
      }
      core.faults.at(CrashPoint.FOLLOWER_PRE_STORE);
      log.put(e, false);
      pa.stored.add(e);
      pa.storedSeq.add(log.writeSeqAt(e.index));
      if (e.type == org.apache.iotdb.consensus.raft.core.log.EntryType.CONFIG) {
        core.onConfigEntryStored(e); // configuration takes effect on append (Raft §4.1)
      }
      pa.maxStored = Math.max(pa.maxStored, e.index);
    }

    if (pa.stored.isEmpty() && pa.truncatedFrom < 0 && !pa.pendingDups.isEmpty()) {
      // only duplicates of copies an earlier write still persists: the reply waits for a write
      // that follows it (the log writer is FIFO), then acks them. Answered at once without their
      // acks, a retransmission whose first copy's reply went to a timed-out RPC would never be
      // acked, and the leader would keep the entry in flight.
      core.persist(new ArrayList<>(), -1, pa);
      return;
    }
    if (pa.stored.isEmpty() && pa.truncatedFrom < 0) {
      if (req.entries.isEmpty()) {
        learnFromEmpty(req, prefix);
        if (!core.policy.replyToHeartbeat && !req.isPush()) {
          return; // Python: CommitNotify has no reply
        }
        if (prefix) {
          // A reconnect probe can overtake the fsync of a previous, timed-out AppendEntries.
          // Matching an in-memory prefix proves identity, not durability. Only acknowledge
          // the durable frontier; a later heartbeat can confirm the remainder after fsync.
          long acknowledged = Math.min(req.prevLogIndex, log.durableContiguousEnd());
          long term = acknowledged == req.prevLogIndex ? req.prevLogTerm : termAtOr0(acknowledged);
          pa.acks.add(EntryAck.ack(acknowledged, term));
        }
      } else if (core.applyPolicy != null && !req.isPush()) {
        core.applyPolicy.onCommitInfo(req, core); // duplicates / NACKs only: still learn commits
      }
      core.emit(new Action.Reply(h, response(pa.acks, pa.truncatedFrom)));
      return;
    }
    core.persist(pa.stored, pa.truncatedFrom, pa);
  }

  /**
   * Step 2b ({@code ackBeyondHoles}): the gate may refuse the whole request (ParallelRaft: a push
   * of another sync term), answered with one NACK per entry. A recovery push carrying {@code
   * truncateFrom} first gets its promise durable (the request is replayed once it is), then cuts
   * every slot at or beyond {@code truncateFrom}. Returns false if the request was answered or
   * deferred.
   */
  private boolean admitRequest(AppendEntriesReq req, int from, ReplyHandle h, Pending pa) {
    LogIndex log = core.log;
    AcceptGate gate = core.gate;
    AcceptGate.Decision d = gate.checkRequest(req, log);
    if (!d.accepted()) {
      List<EntryAck> acks = new ArrayList<>();
      for (LogEntry e : req.entries) {
        acks.add(EntryAck.nack(e.index, e.term, d.reportedPartition));
      }
      core.record(
          ListenerEvent.of("request_nack", core.self)
              .with("kind", req.kind())
              .with("entries", req.entries.size())
              .with("sync", gate.syncNumber())
              .build());
      core.emit(new Action.Reply(h, response(acks, -1)));
      return false;
    }
    AppendExt ext = req.ext;
    if (ext == null || !ext.recovery || ext.truncateFrom < 0) {
      return true;
    }
    if (gate.onPushPromise(req)) {
      Event.Inbound resume = new Event.Inbound(from, req, h);
      core.record(
          ListenerEvent.of("push_promise", core.self)
              .with("sync", ext.syncTerm)
              .with("ballot", req.term)
              .with("end", ext.truncateFrom - 1)
              .build());
      core.persistProfileMeta(() -> core.onInbound(resume));
      return false;
    }
    long cut = ext.truncateFrom;
    if (cut <= log.lastIndex()) {
      Invariants.check(
          core,
          !core.appliedAtOrAfter(cut),
          "INV-S1",
          "push truncation from " + cut + " cuts an applied entry");
      List<Long> dropped = log.removeFrom(cut);
      core.onLogTruncated(cut);
      gate.refresh();
      pa.oldEnd = Math.min(pa.oldEnd, cut - 1);
      pa.truncatedFrom = cut;
      core.record(
          ListenerEvent.of("push_truncate", core.self)
              .with("from", cut)
              .with("dropped", dropped)
              .build());
    }
    return true;
  }

  /**
   * Step 4 ({@code ackBeyondHoles}), gate-first, every entry answered on its own: below the first
   * index = a compacted duplicate (ACK); refused by the gate = NACK; held with another term =
   * suffix repair, then store; held with the same term = overwrite if {@code datedOverwrite} and
   * the gate lets the new date win (by default: a larger one), else a duplicate (ACK once durable);
   * not held = store.
   */
  private void storeGateFirst(AppendEntriesReq req, Pending pa) {
    LogIndex log = core.log;
    AcceptGate gate = core.gate;
    for (LogEntry e : req.entries) {
      gate.observe(e);
      if (e.index < log.firstIndex()) {
        core.record(
            ListenerEvent.of("dup_receive", core.self)
                .with("index", e.index)
                .with("status", AckStatus.ACK.name())
                .with("reason", "compacted")
                .build());
        pa.acks.add(EntryAck.ack(e.index, e.term));
        recordAckEmit(e.index, e.term);
        continue;
      }
      AcceptGate.Decision d = gate.check(e, req, log);
      if (!d.accepted()) {
        core.record(
            ListenerEvent.of("nack", core.self)
                .with("index", e.index)
                .with("reported", d.reportedPartition)
                .build());
        pa.acks.add(EntryAck.nack(e.index, e.term, d.reportedPartition));
        continue;
      }
      boolean overwrite = false;
      if (log.has(e.index)) {
        long oldTerm = log.termAt(e.index);
        if (oldTerm != e.term) {
          Invariants.check(
              core,
              !core.appliedAtOrAfter(e.index),
              "INV-S1",
              "suffix repair of applied slot " + e.index);
          log.removeFrom(e.index);
          core.onLogTruncated(e.index);
          gate.refresh();
          pa.oldEnd = Math.min(pa.oldEnd, e.index - 1);
          pa.truncatedFrom = pa.truncatedFrom < 0 ? e.index : Math.min(pa.truncatedFrom, e.index);
          core.record(
              ListenerEvent.of("suffix_repair", core.self)
                  .with("slot", e.index)
                  .with("old_term", oldTerm)
                  .with("new_term", e.term)
                  .with("applied", core.st.lastApplied)
                  .build());
        } else if (core.caps.datedOverwrite && datedOverwrite(gate, e.meta, log.metaAt(e.index))) {
          overwrite = true;
          core.record(
              ListenerEvent.of("dated_overwrite", core.self)
                  .with("index", e.index)
                  .with("term", e.term)
                  .with("old_date", ((EntryMeta.Dated) log.metaAt(e.index)).date())
                  .with("new_date", ((EntryMeta.Dated) e.meta).date())
                  .build());
        } else {
          // duplicate: ACK once durable; a copy still being persisted is acked by this reply too,
          // which then waits for a write of its own (finish)
          boolean durable = log.isDurable(e.index);
          core.record(
              ListenerEvent.of("dup_receive", core.self)
                  .with("index", e.index)
                  .with("status", durable ? AckStatus.ACK.name() : "PENDING")
                  .build());
          if (durable) {
            pa.acks.add(EntryAck.ack(e.index, e.term));
            recordAckEmit(e.index, e.term);
          } else {
            pa.pendingDups.add(e);
          }
          continue;
        }
      }
      core.faults.at(CrashPoint.FOLLOWER_PRE_STORE);
      if (overwrite) {
        core.replaceSlot(e);
      } else {
        log.put(e, false);
        if (e.type == EntryType.CONFIG) {
          core.onConfigEntryStored(e); // configuration takes effect on append (Raft §4.1)
        }
      }
      pa.stored.add(e);
      pa.storedSeq.add(log.writeSeqAt(e.index));
      pa.maxStored = Math.max(pa.maxStored, e.index);
    }
  }

  /**
   * True if both metadata carry a date and the gate lets the incoming copy replace the held one (by
   * default: the larger date wins, ParallelRaft's overwrite rule).
   */
  private static boolean datedOverwrite(AcceptGate gate, EntryMeta incoming, EntryMeta held) {
    return incoming instanceof EntryMeta.Dated
        && held instanceof EntryMeta.Dated
        && gate.overwrites(((EntryMeta.Dated) incoming).date(), ((EntryMeta.Dated) held).date());
  }

  /** Called by the kernel when the Persist of {@code pa} is durable. */
  void onDurable(Pending pa) {
    core.faults.at(CrashPoint.FOLLOWER_POST_FSYNC);
    for (int k = 0; k < pa.stored.size(); k++) {
      LogEntry e = pa.stored.get(k);
      if (core.caps.ackBeyondHoles && !core.holds(e)) {
        continue; // truncated or replaced meanwhile: the replacement's own Persist marks it
      }
      if (core.log.writeSeqAt(e.index) != pa.storedSeq.get(k)) {
        // cut and stored again since (e.g. a repeated fence truncation, then a retransmission):
        // this write is not the copy the slot holds, whose own Persist is still pending
        continue;
      }
      core.log.markDurable(e.index);
      core.record(
          ListenerEvent.of("store", core.self)
              .with("index", e.index)
              .with("term", e.term)
              .with("durable", true)
              .build());
    }
    finish(pa);
  }

  private void finish(Pending pa) {
    LogIndex log = core.log;
    AppendEntriesReq req = pa.req;
    if (req.term != core.st.currentTerm || core.st.leaderId != pa.from) {
      // I/O may finish after a newer term was adopted. Never let the old request's commit
      // advertisement or acknowledgement run against the new leadership's log.
      core.emit(new Action.Reply(pa.handle, response(java.util.Collections.emptyList(), -1)));
      return;
    }
    if (pa.fenceMatch) {
      pa.acks.add(
          EntryAck.fenceAck(req.prevLogIndex, req.prevLogTerm, core.gate.workingPartition()));
      learnFromEmpty(req, false);
      core.emit(new Action.Reply(pa.handle, response(pa.acks, pa.truncatedFrom)));
      return;
    }
    // the durable frontier: slots stored by later requests may already extend the frontier in the
    // log, but their Persist is still pending (equal to the frontier in the log when each write
    // is durable before the next message arrives)
    long newEnd = log.durableContiguousEnd();
    if (core.gate.prefixCheck()) {
      LogEntry last = pa.stored.isEmpty() ? null : pa.stored.get(pa.stored.size() - 1);
      for (LogEntry entry : pa.pendingDups) {
        // A completed checkpoint is also a durable copy, as in the compacted-duplicate path above.
        if (entry.index <= newEnd
            && (entry.index < log.firstIndex() || core.holds(entry))
            && (last == null || entry.index > last.index)) {
          last = entry;
        }
      }
      if (last != null && last.index <= newEnd) {
        pa.acks.add(EntryAck.ack(last.index, last.term));
        recordAckEmit(last.index, last.term);
      }
    } else if (core.caps.ackBeyondHoles) {
      // every durable entry is acked, also beyond a hole (ParallelRaft); the ack_beyond_hole
      // logic below, restricted to the copies still held
      for (LogEntry e : pa.stored) {
        if (core.holds(e) && log.isDurable(e.index)) {
          pa.acks.add(EntryAck.ack(e.index, e.term));
          recordAckEmit(e.index, e.term);
        }
      }
      for (LogEntry e : pa.pendingDups) {
        // the earlier write persisting it preceded this request's own: durable now, unless the
        // slot was cut or replaced meanwhile
        if (log.has(e.index) && log.termAt(e.index) == e.term && log.isDurable(e.index)) {
          pa.acks.add(EntryAck.ack(e.index, e.term));
          recordAckEmit(e.index, e.term);
        }
      }
    } else if (core.mut.has("ack_beyond_hole")) {
      // BUG (C-2): every stored entry acked immediately
      for (LogEntry e : pa.stored) {
        pa.acks.add(EntryAck.ack(e.index, e.term));
        recordAckEmit(e.index, e.term);
      }
    } else {
      List<Long> covered = new ArrayList<>();
      boolean advanced = false;
      for (LogEntry e : pa.stored) {
        if (e.index <= newEnd) {
          advanced = true;
        }
      }
      if (advanced) {
        if (core.mut.has("ack_only_filled")) {
          // BUG (C-3): only the hole-filling entry itself is acked
          for (LogEntry e : pa.stored) {
            if (e.index <= newEnd) {
              covered.add(e.index);
            }
          }
        } else {
          for (long i = pa.oldEnd + 1; i <= newEnd; i++) {
            covered.add(i);
          }
        }
        core.record(
            ListenerEvent.of("frontier", core.self)
                .with("frontier_end", newEnd)
                .with("covered", covered)
                .build());
        for (long i : covered) {
          pa.acks.add(EntryAck.ack(i, log.termAt(i)));
          recordAckEmit(i, log.termAt(i));
        }
      }
      for (LogEntry e : pa.stored) {
        if (e.index > newEnd && log.isDurable(e.index)) {
          pa.acks.add(EntryAck.received(e.index, e.term));
        }
      }
    }
    if (core.applyPolicy != null) {
      core.applyPolicy.onAppendEntries(req, core); // a push's commit information is ignored
    } else if (pa.maxStored >= 0) {
      // Raft's min(leaderCommit, index of this entry) rule (F-3)
      core.learnCommit(Math.min(req.leaderCommit, pa.maxStored));
    } else if (req.entries.isEmpty()) {
      learnFromEmpty(req, core.gate.prefixCheck());
    }
    core.emit(new Action.Reply(pa.handle, response(pa.acks, pa.truncatedFrom)));
    core.gate.refresh();
    core.faults.at(CrashPoint.FOLLOWER_POST_RESPOND);
  }

  private void learnFromEmpty(AppendEntriesReq req, boolean prefix) {
    if (core.applyPolicy != null) {
      if (!req.isPush()) {
        core.applyPolicy.onCommitInfo(req, core);
      }
      return;
    }
    LogIndex log = core.log;
    long c = req.leaderCommit;
    if (c < 0) {
      return;
    }
    if (prefix && req.prevLogIndex >= 0) {
      core.learnCommit(Math.min(c, req.prevLogIndex));
      return;
    }
    if (c < log.firstIndex()) {
      core.learnCommit(c);
    } else if (log.has(c) && log.termAt(c) == req.leaderCommitTerm) {
      core.learnCommit(c); // identity-checked commit learning (PY on_commit_notify)
    }
  }

  private void recordAckEmit(long index, long term) {
    Invariants.check(
        core,
        index <= core.log.contiguousEnd() || !core.mut.isEmpty() || core.caps.ackBeyondHoles,
        "INV-P5",
        "Ack for e" + index + " beyond the contiguous frontier " + core.log.contiguousEnd());
    core.record(
        ListenerEvent.of("ack_emit", core.self)
            .with("index", index)
            .with("entry_term", term)
            .with("frontier_end", core.log.contiguousEnd())
            .build());
  }

  private AppendEntriesRes response(List<EntryAck> acks, long truncatedFrom) {
    return new AppendEntriesRes(
        core.st.currentTerm,
        acks,
        core.st.lastApplied,
        core.log.contiguousEnd(),
        core.gate.workingPartition(),
        truncatedFrom >= 0 ? truncatedFrom : AppendEntriesRes.UNSET,
        0,
        core.gate.syncNumber(),
        core.caps.ackBeyondHoles ? core.log.firstIndex() : AppendEntriesRes.UNSET);
  }

  private long termAtOr0(long index) {
    LogIndex log = core.log;
    if (index < log.firstIndex()) {
      return index == log.firstIndex() - 1 ? log.snapshotTerm() : 0;
    }
    return log.has(index) ? log.termAt(index) : 0;
  }
}
