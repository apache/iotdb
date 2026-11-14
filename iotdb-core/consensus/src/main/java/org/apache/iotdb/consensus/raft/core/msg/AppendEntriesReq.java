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

import org.apache.iotdb.consensus.raft.core.log.LogEntry;

import java.util.Collections;
import java.util.List;

/**
 * AppendEntries request. Empty {@code entries} = heartbeat / commit notification. {@code ext}
 * carries the optional out-of-order extension (ParallelRaft commit bitmap / recovery push); Raft
 * and TRaft leave it null.
 */
public final class AppendEntriesReq extends Message {

  public final int leaderId;

  /** Raft prefix check; TRaft partition mode: -1; TRaft fence probe: the probe point. */
  public final long prevLogIndex;

  public final long prevLogTerm;
  public final List<LogEntry> entries;
  public final long leaderCommit;

  /** Identity of the committed entry named by {@code leaderCommit} (Python CN.entry_term). */
  public final long leaderCommitTerm;

  /** TRaft: the entries belong to a partition beyond the follower's estimated one. */
  public final boolean speculative;

  /** TRaft: leader-term fence probe. */
  public final boolean fence;

  public final long msgId;
  public final long schedRound;

  /** Out-of-order extension; null = none (Raft / TRaft). */
  public final AppendExt ext;

  public AppendEntriesReq(
      long term,
      int leaderId,
      long prevLogIndex,
      long prevLogTerm,
      List<LogEntry> entries,
      long leaderCommit,
      long leaderCommitTerm,
      boolean speculative,
      boolean fence,
      long msgId,
      long schedRound) {
    this(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit,
        leaderCommitTerm,
        speculative,
        fence,
        msgId,
        schedRound,
        null);
  }

  public AppendEntriesReq(
      long term,
      int leaderId,
      long prevLogIndex,
      long prevLogTerm,
      List<LogEntry> entries,
      long leaderCommit,
      long leaderCommitTerm,
      boolean speculative,
      boolean fence,
      long msgId,
      long schedRound,
      AppendExt ext) {
    super(term);
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries == null ? Collections.emptyList() : entries;
    this.leaderCommit = leaderCommit;
    this.leaderCommitTerm = leaderCommitTerm;
    this.speculative = speculative;
    this.fence = fence;
    this.msgId = msgId;
    this.schedRound = schedRound;
    this.ext = ext;
  }

  public AppendEntriesReq withEntries(List<LogEntry> newEntries, boolean spec) {
    return new AppendEntriesReq(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        newEntries,
        leaderCommit,
        leaderCommitTerm,
        spec,
        fence,
        msgId,
        schedRound,
        ext);
  }

  public AppendEntriesReq withMsgId(long newMsgId) {
    return new AppendEntriesReq(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit,
        leaderCommitTerm,
        speculative,
        fence,
        newMsgId,
        schedRound,
        ext);
  }

  /** The same request with {@code newExt} (null removes the extension). */
  public AppendEntriesReq withExt(AppendExt newExt) {
    return new AppendEntriesReq(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit,
        leaderCommitTerm,
        speculative,
        fence,
        msgId,
        schedRound,
        newExt);
  }

  /** A recovery / catch-up push (harness kind PUSH). */
  public boolean isPush() {
    return ext != null && ext.recovery;
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  @Override
  public String kind() {
    if (fence) {
      return "FENCE";
    }
    if (isPush()) {
      return "PUSH";
    }
    return entries.isEmpty() ? "CN" : "AE";
  }

  @Override
  public long matchIndex() {
    return entries.isEmpty() ? -1 : entries.get(0).index;
  }

  @Override
  public String brief() {
    String base = baseBrief();
    return ext == null ? base : base + " " + ext.brief();
  }

  private String baseBrief() {
    if (fence) {
      return "Fence(prev=" + prevLogIndex + ",t" + prevLogTerm + ") term=" + term;
    }
    if (entries.isEmpty() && !isPush()) {
      return "CommitNotify(commit=" + leaderCommit + ",t=" + term + ")";
    }
    StringBuilder sb = new StringBuilder(isPush() ? "Push(" : "AE(");
    for (int i = 0; i < entries.size(); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(entries.get(i));
    }
    if (speculative) {
      sb.append(" spec");
    }
    return sb.append(") term=").append(term).toString();
  }
}
