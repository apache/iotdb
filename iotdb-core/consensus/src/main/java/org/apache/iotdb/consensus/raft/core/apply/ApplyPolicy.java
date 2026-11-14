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

package org.apache.iotdb.consensus.raft.core.apply;

import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;

import java.util.List;
import java.util.NavigableSet;

/**
 * Strategy point: what a node knows to be committed and which committed entries it applies, in
 * which order. {@code null} (the default of {@code ProtocolProfile.applyPolicy()}) keeps the
 * kernel's built-in prefix application: the leader applies every committed range in index order and
 * a follower applies up to {@code min(leaderCommit, contiguousEnd)}. An out-of-order protocol
 * (ParallelRaft) supplies its own policy; the kernel then takes {@code lastApplied} from {@link
 * #appliedFrontier()} and its commit views from {@link #commitFrontier()} / {@link
 * #maxCommitted()}, and no longer learns follower commit information itself.
 *
 * <p>Every call happens on the kernel's event thread, inside an event: the {@code Apply} actions
 * the policy emits through {@link ApplyContext#emitApply} join that event's actions. The only
 * exception is the kernel's construction, which calls {@link #reset} (it must not emit: there is no
 * event to carry the actions yet) and reads the frontiers once. Readers on other threads (status
 * RPC, metrics) get copies of {@link #commitFrontier()} and {@link #maxCommitted()} the kernel
 * publishes at the end of every event, never the policy itself, so a policy needs no
 * synchronization.
 */
public interface ApplyPolicy {

  /**
   * Start over from {@code appliedFrontier}: every index up to it counts as applied and committed,
   * everything beyond it is forgotten. Called when the kernel is constructed (start / restart) and
   * after a snapshot was installed.
   */
  void reset(long appliedFrontier, ApplyContext ctx);

  /** Leader: these indexes were just committed (ascending, each reported once). */
  void onCommitted(List<Long> indexes, ApplyContext ctx);

  /**
   * Follower: an AppendEntries of the current leader carries commit information ({@code
   * leaderCommit} and, for out-of-order protocols, {@code ext.commitBase / commitBits}). Not called
   * for recovery pushes, whose commit information is meaningless.
   */
  void onCommitInfo(AppendEntriesReq req, ApplyContext ctx);

  /**
   * The log or the protocol state changed (entries stored, overwritten, truncated or made durable,
   * log compacted, sync number raised): re-evaluate what can be applied.
   */
  void onLogChanged(ApplyContext ctx);

  /**
   * Follower: an AppendEntries was handled (its entries stored and durable, or answered as
   * duplicates / refusals): the log changed and, unless it is a recovery push, it carries commit
   * information. One call per request, so that a policy evaluates once; the default does what the
   * two calls did, {@link #onLogChanged} then (not for a push) {@link #onCommitInfo}.
   */
  default void onAppendEntries(AppendEntriesReq req, ApplyContext ctx) {
    onLogChanged(ctx);
    if (!req.isPush()) {
      onCommitInfo(req, ctx);
    }
  }

  /** The applier finished {@code index}. */
  void onApplied(long index, ApplyContext ctx);

  /** Largest k such that every index up to k is applied (the kernel's {@code lastApplied}). */
  long appliedFrontier();

  /** Largest k such that every index up to k is known to be committed. */
  long commitFrontier();

  /** Largest index known to be committed ({@link #commitFrontier()} if none beyond it). */
  long maxCommitted();

  boolean isCommitted(long index);

  /** True if some index at or beyond {@code index} is applied or its Apply was already issued. */
  boolean appliedAtOrAfter(long index);

  /** The committed indexes beyond {@link #commitFrontier()} (the new leader's initial set). */
  NavigableSet<Long> committedBeyondFrontier();

  /**
   * Status view: committed entries beyond {@link #appliedFrontier()} not applied yet (issued but
   * unfinished, waiting for a predecessor, or not held yet); -1 if the policy does not tell.
   */
  default long pendingApply() {
    return -1;
  }
}
