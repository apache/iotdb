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

package org.apache.iotdb.consensus.raft.core.replication;

import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

/** Strategy point: the follower-side acceptance gate for one incoming entry. */
public interface AcceptGate {

  final class Decision {
    public static final Decision ACCEPT = new Decision(null, EntryAck.UNSET, EntryAck.UNSET);

    public final AckStatus reject; // null = accept
    public final long reportedPartition;
    public final long hintIndex;

    private Decision(AckStatus reject, long reportedPartition, long hintIndex) {
      this.reject = reject;
      this.reportedPartition = reportedPartition;
      this.hintIndex = hintIndex;
    }

    public static Decision nack(long reportedPartition) {
      return new Decision(AckStatus.NACK, reportedPartition, EntryAck.UNSET);
    }

    public boolean accepted() {
      return reject == null;
    }
  }

  /**
   * True for Raft: the receiver performs the (prevLogIndex, prevLogTerm) prefix check on every
   * AppendEntries and answers with a single ack per request. False for TRaft: the prefix check only
   * runs for fence probes and acks are per entry.
   */
  boolean prefixCheck();

  Decision check(LogEntry entry, AppendEntriesReq req, LogView log);

  /** Follower working partition for responses; -1 when not partitioned. */
  default long workingPartition() {
    return -1;
  }

  /** Every entry of every AppendEntries passes here before deduplication (TRaft: AE counters). */
  default void observe(LogEntry entry) {}

  /**
   * The receiver finished one AppendEntries (stored / repaired / duplicate): refresh derived state.
   */
  default void refresh() {}

  /** The log was compacted behind (or replaced by) a snapshot carrying this protocol summary. */
  default void onSnapshot(SnapshotMetaExt ext) {}

  // ------------------------------------------------------- out-of-order protocols (ParallelRaft)
  // Consulted only under the capability {@code ackBeyondHoles}, except syncNumber and
  // onSnapshotInstalled, whose defaults keep Raft / TRaft unchanged.

  /**
   * Request-level gate, before any entry is looked at: a refused request is answered with one NACK
   * per entry (an empty answer if it has none). Default: accept.
   */
  default Decision checkRequest(AppendEntriesReq req, LogView log) {
    return Decision.ACCEPT;
  }

  /**
   * A recovery push carries the promise {@code (ext.syncTerm, req.term, ext.truncateFrom - 1)}.
   * Return true if it differs from the promise held (and the gate adopted it): the kernel then
   * persists the profile state first and replays the request once that is durable. Default: no
   * promises.
   */
  default boolean onPushPromise(AppendEntriesReq req) {
    return false;
  }

  /**
   * Under the capability {@code datedOverwrite} too, for an incoming copy of a slot held with the
   * same term, both copies dated: true if the incoming copy ({@code incomingDate}) replaces the
   * held one ({@code heldDate}), false if it is a duplicate. Default: iff its date is larger
   * (ParallelRaft's overwrite rule).
   */
  default boolean overwrites(long incomingDate, long heldDate) {
    return incomingDate > heldDate;
  }

  /** The follower's sync number for responses; {@link AppendEntriesRes#UNSET} if none. */
  default long syncNumber() {
    return AppendEntriesRes.UNSET;
  }

  /**
   * A snapshot ending at ({@code index}, {@code term}) was installed. Return true if the gate's
   * persistent state changed (the kernel then persists the profile state). Default: nothing.
   */
  default boolean onSnapshotInstalled(long index, long term) {
    return false;
  }

  /**
   * Whether installing a snapshot ending at ({@code index}, {@code term}) keeps the slots beyond it
   * (asked, under the capability {@code outOfOrderCommit}, before the install changes anything).
   * Default: no, the log restarts empty after the snapshot.
   */
  default boolean keepsTailOnInstall(long index, long term) {
    return false;
  }
}
