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

/** One acknowledgement inside an {@link AppendEntriesRes}. */
public final class EntryAck {

  public static final long UNSET = Long.MIN_VALUE;

  public final long index;
  public final long entryTerm;
  public final AckStatus status;

  /** NACK: follower working partition; {@link #UNSET} = stale-term NACK. */
  public final long reportedPartition;

  /** REJECT / NEED_FENCE: follower contiguousEnd + 1. */
  public final long hintIndex;

  /** REJECT: term at the follower's contiguousEnd (conflict-term skip); {@link #UNSET} if none. */
  public final long hintTerm;

  /** Fence ACK: follower working partition; {@link #UNSET} for ordinary acks. */
  public final long workingPartition;

  public EntryAck(
      long index,
      long entryTerm,
      AckStatus status,
      long reportedPartition,
      long hintIndex,
      long hintTerm,
      long workingPartition) {
    this.index = index;
    this.entryTerm = entryTerm;
    this.status = status;
    this.reportedPartition = reportedPartition;
    this.hintIndex = hintIndex;
    this.hintTerm = hintTerm;
    this.workingPartition = workingPartition;
  }

  public static EntryAck ack(long index, long entryTerm) {
    return new EntryAck(index, entryTerm, AckStatus.ACK, UNSET, UNSET, UNSET, UNSET);
  }

  public static EntryAck received(long index, long entryTerm) {
    return new EntryAck(index, entryTerm, AckStatus.RECEIVED, UNSET, UNSET, UNSET, UNSET);
  }

  public static EntryAck nack(long index, long entryTerm, long reportedPartition) {
    return new EntryAck(index, entryTerm, AckStatus.NACK, reportedPartition, UNSET, UNSET, UNSET);
  }

  public static EntryAck reject(long index, long entryTerm, long hintIndex, long hintTerm) {
    return new EntryAck(index, entryTerm, AckStatus.REJECT, UNSET, hintIndex, hintTerm, UNSET);
  }

  public static EntryAck needFence(long hintIndex, long hintTerm) {
    return new EntryAck(-1, 0, AckStatus.NEED_FENCE, UNSET, hintIndex, hintTerm, UNSET);
  }

  public static EntryAck fenceAck(long matchIndex, long matchTerm, long workingPartition) {
    return new EntryAck(
        matchIndex, matchTerm, AckStatus.ACK, UNSET, UNSET, UNSET, workingPartition);
  }

  public boolean isFenceAck() {
    return status == AckStatus.ACK && workingPartition != UNSET;
  }

  public boolean hasReportedPartition() {
    return reportedPartition != UNSET;
  }

  public boolean hasHint() {
    return hintIndex != UNSET;
  }

  public String brief() {
    String extra = "";
    if (status == AckStatus.NACK) {
      extra = ",part=" + (hasReportedPartition() ? String.valueOf(reportedPartition) : "None");
    } else if (hasHint()) {
      extra = ",hint=" + hintIndex;
    }
    return status + "(e" + index + ",t" + entryTerm + extra + ")";
  }

  @Override
  public String toString() {
    return brief();
  }
}
