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

import java.util.Collections;
import java.util.List;

/** AppendEntries response: one {@link EntryAck} per acknowledged entry (Raft: exactly one). */
public final class AppendEntriesRes extends Message {

  public static final long UNSET = Long.MIN_VALUE;

  public final List<EntryAck> acks;
  public final long appliedIndex;
  public final long contiguousEnd;
  public final long workingPartition;

  /** Smallest index removed by suffix repair / fence truncation while handling the request. */
  public final long truncatedFrom;

  public final long msgId;

  /** ParallelRaft: the follower's sync number; {@link #UNSET} for Raft / TRaft. */
  public final long syncNumber;

  /** ParallelRaft: the follower's first log index (after compaction); {@link #UNSET} if absent. */
  public final long logFirstIndex;

  public AppendEntriesRes(
      long term,
      List<EntryAck> acks,
      long appliedIndex,
      long contiguousEnd,
      long workingPartition,
      long truncatedFrom,
      long msgId) {
    this(
        term,
        acks,
        appliedIndex,
        contiguousEnd,
        workingPartition,
        truncatedFrom,
        msgId,
        UNSET,
        UNSET);
  }

  /** Every field: the original seven followed by {@code syncNumber} and {@code logFirstIndex}. */
  public AppendEntriesRes(
      long term,
      List<EntryAck> acks,
      long appliedIndex,
      long contiguousEnd,
      long workingPartition,
      long truncatedFrom,
      long msgId,
      long syncNumber,
      long logFirstIndex) {
    super(term);
    this.acks = acks == null ? Collections.emptyList() : acks;
    this.appliedIndex = appliedIndex;
    this.contiguousEnd = contiguousEnd;
    this.workingPartition = workingPartition;
    this.truncatedFrom = truncatedFrom;
    this.msgId = msgId;
    this.syncNumber = syncNumber;
    this.logFirstIndex = logFirstIndex;
  }

  /** The same response with other acks (every other field kept, e.g. when splitting a burst). */
  public AppendEntriesRes withAcks(List<EntryAck> newAcks) {
    return new AppendEntriesRes(
        term,
        newAcks,
        appliedIndex,
        contiguousEnd,
        workingPartition,
        truncatedFrom,
        msgId,
        syncNumber,
        logFirstIndex);
  }

  public boolean hasTruncatedFrom() {
    return truncatedFrom != UNSET;
  }

  public boolean hasSyncNumber() {
    return syncNumber != UNSET;
  }

  public boolean hasLogFirstIndex() {
    return logFirstIndex != UNSET;
  }

  public boolean hasAppliedIndex() {
    return appliedIndex != UNSET;
  }

  public boolean containsIndex(long index) {
    for (EntryAck a : acks) {
      if (a.index == index) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String kind() {
    return "RESP";
  }

  @Override
  public long matchIndex() {
    return acks.isEmpty() ? -1 : acks.get(0).index;
  }

  @Override
  public String brief() {
    StringBuilder sb = new StringBuilder("RESP[");
    for (int i = 0; i < acks.size(); i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(acks.get(i).brief());
    }
    sb.append("] term=").append(term);
    if (hasAppliedIndex()) {
      sb.append(" applied=").append(appliedIndex);
    }
    if (hasTruncatedFrom()) {
      sb.append(" truncatedFrom=").append(truncatedFrom);
    }
    if (hasSyncNumber()) {
      sb.append(" sync=").append(syncNumber);
    }
    if (hasLogFirstIndex()) {
      sb.append(" firstIndex=").append(logFirstIndex);
    }
    return sb.toString();
  }
}
