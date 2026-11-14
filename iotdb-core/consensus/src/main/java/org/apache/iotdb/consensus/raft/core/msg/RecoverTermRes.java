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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Answer to {@link RecoverTermReq}. A voter whose sync number is below the request's answers only
 * {@code syncNumber / contiguousEnd / logFirstIndex}; one with the same sync number adds its
 * promise for that term (if any) and one page of its entries of that term ({@code committedIndexes}
 * = the subset it knows to be committed; {@code more / nextIndex} continue the paging); one with a
 * larger sync number answers only {@code syncNumber}. Unset numeric fields hold {@link #UNSET}.
 */
public final class RecoverTermRes extends Message {

  public static final long UNSET = Long.MIN_VALUE;

  public final boolean granted;
  public final long syncNumber;
  public final long contiguousEnd;
  public final long logFirstIndex;

  /** Ballot of the end-point promise for the requested sync term; {@link #UNSET} = none. */
  public final long promiseBallot;

  /** End point of that promise; {@link #UNSET} = none. */
  public final long promiseEnd;

  /** Held entries of the requested sync term, ascending (one page). */
  public final List<LogEntry> entries;

  /** The indexes among {@code entries} this node knows to be committed. */
  public final long[] committedIndexes;

  /** Another page follows, starting at {@code nextIndex}. */
  public final boolean more;

  public final long nextIndex;

  public RecoverTermRes(
      long term,
      boolean granted,
      long syncNumber,
      long contiguousEnd,
      long logFirstIndex,
      long promiseBallot,
      long promiseEnd,
      List<LogEntry> entries,
      long[] committedIndexes,
      boolean more,
      long nextIndex) {
    super(term);
    this.granted = granted;
    this.syncNumber = syncNumber;
    this.contiguousEnd = contiguousEnd;
    this.logFirstIndex = logFirstIndex;
    this.promiseBallot = promiseBallot;
    this.promiseEnd = promiseEnd;
    this.entries = entries == null ? Collections.emptyList() : entries;
    this.committedIndexes = committedIndexes == null ? new long[0] : committedIndexes;
    this.more = more;
    this.nextIndex = nextIndex;
  }

  /** The same answer carrying {@code newEntries} (e.g. once their payloads are loaded). */
  public RecoverTermRes withEntries(List<LogEntry> newEntries) {
    return new RecoverTermRes(
        term,
        granted,
        syncNumber,
        contiguousEnd,
        logFirstIndex,
        promiseBallot,
        promiseEnd,
        newEntries,
        committedIndexes,
        more,
        nextIndex);
  }

  public boolean hasPromise() {
    return promiseBallot != UNSET;
  }

  public boolean hasLogFirstIndex() {
    return logFirstIndex != UNSET;
  }

  public boolean isCommitted(long index) {
    for (long i : committedIndexes) {
      if (i == index) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String kind() {
    return "RTR";
  }

  @Override
  public long matchIndex() {
    return entries.isEmpty() ? -1 : entries.get(0).index;
  }

  @Override
  public String brief() {
    StringBuilder sb =
        new StringBuilder("RecoverTermResp(")
            .append(granted ? "GRANT" : "REJECT")
            .append(",sync=")
            .append(syncNumber)
            .append(",ce=")
            .append(contiguousEnd);
    if (hasLogFirstIndex()) {
      sb.append(",first=").append(logFirstIndex);
    }
    if (hasPromise()) {
      sb.append(",promise=(").append(promiseBallot).append(',').append(promiseEnd).append(')');
    }
    if (!entries.isEmpty()) {
      sb.append(",entries=").append(entries);
    }
    if (committedIndexes.length > 0) {
      sb.append(",committed=").append(Arrays.toString(committedIndexes));
    }
    if (more) {
      sb.append(",more,next=").append(nextIndex);
    }
    return sb.append(") t=").append(term).toString();
  }
}
