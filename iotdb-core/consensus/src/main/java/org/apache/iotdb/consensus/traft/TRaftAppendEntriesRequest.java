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

package org.apache.iotdb.consensus.traft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** In-memory form of an AppendEntries RPC. */
class TRaftAppendEntriesRequest {

  private final int leaderId;
  private final long term;
  private final long prevLogIndex;
  private final long prevLogTerm;
  private final long leaderCommit;
  private final List<TRaftLogEntry> entries;

  TRaftAppendEntriesRequest(
      int leaderId,
      long term,
      long prevLogIndex,
      long prevLogTerm,
      long leaderCommit,
      List<TRaftLogEntry> entries) {
    this.leaderId = leaderId;
    this.term = term;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.leaderCommit = leaderCommit;
    this.entries = new ArrayList<>(entries.size());
    for (TRaftLogEntry entry : entries) {
      this.entries.add(entry.copy());
    }
  }

  int getLeaderId() {
    return leaderId;
  }

  long getTerm() {
    return term;
  }

  long getPrevLogIndex() {
    return prevLogIndex;
  }

  long getPrevLogTerm() {
    return prevLogTerm;
  }

  long getLeaderCommit() {
    return leaderCommit;
  }

  List<TRaftLogEntry> getEntries() {
    return Collections.unmodifiableList(entries);
  }
}
