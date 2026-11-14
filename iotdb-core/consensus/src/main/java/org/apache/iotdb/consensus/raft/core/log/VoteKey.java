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

package org.apache.iotdb.consensus.raft.core.log;

/**
 * Up-to-date comparison key carried by RequestVote: Raft uses (lastTerm, lastIndex); TRaft's TRIPLE
 * uses (lastTerm, rhoHat, sigmaHat) and PAIR uses (committedTerm, committedRho); ParallelRaft uses
 * the sync number (Raft / TRaft leave it 0). All components travel together so the protocols share
 * one message.
 */
public final class VoteKey {

  public final long lastTerm;
  public final long lastIndex;
  public final long rhoHat;
  public final long sigmaHat;
  public final long committedTerm;
  public final long committedRho;

  /** ParallelRaft sync number; 0 for Raft / TRaft. */
  public final long syncNumber;

  public VoteKey(
      long lastTerm,
      long lastIndex,
      long rhoHat,
      long sigmaHat,
      long committedTerm,
      long committedRho) {
    this(lastTerm, lastIndex, rhoHat, sigmaHat, committedTerm, committedRho, 0);
  }

  public VoteKey(
      long lastTerm,
      long lastIndex,
      long rhoHat,
      long sigmaHat,
      long committedTerm,
      long committedRho,
      long syncNumber) {
    this.lastTerm = lastTerm;
    this.lastIndex = lastIndex;
    this.rhoHat = rhoHat;
    this.sigmaHat = sigmaHat;
    this.committedTerm = committedTerm;
    this.committedRho = committedRho;
    this.syncNumber = syncNumber;
  }

  public static VoteKey raft(long lastTerm, long lastIndex) {
    return new VoteKey(lastTerm, lastIndex, 0, 0, 0, -1);
  }

  public static VoteKey triple(long tHat, long fe, long rhoHat, long sigmaHat) {
    return new VoteKey(tHat, fe, rhoHat, sigmaHat, 0, -1);
  }

  /** ParallelRaft: the sync number plus the Raft components (for an optional log tie-break). */
  public static VoteKey sync(long syncNumber, long lastTerm, long lastIndex) {
    return new VoteKey(lastTerm, lastIndex, 0, 0, 0, -1, syncNumber);
  }

  public static int compareRaft(VoteKey a, VoteKey b) {
    int c = Long.compare(a.lastTerm, b.lastTerm);
    return c != 0 ? c : Long.compare(a.lastIndex, b.lastIndex);
  }

  public static int compareTriple(VoteKey a, VoteKey b) {
    int c = Long.compare(a.lastTerm, b.lastTerm);
    if (c != 0) {
      return c;
    }
    c = Long.compare(a.rhoHat, b.rhoHat);
    return c != 0 ? c : Long.compare(a.sigmaHat, b.sigmaHat);
  }

  public static int comparePair(VoteKey a, VoteKey b) {
    int c = Long.compare(a.committedTerm, b.committedTerm);
    return c != 0 ? c : Long.compare(a.committedRho, b.committedRho);
  }

  /** ParallelRaft {@code syncOK}: compares the sync numbers only (no log comparison). */
  public static int compareSync(VoteKey a, VoteKey b) {
    return Long.compare(a.syncNumber, b.syncNumber);
  }

  @Override
  public String toString() {
    return "(" + lastTerm + ", " + rhoHat + ", " + sigmaHat + ")";
  }

  public String raftString() {
    return "(" + lastTerm + ", " + lastIndex + ")";
  }

  public String syncString() {
    return "(sync=" + syncNumber + ", " + lastTerm + ", " + lastIndex + ")";
  }
}
