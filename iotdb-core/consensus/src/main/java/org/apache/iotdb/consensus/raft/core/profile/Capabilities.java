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

package org.apache.iotdb.consensus.raft.core.profile;

/**
 * Kernel capability switches a protocol profile turns on. Raft and TRaft use {@link #NONE}: every
 * switch off, so the kernel takes exactly its original (HEAD) paths.
 */
public final class Capabilities {

  public static final Capabilities NONE = new Capabilities(false, false, false, false, 0);

  /**
   * Commit frontier = the contiguous prefix of the committed set; indexes may commit out of order;
   * request templates carry the commit bitmap.
   */
  public final boolean outOfOrderCommit;

  /** Followers ack every durable entry, also beyond holes; the entry path is gate-first. */
  public final boolean ackBeyondHoles;

  /** A copy with the same (index, term) and a larger date overwrites the held one. */
  public final boolean datedOverwrite;

  /** A new leader enters recovery; its election entry waits for {@code recoveryComplete()}. */
  public final boolean leaderRecovery;

  /** Number of indexes the commit bitmap of a request covers. */
  public final int commitBitmapBits;

  public Capabilities(
      boolean outOfOrderCommit,
      boolean ackBeyondHoles,
      boolean datedOverwrite,
      boolean leaderRecovery,
      int commitBitmapBits) {
    if (commitBitmapBits < 0) {
      throw new IllegalArgumentException("commitBitmapBits must be >= 0: " + commitBitmapBits);
    }
    this.outOfOrderCommit = outOfOrderCommit;
    this.ackBeyondHoles = ackBeyondHoles;
    this.datedOverwrite = datedOverwrite;
    this.leaderRecovery = leaderRecovery;
    this.commitBitmapBits = commitBitmapBits;
  }

  /** True if every switch is off (the kernel behaves as plain Raft / TRaft). */
  public boolean isNone() {
    return !outOfOrderCommit && !ackBeyondHoles && !datedOverwrite && !leaderRecovery;
  }

  @Override
  public String toString() {
    return "Capabilities{outOfOrderCommit="
        + outOfOrderCommit
        + ",ackBeyondHoles="
        + ackBeyondHoles
        + ",datedOverwrite="
        + datedOverwrite
        + ",leaderRecovery="
        + leaderRecovery
        + ",commitBitmapBits="
        + commitBitmapBits
        + "}";
  }
}
