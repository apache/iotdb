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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;

/**
 * Optional AppendEntries extension of the out-of-order protocols (ParallelRaft). Raft and TRaft
 * never set it ({@code AppendEntriesReq.ext == null}), so their messages and wire format are
 * unchanged.
 *
 * <ul>
 *   <li>Commit information: {@code commitBits} is a bitmap over {@code (commitBase, commitBase +
 *       B]}; bit {@code i} (byte {@code i / 8}, mask {@code 1 << (i % 8)}, the layout of {@link
 *       BitSet#toByteArray()}) stands for index {@code commitBase + 1 + i}. Only the bytes up to
 *       the highest set bit are encoded; {@code null} means no bit is set.
 *   <li>Recovery / catch-up push: {@code recovery} marks the request as a push (harness kind {@code
 *       PUSH}); {@code truncateFrom} (if set) removes every index at or beyond it before the pushed
 *       entries are stored; {@code syncTerm} is the sync term the pushed entries belong to.
 * </ul>
 *
 * <p>Unset numeric fields hold {@link #UNSET}. An extension with no field set travels as no
 * extension at all (it decodes to {@code null}).
 */
public final class AppendExt {

  public static final long UNSET = Long.MIN_VALUE;

  public final long commitBase;

  /** Commit bitmap over {@code (commitBase, commitBase + B]}; null = no bit set. */
  public final byte[] commitBits;

  public final long truncateFrom;
  public final long syncTerm;
  public final boolean recovery;

  public AppendExt(
      long commitBase, byte[] commitBits, long truncateFrom, long syncTerm, boolean recovery) {
    this.commitBase = commitBase;
    this.commitBits = commitBits;
    this.truncateFrom = truncateFrom;
    this.syncTerm = syncTerm;
    this.recovery = recovery;
  }

  /** Commit information only (steady-state AppendEntries / heartbeats). */
  public static AppendExt commit(long commitBase, byte[] commitBits) {
    return new AppendExt(commitBase, commitBits, UNSET, UNSET, false);
  }

  /** A recovery / catch-up push of term {@code syncTerm}; {@code truncateFrom} may be UNSET. */
  public static AppendExt push(long truncateFrom, long syncTerm) {
    return new AppendExt(UNSET, null, truncateFrom, syncTerm, true);
  }

  public AppendExt withCommit(long base, byte[] bits) {
    return new AppendExt(base, bits, truncateFrom, syncTerm, recovery);
  }

  /** The same extension turned into a push (recovery = true). */
  public AppendExt withPush(long newTruncateFrom, long newSyncTerm) {
    return new AppendExt(commitBase, commitBits, newTruncateFrom, newSyncTerm, true);
  }

  public AppendExt withTruncateFrom(long newTruncateFrom) {
    return new AppendExt(commitBase, commitBits, newTruncateFrom, syncTerm, recovery);
  }

  public AppendExt withSyncTerm(long newSyncTerm) {
    return new AppendExt(commitBase, commitBits, truncateFrom, newSyncTerm, recovery);
  }

  public AppendExt withRecovery(boolean newRecovery) {
    return new AppendExt(commitBase, commitBits, truncateFrom, syncTerm, newRecovery);
  }

  public boolean hasCommit() {
    return commitBase != UNSET;
  }

  public boolean hasTruncateFrom() {
    return truncateFrom != UNSET;
  }

  public boolean hasSyncTerm() {
    return syncTerm != UNSET;
  }

  /** True if no field is set (such an extension is not distinguishable from none on the wire). */
  public boolean isEmpty() {
    return commitBase == UNSET
        && commitBits == null
        && truncateFrom == UNSET
        && syncTerm == UNSET
        && !recovery;
  }

  // ------------------------------------------------------------- commit bitmap

  /**
   * The bitmap of the members of {@code committed} in {@code (base, base + maxBits]}, encoded up to
   * the highest set bit; null if none of them is in range or {@code maxBits <= 0}.
   */
  public static byte[] bitsOf(long base, NavigableSet<Long> committed, int maxBits) {
    if (maxBits <= 0 || committed.isEmpty()) {
      return null;
    }
    BitSet bits = new BitSet();
    for (long i : committed.subSet(base + 1, true, base + maxBits, true)) {
      bits.set((int) (i - base - 1));
    }
    return bits.isEmpty() ? null : bits.toByteArray();
  }

  /** True if the bitmap marks {@code index} committed (false outside the covered range). */
  public boolean bitSet(long index) {
    if (commitBits == null || commitBase == UNSET || index <= commitBase) {
      return false;
    }
    long bit = index - commitBase - 1;
    if (bit >= (long) commitBits.length * 8) {
      return false;
    }
    return (commitBits[(int) (bit >>> 3)] & (1 << (int) (bit & 7))) != 0;
  }

  /** Every index the bitmap marks committed, ascending. */
  public List<Long> committedInBits() {
    if (commitBits == null || commitBase == UNSET) {
      return Collections.emptyList();
    }
    BitSet bits = BitSet.valueOf(commitBits);
    List<Long> out = new ArrayList<>(bits.cardinality());
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      out.add(commitBase + 1 + i);
    }
    return out;
  }

  // ------------------------------------------------------------------ object

  public String brief() {
    StringBuilder sb = new StringBuilder("ext{");
    String sep = "";
    if (hasCommit()) {
      sb.append("commitBase=").append(commitBase);
      sep = ",";
    }
    if (commitBits != null) {
      sb.append(sep).append("commitBits=").append(committedInBits());
      sep = ",";
    }
    if (hasTruncateFrom()) {
      sb.append(sep).append("truncateFrom=").append(truncateFrom);
      sep = ",";
    }
    if (hasSyncTerm()) {
      sb.append(sep).append("sync=").append(syncTerm);
      sep = ",";
    }
    if (recovery) {
      sb.append(sep).append("recovery");
    }
    return sb.append('}').toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AppendExt)) {
      return false;
    }
    AppendExt x = (AppendExt) o;
    return commitBase == x.commitBase
        && truncateFrom == x.truncateFrom
        && syncTerm == x.syncTerm
        && recovery == x.recovery
        && Arrays.equals(commitBits, x.commitBits);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitBase, truncateFrom, syncTerm, recovery) * 31
        + Arrays.hashCode(commitBits);
  }

  @Override
  public String toString() {
    return brief();
  }
}
