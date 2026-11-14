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
 * Protocol-specific per-entry metadata (Raft: none; TRaft: tau, rho, prevSize, flags; ParallelRaft:
 * date, conflict keys, look-behind keys).
 */
public interface EntryMeta {

  EntryMeta EMPTY =
      new EntryMeta() {
        @Override
        public String toString() {
          return "";
        }
      };

  /** Metadata of a partitioned (TRaft) entry; the invariant checker reads it generically. */
  interface Partitioned extends EntryMeta {
    long ts();

    long tsMax();

    long rho();

    int prevSize();

    boolean isBarrier();
  }

  /**
   * Metadata carrying a proposal number ("date"): the leader creating an entry stamps {@code date =
   * term}; a recovering leader rewriting it stamps its own term. The receiver overwrites a held
   * copy with the same (index, term) only if the new date is larger (ParallelRaft).
   */
  interface Dated extends EntryMeta {
    long date();

    /** The same metadata with another date. */
    EntryMeta withDate(long date);
  }

  /**
   * Metadata able to tell whether two entries conflict, i.e. must be applied in index order
   * (ParallelRaft conflict keys); the invariant checker reads it generically.
   */
  interface Conflicting extends EntryMeta {
    boolean conflictsWith(EntryMeta other);
  }
}
