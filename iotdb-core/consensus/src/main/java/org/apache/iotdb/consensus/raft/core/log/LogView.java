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

import java.util.NavigableSet;

/** Read-only view of the in-memory log index (terms, types, metadata; no payloads). */
public interface LogView {

  long INVALID_INDEX = -1;

  /** First index still present (after snapshots); initially 0. */
  long firstIndex();

  /** Largest present slot (possibly beyond a hole); -1 when empty. */
  long lastIndex();

  /** Largest k such that firstIndex..k are all present and chain-linked; firstIndex-1 if none. */
  long contiguousEnd();

  /** Like {@link #contiguousEnd()} but every slot must also be durable. */
  long durableContiguousEnd();

  boolean has(long index);

  long termAt(long index);

  EntryType typeAt(long index);

  EntryMeta metaAt(long index);

  int sizeAt(long index);

  boolean isDurable(long index);

  /** Physical slot set (for TRaft working-partition derivation and sigma counting). */
  NavigableSet<Long> slots();

  /** Term of the entry covered by the last snapshot (0 when none). */
  long snapshotTerm();
}
