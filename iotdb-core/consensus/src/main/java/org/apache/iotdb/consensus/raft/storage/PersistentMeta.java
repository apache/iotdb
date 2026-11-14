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

package org.apache.iotdb.consensus.raft.storage;

/**
 * The two persistent Raft fields plus the protocol profile's opaque state (ParallelRaft: sync
 * number and promise; empty for Raft / TRaft), all written atomically.
 */
public final class PersistentMeta {

  public static final PersistentMeta INITIAL = new PersistentMeta(0, -1);

  public final long term;
  public final int votedFor;

  /** {@code ProtocolProfile.encodeProfileMeta()} as last persisted; never null. */
  public final byte[] profileMeta;

  public PersistentMeta(long term, int votedFor) {
    this(term, votedFor, new byte[0]);
  }

  public PersistentMeta(long term, int votedFor, byte[] profileMeta) {
    this.term = term;
    this.votedFor = votedFor;
    this.profileMeta = profileMeta == null ? new byte[0] : profileMeta;
  }

  @Override
  public String toString() {
    return "term="
        + term
        + ",votedFor="
        + votedFor
        + (profileMeta.length > 0 ? ",profileMeta=" + profileMeta.length + "B" : "");
  }
}
