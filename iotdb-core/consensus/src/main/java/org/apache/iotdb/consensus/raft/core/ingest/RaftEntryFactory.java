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

package org.apache.iotdb.consensus.raft.core.ingest;

import org.apache.iotdb.consensus.raft.core.NodeEnv;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import java.nio.ByteBuffer;

public final class RaftEntryFactory implements EntryFactory {

  @Override
  public LogEntry create(
      long index,
      long term,
      EntryType type,
      RequestMeta req,
      LeaderIngestState state,
      ByteBuffer payload,
      long reqId,
      Object liveRequest) {
    return new LogEntry(index, term, type, EntryMeta.EMPTY, payload, reqId, liveRequest);
  }

  @Override
  public LeaderIngestState rebuild(LogView log, SnapshotMetaExt ext, NodeEnv env) {
    return LeaderIngestState.NONE;
  }
}
