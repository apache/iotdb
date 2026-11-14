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

package org.apache.iotdb.consensus.raft.transport;

import org.apache.iotdb.consensus.raft.core.action.SendMeta;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.Message;

import java.util.List;
import java.util.Map;

/**
 * Outbound side of one group. Responses and failures come back to the group as {@code Inbound} /
 * {@code RpcFailed} events through the sink the transport was created with.
 */
public interface Transport {

  /**
   * Send one message (RequestVote, TimeoutNow, empty AppendEntries, RecoverTerm, UpdateSync) to
   * {@code to}.
   */
  void send(int to, Message msg, long msgId);

  /** Send the same (already loaded) entries to every target; per-target speculation flags. */
  void sendAppend(List<Integer> targets, AppendEntriesReq req, Map<Integer, SendMeta> metaByTarget);

  void close();
}
