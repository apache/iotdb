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

package org.apache.iotdb.consensus.raft.core.replication;

import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;

/**
 * Follower side of a protocol's own requests (ParallelRaft: {@code RecoverTermReq} and {@code
 * UpdateSyncReq}). An {@link AcceptGate} may implement it. A profile whose gate does not (Raft,
 * TRaft) does not speak these messages: the kernel ignores them, and their answers, whatever their
 * term, as it always did.
 *
 * <p>For a gate that handles them, the kernel applies the Raft term rules first: a request of an
 * older term is answered with {@link #staleReply}, a newer term is adopted (durably) before the
 * request is handed over, and a request of the current term makes this node a follower of the
 * sender. {@link #onRequest} thus always runs on a follower of {@code from}, in {@code
 * ctx.currentTerm() == m.term}.
 */
public interface ProtocolRequestHandler {

  /** True for the message types this handler answers. */
  boolean handles(Message m);

  /** The answer to a request of a term older than {@code currentTerm}; null = no answer. */
  Message staleReply(Message m, long currentTerm);

  /** Handle {@code m} from the leader {@code from}; answer through {@code ctx.reply(h, ...)}. */
  void onRequest(Message m, int from, ReplyHandle h, ProtocolContext ctx);
}
