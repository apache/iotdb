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

import org.apache.iotdb.consensus.raft.core.NodeEnv;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.Message;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;

import java.util.List;
import java.util.function.Consumer;

/** What a {@link ProtocolRequestHandler} may read and do on a follower (provided by the kernel). */
public interface ProtocolContext extends NodeEnv {

  LogView log();

  long currentTerm();

  /** Answer the request behind {@code h}. */
  void reply(ReplyHandle h, Message res);

  /**
   * Persist {@code ProtocolProfile.encodeProfileMeta()} together with term and vote; {@code
   * onDurable} runs once it is durable, and only if the node is still in the same term (and, if it
   * was the leader, still the same leadership).
   */
  void persistProfileMeta(Runnable onDurable);

  /**
   * Read the entries at {@code indexes} (payloads included) from the log store; {@code callback}
   * gets them in the same order, under the same term / leadership condition as {@link
   * #persistProfileMeta}. Each is the copy its slot holds when the callback runs, or null: the slot
   * is empty, or the store did not return that copy (its Persist still queued; an older copy the
   * store may still hold is never passed on). Only durable slots are guaranteed to load.
   */
  void loadEntries(long[] indexes, Consumer<List<LogEntry>> callback);
}
