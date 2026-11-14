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

import java.util.concurrent.CompletableFuture;

/**
 * Where the response to a synchronous RPC goes: a future the Thrift worker is waiting on, or (in
 * the simulation) just the requester id so the harness can queue the response as a message.
 */
public final class ReplyHandle {

  public final int requester;
  public final long msgId;
  public final CompletableFuture<Message> future;

  public ReplyHandle(int requester, long msgId, CompletableFuture<Message> future) {
    this.requester = requester;
    this.msgId = msgId;
    this.future = future;
  }

  public static ReplyHandle forSim(int requester, long msgId) {
    return new ReplyHandle(requester, msgId, null);
  }
}
