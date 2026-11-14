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

package org.apache.iotdb.consensus.raft.server;

import org.apache.iotdb.consensus.raft.core.msg.Message;

/**
 * Test / benchmark hook on the inbound side of every group of this JVM: before an {@link
 * org.apache.iotdb.consensus.raft.core.event.Event.Inbound} (a request from a peer, or the response
 * to one of our RPCs) reaches the kernel, {@link RaftGroupServer#onEvent} asks the installed
 * instance whether to {@link #drop} it (lost on the wire: a dropped request never gets its reply,
 * so the sender sees an RPC timeout; a dropped response is simply never heard) or to delay it by
 * {@link #delayMillis} (re-submitted through the node's timer pool after the delay, so that later
 * messages overtake it: network jitter, long tails, reordering).
 *
 * <p>Protocol behaviour is untouched: the kernel sees exactly the events it would see on a lossy or
 * jittery network, in the order the hook lets them through. The default instance {@link #NONE}
 * never drops or delays, and the server reads one volatile per inbound event; with {@link #NONE}
 * installed the fast path is one reference comparison. Nothing in production installs an instance;
 * the benchmark driver ({@code org.apache.iotdb.consensus.bench}) and fault-injection tests do. The
 * instance is global to the JVM: every node of an in-process cluster consults the same one, which
 * identifies the node by {@code self}.
 */
public abstract class InboundFaults {

  /** Never drops or delays: the production behaviour. */
  public static final InboundFaults NONE = new InboundFaults() {};

  private static volatile InboundFaults current = NONE;

  /** The installed instance ({@link #NONE} by default). */
  public static InboundFaults current() {
    return current;
  }

  /** Install {@code faults} for every group of this JVM; null restores {@link #NONE}. */
  public static void install(InboundFaults faults) {
    current = faults == null ? NONE : faults;
  }

  /**
   * True to discard {@code msg}, sent by node {@code from} to node {@code self}. Called on the
   * thread that received the message (a Thrift worker, or the async client's selector): keep it
   * cheap.
   */
  /** Test-only isolation of the file RPC path, which does not enter Event.Inbound. */
  public boolean blockSnapshot(int self) {
    return false;
  }

  public boolean drop(int self, int from, Message msg) {
    return false;
  }

  /**
   * Milliseconds to hold {@code msg} (from node {@code from}, arriving at node {@code self}) before
   * the kernel sees it; 0 or less delivers it at once. Consulted only when {@link #drop} returned
   * false.
   */
  public long delayMillis(int self, int from, Message msg) {
    return 0;
  }
}
