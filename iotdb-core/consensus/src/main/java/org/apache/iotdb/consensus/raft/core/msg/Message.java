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

/** Kernel-level RPC message (transport-independent POJO). */
public abstract class Message {

  public final long term;

  protected Message(long term) {
    this.term = term;
  }

  /**
   * Harness kind: AE (entries), CN (empty AppendEntries), FENCE, PUSH (recovery push), RESP, RV,
   * RVR, TIMEOUT_NOW, RT / RTR (RecoverTerm), US / USR (UpdateSync).
   */
  public abstract String kind();

  /** Index used by the harness to match messages (entry index / acked index); -1 if none. */
  public long matchIndex() {
    return -1;
  }

  public abstract String brief();

  @Override
  public String toString() {
    return brief();
  }
}
