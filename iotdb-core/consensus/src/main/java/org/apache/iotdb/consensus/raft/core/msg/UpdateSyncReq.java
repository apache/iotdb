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

/**
 * ParallelRaft sync-number upgrade: the leader (or recovering leader) of {@code term} asks a voter
 * whose push of its current sync term is complete to move to sync number {@code newSync}.
 */
public final class UpdateSyncReq extends Message {

  public final int leaderId;
  public final long newSync;

  public UpdateSyncReq(long term, int leaderId, long newSync) {
    super(term);
    this.leaderId = leaderId;
    this.newSync = newSync;
  }

  @Override
  public String kind() {
    return "US";
  }

  @Override
  public String brief() {
    return "UpdateSync(t=" + term + ",newSync=" + newSync + ") from N" + leaderId;
  }
}
