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

import org.apache.iotdb.consensus.raft.core.log.VoteKey;

public final class RequestVoteReq extends Message {

  public final int candidateId;
  public final VoteKey key;
  public final boolean preVote;

  public RequestVoteReq(long term, int candidateId, VoteKey key, boolean preVote) {
    super(term);
    this.candidateId = candidateId;
    this.key = key;
    this.preVote = preVote;
  }

  @Override
  public String kind() {
    return preVote ? "PREVOTE" : "RV";
  }

  @Override
  public String brief() {
    return (preVote ? "PreVote" : "RequestVote")
        + "(t_c="
        + term
        + ",t^="
        + key.lastTerm
        + ",rho^="
        + key.rhoHat
        + ",sigma^="
        + key.sigmaHat
        + ",lastIndex="
        + key.lastIndex
        + ") cand=N"
        + candidateId;
  }
}
