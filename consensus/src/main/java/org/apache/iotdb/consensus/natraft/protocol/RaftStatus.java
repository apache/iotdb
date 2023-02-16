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

package org.apache.iotdb.consensus.natraft.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RaftStatus {
  /** when the node is a leader, this map is used to track log progress of each follower. */
  protected Map<TEndPoint, PeerInfo> peerMap;
  /**
   * the current term of the node, this object also works as lock of some transactions of the member
   * like elections.
   */
  protected AtomicLong term = new AtomicLong(0);

  volatile RaftRole role = RaftRole.CANDIDATE;
  AtomicReference<TEndPoint> leader = new AtomicReference<>(null);
  volatile TEndPoint voteFor;

  public Map<TEndPoint, PeerInfo> getPeerMap() {
    return peerMap;
  }

  public void setPeerMap(Map<TEndPoint, PeerInfo> peerMap) {
    this.peerMap = peerMap;
  }

  public AtomicLong getTerm() {
    return term;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public RaftRole getRole() {
    return role;
  }

  public void setRole(RaftRole role) {
    this.role = role;
  }

  public AtomicReference<TEndPoint> getLeader() {
    return leader;
  }

  public void setLeader(AtomicReference<TEndPoint> leader) {
    this.leader = leader;
  }

  public TEndPoint getVoteFor() {
    return voteFor;
  }

  public void setVoteFor(TEndPoint voteFor) {
    this.voteFor = voteFor;
  }
}
