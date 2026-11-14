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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.ReplyHandle;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;

/**
 * Elections (Raft §5.2; TRaft Algorithm 1 via {@code VoteKeyPolicy}). Every state change that must
 * be durable before a message leaves the node goes through PersistMeta -> MetaDurable.
 */
final class ElectionModule {

  private final RaftCore core;

  ElectionModule(RaftCore core) {
    this.core = core;
  }

  // ---------------------------------------------------------------- campaign

  void startElection(boolean skipPreVote) {
    RaftState st = core.st;
    if (st.role == Role.LEADER) {
      core.leaveLeadership(); // an explicit timeout on a leader (PY start_election): step down and
      // campaign
      // no longer leader whatever comes next (a PreVote round, or no campaign for a learner):
      // leaveLeadership cleared the replicator, so the role must not stay LEADER
      st.role = Role.FOLLOWER;
      st.leaderId = -1;
      core.emit(new Action.RoleChanged(Role.FOLLOWER, -1, st.currentTerm));
    }
    if (st.membership.isEmpty() || !st.membership.isVoter(core.self)) {
      return; // learners never campaign
    }
    if (core.policy.preVote && !skipPreVote) {
      st.preVotes.clear();
      st.preVotes.add(core.self);
      VoteKey key = core.myVoteKey();
      for (int peer : st.membership.voters()) {
        if (peer != core.self) {
          core.emit(
              new Action.Send(peer, new RequestVoteReq(st.currentTerm + 1, core.self, key, true)));
        }
      }
      core.armElectionTimer();
      if (st.preVotes.size() >= st.membership.majority()) {
        startElection(true);
      }
      return;
    }
    st.role = Role.CANDIDATE;
    st.currentTerm++;
    st.votedFor = core.self;
    st.leaderId = -1;
    st.votes.clear();
    st.votes.add(core.self);
    core.faults.at(CrashPoint.VOTE_PRE_PERSIST);
    core.persistMeta(
        new RaftCore.MetaCtx(RaftCore.MetaKind.CAMPAIGN, st.currentTerm, -1, null, null));
  }

  void onCampaignDurable(RaftCore.MetaCtx c) {
    RaftState st = core.st;
    if (st.role != Role.CANDIDATE || st.currentTerm != c.term) {
      return; // superseded meanwhile
    }
    core.faults.at(CrashPoint.VOTE_POST_PERSIST);
    VoteKey key = core.myVoteKey();
    core.record(
        ListenerEvent.of("campaign", core.self)
            .with("term", st.currentTerm)
            .with("t_hat", key.lastTerm)
            .with("rho_hat", key.rhoHat)
            .with("sigma_hat", key.sigmaHat)
            .with("last_index", key.lastIndex)
            .build());
    for (int peer : st.membership.voters()) {
      if (peer != core.self) {
        core.emit(new Action.Send(peer, new RequestVoteReq(st.currentTerm, core.self, key, false)));
      }
    }
    core.armElectionTimer();
    maybeWin();
  }

  // ------------------------------------------------------------------ voting

  void onRequestVote(RequestVoteReq m, int from, ReplyHandle h) {
    RaftState st = core.st;
    if (m.preVote) {
      boolean grant =
          st.role != Role.LEADER
              && m.term > st.currentTerm
              && core.profile.voteKeyPolicy().compare(m.key, core.myVoteKey(), core.mut) >= 0;
      core.emit(
          new Action.Reply(h, new RequestVoteRes(grant ? m.term : st.currentTerm, grant, true)));
      return;
    }
    if (m.term < st.currentTerm) {
      core.emit(new Action.Reply(h, new RequestVoteRes(st.currentTerm, false, false)));
      return;
    }
    if (m.term > st.currentTerm) {
      core.adoptTerm(m.term, new Event.Inbound(from, m, h));
      return;
    }
    if (!core.mut.has("allow_revote") && st.votedFor != -1 && st.votedFor != m.candidateId) {
      core.record(
          ListenerEvent.of("vote", core.self)
              .with("cand", m.candidateId)
              .with("term", m.term)
              .with("granted", false)
              .with("reason", "already voted for N" + st.votedFor)
              .build());
      core.emit(new Action.Reply(h, new RequestVoteRes(st.currentTerm, false, false)));
      return;
    }
    VoteKey mine = core.myVoteKey();
    if (core.profile.voteKeyPolicy().compare(m.key, mine, core.mut) < 0) {
      core.record(
          ListenerEvent.of("vote", core.self)
              .with("cand", m.candidateId)
              .with("term", m.term)
              .with("granted", false)
              .with("reason", m.key + " < " + mine)
              .build());
      core.emit(new Action.Reply(h, new RequestVoteRes(st.currentTerm, false, false)));
      return;
    }
    st.votedFor = m.candidateId;
    core.faults.at(CrashPoint.VOTE_PRE_PERSIST);
    core.persistMeta(
        new RaftCore.MetaCtx(RaftCore.MetaKind.VOTE, st.currentTerm, m.candidateId, h, null));
  }

  void onVoteDurable(RaftCore.MetaCtx c) {
    RaftState st = core.st;
    core.faults.at(CrashPoint.VOTE_POST_PERSIST);
    if (st.currentTerm != c.term || st.votedFor != c.cand) {
      return; // the term moved on while the vote was being persisted
    }
    core.record(
        ListenerEvent.of("vote", core.self)
            .with("cand", c.cand)
            .with("term", c.term)
            .with("granted", true)
            .with("reason", "")
            .build());
    core.armElectionTimer();
    core.emit(new Action.Reply(c.handle, new RequestVoteRes(st.currentTerm, true, false)));
  }

  void onRequestVoteRes(RequestVoteRes m, int from) {
    RaftState st = core.st;
    if (m.term > st.currentTerm && !(m.preVote && m.granted)) {
      core.adoptTerm(m.term, null);
      return;
    }
    if (m.preVote) {
      if (m.granted && st.role != Role.LEADER && m.term == st.currentTerm + 1) {
        st.preVotes.add(from);
        if (st.preVotes.size() >= st.membership.majority()) {
          st.preVotes.clear();
          startElection(true);
        }
      }
      return;
    }
    if (st.role != Role.CANDIDATE || m.term < st.currentTerm || !m.granted) {
      return;
    }
    st.votes.add(from);
    maybeWin();
  }

  void maybeWin() {
    RaftState st = core.st;
    if (st.role == Role.CANDIDATE && st.votes.size() >= st.membership.majority()) {
      core.becomeLeader();
    }
  }
}
