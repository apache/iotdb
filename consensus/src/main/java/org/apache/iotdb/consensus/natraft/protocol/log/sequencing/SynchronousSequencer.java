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

package org.apache.iotdb.consensus.natraft.protocol.log.sequencing;

import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.natraft.utils.LogUtils;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * SynchronizedSequencer performs sequencing by taking the monitor of a LogManager within the caller
 * thread.
 */
public class SynchronousSequencer implements LogSequencer {

  private static final Logger logger = LoggerFactory.getLogger(SynchronousSequencer.class);
  private RaftMember member;
  private RaftConfig config;

  public SynchronousSequencer(RaftMember member, RaftConfig config) {
    this.member = member;
    this.config = config;
  }

  @Override
  public VotingEntry sequence(Entry e) {
    VotingEntry votingEntry = null;

    // TODO: control the number of uncommitted entries while not letting the new leader be blocked
    RaftLogManager logManager = member.getLogManager();
    try {
      long startTime =
          Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.getOperationStartTime();
      logManager.writeLock();
      Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.calOperationCostTimeFromStart(
          startTime);

      startTime = Statistic.RAFT_SENDER_OCCUPY_LOG_MANAGER_IN_APPEND.getOperationStartTime();
      Entry lastEntry = logManager.getLastEntry();
      long lastIndex = lastEntry.getCurrLogIndex();
      long lastTerm = lastEntry.getCurrLogTerm();

      e.setCurrLogTerm(member.getStatus().getTerm().get());
      e.setCurrLogIndex(lastIndex + 1);
      e.setPrevTerm(lastTerm);
      e.setFromThisNode(true);
      e.createTime = System.nanoTime();

      votingEntry = LogUtils.buildVotingLog(e, member);
      // logDispatcher will serialize log, and set log size, and we will use the size after it
      logManager.append(Collections.singletonList(e), true);

      if (!(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance())) {
        votingEntry = LogUtils.enqueueEntry(votingEntry, member);
      }

      if (member.getAllNodes().size() == 1) {
        logManager.commitTo(e.getCurrLogIndex());
      }

      Statistic.RAFT_SENDER_OCCUPY_LOG_MANAGER_IN_APPEND.calOperationCostTimeFromStart(startTime);
    } finally {
      logManager.writeUnlock();
    }

    if (config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance()) {
      votingEntry = LogUtils.enqueueEntry(votingEntry, member);
    }

    return votingEntry;
  }

  public static class Factory implements LogSequencerFactory {

    @Override
    public LogSequencer create(RaftMember member, RaftConfig config) {
      return new SynchronousSequencer(member, config);
    }
  }

  @Override
  public void close() {}
}
