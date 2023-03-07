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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

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

    long startWaitingTime = System.currentTimeMillis();
    RaftLogManager logManager = member.getLogManager();
    while (true) {
      try {
        logManager.getLock().writeLock().lock();
        Entry lastEntry = logManager.getLastEntry();
        long lastIndex = lastEntry.getCurrLogIndex();
        long lastTerm = lastEntry.getCurrLogTerm();
        if ((lastEntry.getCurrLogIndex() - logManager.getCommitLogIndex()
            <= config.getUncommittedRaftLogNumForRejectThreshold())) {
          // if the log contains a physical plan which is not a LogPlan, assign the same index to
          // the plan so the state machine can be bridged with the consensus
          e.setCurrLogTerm(member.getStatus().getTerm().get());
          e.setCurrLogIndex(lastIndex + 1);
          e.setPrevTerm(lastTerm);
          e.setFromThisNode(true);

          // logDispatcher will serialize log, and set log size, and we will use the size after it
          logManager.append(Collections.singletonList(e));

          votingEntry = LogUtils.buildVotingLog(e, member);

          if (!(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance())) {
            votingEntry = LogUtils.enqueueEntry(votingEntry, member);
          }
          break;
        }
      } finally {
        logManager.getLock().writeLock().unlock();
      }

      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          return null;
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
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
