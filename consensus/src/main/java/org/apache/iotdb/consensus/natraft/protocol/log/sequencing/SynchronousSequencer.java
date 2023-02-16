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
import org.apache.iotdb.consensus.natraft.protocol.log.VotingLog;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SynchronizedSequencer performs sequencing by taking the monitor of a LogManager within the caller
 * thread.
 */
public class SynchronousSequencer implements LogSequencer {

  private static final Logger logger = LoggerFactory.getLogger(SynchronousSequencer.class);
  private RaftMember member;
  private RaftLogManager logManager;
  private RaftConfig config;

  public SynchronousSequencer(RaftMember member, RaftLogManager logManager, RaftConfig config) {
    this.member = member;
    this.logManager = logManager;
    this.config = config;
  }

  private VotingLog enqueueEntry(VotingLog sendLogRequest) {

    if (member.getAllNodes().size() > 1) {
      member.getLogDispatcher().offer(sendLogRequest);
    }
    return sendLogRequest;
  }

  private static AtomicLong indexBlockCounter = new AtomicLong();

  @Override
  public VotingLog sequence(Entry e) {
    VotingLog sendLogRequest = null;

    long startWaitingTime = System.currentTimeMillis();

    while (true) {
      try {
        logManager.getLock().writeLock().lock();
        indexBlockCounter.decrementAndGet();
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

          // logDispatcher will serialize log, and set log size, and we will use the size after it
          logManager.append(Collections.singletonList(e));

          sendLogRequest = buildSendLogRequest(e);

          if (!(config.isUseFollowerSlidingWindow() && config.isEnableWeakAcceptance())) {
            sendLogRequest = enqueueEntry(sendLogRequest);
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
      sendLogRequest = enqueueEntry(sendLogRequest);
    }

    return sendLogRequest;
  }

  @Override
  public void setLogManager(RaftLogManager logManager) {
    this.logManager = logManager;
  }

  private VotingLog buildSendLogRequest(Entry e) {
    VotingLog votingLog = member.buildVotingLog(e);

    AppendEntryRequest appendEntryRequest = buildAppendEntryRequest(e, false);
    votingLog.setAppendEntryRequest(appendEntryRequest);

    return votingLog;
  }

  public AppendEntryRequest buildAppendEntryRequest(Entry e, boolean serializeNow) {
    AppendEntryRequest request = buildAppendEntryRequestBasic(e, serializeNow);
    request = buildAppendEntryRequestExtended(request, e, serializeNow);
    return request;
  }

  protected AppendEntryRequest buildAppendEntryRequestBasic(Entry entry, boolean serializeNow) {
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(member.getStatus().getTerm().get());
    if (serializeNow) {
      ByteBuffer byteBuffer = entry.serialize();
      entry.setByteSize(byteBuffer.array().length);
      request.entry = byteBuffer;
    }
    try {
      if (entry.getPrevTerm() != -1) {
        request.setPrevLogTerm(entry.getPrevTerm());
      } else {
        request.setPrevLogTerm(logManager.getTerm(entry.getCurrLogIndex() - 1));
      }
    } catch (Exception e) {
      logger.error("getTerm failed for newly append entries", e);
    }
    request.setLeader(member.getThisNode());
    // don't need lock because even if it's larger than the commitIndex when appending this log to
    // logManager, the follower can handle the larger commitIndex with no effect
    request.setLeaderCommit(logManager.getCommitLogIndex());
    request.setPrevLogIndex(entry.getCurrLogIndex() - 1);
    request.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());

    return request;
  }

  protected AppendEntryRequest buildAppendEntryRequestExtended(
      AppendEntryRequest request, Entry e, boolean serializeNow) {
    return request;
  }

  public static class Factory implements LogSequencerFactory {

    @Override
    public LogSequencer create(RaftMember member, RaftLogManager logManager, RaftConfig config) {
      return new SynchronousSequencer(member, logManager, config);
    }
  }

  @Override
  public void close() {}
}
