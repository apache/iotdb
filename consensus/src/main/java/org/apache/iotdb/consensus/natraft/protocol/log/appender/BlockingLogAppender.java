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

package org.apache.iotdb.consensus.natraft.protocol.log.appender;

import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.ConfigChangeEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.natraft.utils.Response;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * BlockingLogAppender wait for a certain amount of time when it receives out-of-order entries
 * (entries with indices larger than local last entry's index + 1), if the local log is updated
 * during the waiting and the received entries are now appendable, it appends them normally.
 * Otherwise, a LOG_MISMATCH is reported to the leader.
 */
public class BlockingLogAppender implements LogAppender {

  private static final Logger logger = LoggerFactory.getLogger(BlockingLogAppender.class);

  private RaftMember member;
  private RaftLogManager logManager;
  private RaftConfig config;

  public BlockingLogAppender(RaftMember member, RaftConfig config) {
    this.member = member;
    this.logManager = member.getLogManager();
    this.config = config;
  }

  /** Wait until all logs before "prevLogIndex" arrive or a timeout is reached. */
  private boolean waitForPrevLog(long prevLogIndex) {
    long waitStart = System.currentTimeMillis();
    long alreadyWait = 0;
    Object logUpdateCondition = logManager.getLogUpdateCondition(prevLogIndex);
    long lastLogIndex = logManager.getLastLogIndex();
    long waitTime = 1;
    while (lastLogIndex < prevLogIndex && alreadyWait <= config.getWriteOperationTimeoutMS()) {
      try {
        // each time new logs are appended, this will be notified
        synchronized (logUpdateCondition) {
          logUpdateCondition.wait(waitTime);
        }
        lastLogIndex = logManager.getLastLogIndex();
        if (lastLogIndex >= prevLogIndex) {
          return true;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      waitTime = waitTime * 2;
      alreadyWait = System.currentTimeMillis() - waitStart;
    }

    return alreadyWait <= config.getWriteOperationTimeoutMS();
  }

  protected boolean checkPrevLogIndex(long prevLogIndex) {
    long lastLogIndex = logManager.getLastLogIndex();
    // there are logs missing between the incoming log and the local last log, and such logs
    // did not come within a timeout, report a mismatch to the sender and it shall fix this
    // through catch-up
    return lastLogIndex >= prevLogIndex || waitForPrevLog(prevLogIndex);
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @param logs append logs
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  public AppendEntryResult appendEntries(long leaderCommit, long term, List<Entry> logs) {
    logger.debug("{}, entries={}, leaderCommit={}", member.getName(), logs, leaderCommit);
    if (logs.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    long startTime = Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.getOperationStartTime();
    boolean resp = checkPrevLogIndex(logs.get(0).getCurrLogIndex() - 1);
    Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);

    if (!resp) {
      return new AppendEntryResult(Response.RESPONSE_LOG_MISMATCH)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    ConfigChangeEntry lastConfigEntry = null;
    for (Entry log : logs) {
      if (log instanceof ConfigChangeEntry) {
        lastConfigEntry = ((ConfigChangeEntry) log);
      }
    }

    startTime = Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
    AppendEntryResult result = new AppendEntryResult();
    long startWaitingTime = System.currentTimeMillis();
    while (true) {
      if ((logManager.getCommitLogIndex() - logManager.getAppliedIndex())
          <= config.getUnAppliedRaftLogNumForRejectThreshold()) {
        if (lastConfigEntry == null) {
          appendWithoutConfigChange(leaderCommit, term, logs, result);
        } else {
          appendWithConfigChange(leaderCommit, term, logs, result, lastConfigEntry);
        }
        break;
      }

      try {
        TimeUnit.MILLISECONDS.sleep(10);
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          result.status = Response.RESPONSE_TOO_BUSY;
          return result;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);

    return result;
  }

  protected boolean appendWithConfigChange(
      long leaderCommit,
      long term,
      List<Entry> logs,
      AppendEntryResult result,
      ConfigChangeEntry configChangeEntry) {
    boolean resp;
    try {
      logManager.writeLock();
      resp = logManager.maybeAppend(logs);

      if (resp) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} append a new log list {}, commit to {}", member.getName(), logs, leaderCommit);
        }

        result.status = Response.RESPONSE_STRONG_ACCEPT;
        result.setLastLogIndex(logManager.getLastLogIndex());
        member.setNewNodes(configChangeEntry.getNewPeers());
        member.tryUpdateCommitIndex(term, leaderCommit, -1);
      } else {
        // the incoming log points to an illegal position, reject it
        result.status = Response.RESPONSE_LOG_MISMATCH;
      }
    } finally {
      logManager.writeUnlock();
    }
    return resp;
  }

  protected boolean appendWithoutConfigChange(
      long leaderCommit, long term, List<Entry> logs, AppendEntryResult result) {
    boolean resp = logManager.maybeAppend(logs);
    if (resp) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{} append a new log list {}, commit to {}", member.getName(), logs, leaderCommit);
      }
      result.status = Response.RESPONSE_STRONG_ACCEPT;
      result.setLastLogIndex(logManager.getLastLogIndex());
      member.tryUpdateCommitIndex(term, leaderCommit, -1);
    } else {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return resp;
  }

  @Override
  public void reset() {
    // no states maintained by this implementation
  }

  public static class Factory implements LogAppenderFactory {

    @Override
    public LogAppender create(RaftMember member, RaftConfig config) {
      return new BlockingLogAppender(member, config);
    }
  }
}
