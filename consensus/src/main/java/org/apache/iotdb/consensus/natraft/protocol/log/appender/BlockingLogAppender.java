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
import org.apache.iotdb.consensus.natraft.protocol.Response;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  public AppendEntryResult appendEntry(AppendEntryRequest request, Entry log) {
    long resp = checkPrevLogIndex(request.prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(resp)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    long startWaitingTime = System.currentTimeMillis();
    long success;
    AppendEntryResult result = new AppendEntryResult();
    while (true) {
      // TODO: Consider memory footprint to execute a precise rejection
      if ((logManager.getCommitLogIndex() - logManager.getAppliedIndex())
          <= config.getUnAppliedRaftLogNumForRejectThreshold()) {
        success =
            logManager.maybeAppend(
                request.prevLogIndex,
                request.prevLogTerm,
                request.leaderCommit,
                Collections.singletonList(log));
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          result.status = Response.RESPONSE_TOO_BUSY;
          return result;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (success != -1) {
      logger.debug("{} append a new log {}", member.getName(), log);
      result.status = Response.RESPONSE_STRONG_ACCEPT;
    } else {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    result.setGroupId(request.getGroupId());
    return result;
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

  protected long checkPrevLogIndex(long prevLogIndex) {
    long lastLogIndex = logManager.getLastLogIndex();
    if (lastLogIndex < prevLogIndex && !waitForPrevLog(prevLogIndex)) {
      // there are logs missing between the incoming log and the local last log, and such logs
      // did not come within a timeout, report a mismatch to the sender and it shall fix this
      // through catch-up
      return Response.RESPONSE_LOG_MISMATCH;
    }
    return Response.RESPONSE_AGREE;
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @param logs append logs
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  public AppendEntryResult appendEntries(AppendEntriesRequest request, List<Entry> logs) {
    logger.debug(
        "{}, prevLogIndex={}, prevLogTerm={}, leaderCommit={}",
        member.getName(),
        request.prevLogIndex,
        request.prevLogTerm,
        request.leaderCommit);
    if (logs.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    long resp = checkPrevLogIndex(request.prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(resp)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    AppendEntryResult result = new AppendEntryResult();
    long startWaitingTime = System.currentTimeMillis();
    while (true) {
      synchronized (logManager) {
        // TODO: Consider memory footprint to execute a precise rejection
        if ((logManager.getCommitLogIndex() - logManager.getAppliedIndex())
            <= config.getUnAppliedRaftLogNumForRejectThreshold()) {
          resp =
              logManager.maybeAppend(
                  request.prevLogIndex, request.prevLogTerm, request.leaderCommit, logs);
          break;
        }
      }

      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          result.status = Response.RESPONSE_TOO_BUSY;
          return result;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (resp != -1) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{} append a new log list {}, commit to {}",
            member.getName(),
            logs,
            request.leaderCommit);
      }
      result.status = Response.RESPONSE_STRONG_ACCEPT;
      result.setLastLogIndex(logManager.getLastLogIndex());
      result.setLastLogTerm(logManager.getLastLogTerm());

    } else {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return result;
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
