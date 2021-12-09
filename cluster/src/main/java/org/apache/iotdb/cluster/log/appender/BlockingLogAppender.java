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

package org.apache.iotdb.cluster.log.appender;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

  public BlockingLogAppender(RaftMember member) {
    this.member = member;
    this.logManager = member.getLogManager();
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  public AppendEntryResult appendEntry(
      long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    long resp = checkPrevLogIndex(prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(resp).setHeader(member.getHeader());
    }

    long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
    long success;
    AppendEntryResult result = new AppendEntryResult();
    synchronized (logManager) {
      success = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
      if (success != -1) {
        result.setLastLogIndex(logManager.getLastLogIndex());
        result.setLastLogTerm(logManager.getLastLogTerm());
      }
    }
    Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
    if (success != -1) {
      logger.debug("{} append a new log {}", member.getName(), log);
      result.status = Response.RESPONSE_STRONG_ACCEPT;
    } else {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return result;
  }

  /** Wait until all logs before "prevLogIndex" arrive or a timeout is reached. */
  private boolean waitForPrevLog(long prevLogIndex) {
    long waitStart = System.currentTimeMillis();
    long alreadyWait = 0;
    Object logUpdateCondition = logManager.getLogUpdateCondition(prevLogIndex);
    long lastLogIndex = logManager.getLastLogIndex();
    Timer.Statistic.RAFT_RECEIVER_INDEX_DIFF.add(prevLogIndex - lastLogIndex);
    while (lastLogIndex < prevLogIndex
        && alreadyWait <= ClusterConstant.getWriteOperationTimeoutMS()) {
      try {
        // each time new logs are appended, this will be notified
        synchronized (logUpdateCondition) {
          logUpdateCondition.wait(1);
        }
        lastLogIndex = logManager.getLastLogIndex();
        if (lastLogIndex >= prevLogIndex) {
          return true;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      alreadyWait = System.currentTimeMillis() - waitStart;
    }

    return alreadyWait <= ClusterConstant.getWriteOperationTimeoutMS();
  }

  protected long checkPrevLogIndex(long prevLogIndex) {
    long lastLogIndex = logManager.getLastLogIndex();
    long startTime = Timer.Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.getOperationStartTime();
    if (lastLogIndex < prevLogIndex && !waitForPrevLog(prevLogIndex)) {
      // there are logs missing between the incoming log and the local last log, and such logs
      // did not come within a timeout, report a mismatch to the sender and it shall fix this
      // through catch-up
      return Response.RESPONSE_LOG_MISMATCH;
    }
    Timer.Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);
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
  public AppendEntryResult appendEntries(
      long prevLogIndex, long prevLogTerm, long leaderCommit, List<Log> logs) {
    logger.debug(
        "{}, prevLogIndex={}, prevLogTerm={}, leaderCommit={}",
        member.getName(),
        prevLogIndex,
        prevLogTerm,
        leaderCommit);
    if (logs.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE).setHeader(member.getHeader());
    }

    long resp = checkPrevLogIndex(prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return new AppendEntryResult(resp).setHeader(member.getHeader());
    }

    AppendEntryResult result = new AppendEntryResult();
    synchronized (logManager) {
      long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
      resp = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, logs);
      Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
      if (resp != -1) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} append a new log list {}, commit to {}", member.getName(), logs, leaderCommit);
        }
        result.status = Response.RESPONSE_STRONG_ACCEPT;
        result.setLastLogIndex(logManager.getLastLogIndex());
        result.setLastLogTerm(logManager.getLastLogTerm());
      } else {
        // the incoming log points to an illegal position, reject it
        result.status = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return result;
  }

  @Override
  public void reset() {
    // no states maintained by this implementation
  }

  public static class Factory implements LogAppenderFactory {

    @Override
    public LogAppender create(RaftMember member) {
      return new BlockingLogAppender(member);
    }
  }
}
