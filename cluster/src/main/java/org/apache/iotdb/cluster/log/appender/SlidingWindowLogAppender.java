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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryResult;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.util.Arrays;
import java.util.List;

public class SlidingWindowLogAppender implements LogAppender {

  private static final Logger logger = LoggerFactory.getLogger(SlidingWindowLogAppender.class);

  private int windowCapacity = ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem();
  private int windowLength = 0;
  private Log[] logWindow = new Log[windowCapacity];
  private long firstPosPrevIndex;
  private long[] prevTerms = new long[windowCapacity];

  private RaftMember member;
  private RaftLogManager logManager;

  public SlidingWindowLogAppender(RaftMember member) {
    this.member = member;
    this.logManager = member.getLogManager();
    reset();
  }

  /**
   * After insert an entry into the window, check if its previous and latter entries should be
   * removed if it mismatches.
   *
   * @param pos
   */
  private void checkLog(int pos) {
    checkLogPrev(pos);
    checkLogNext(pos);
  }

  private void checkLogPrev(int pos) {
    // check the previous entry
    long prevLogTerm = prevTerms[pos];
    if (pos > 0) {
      Log prev = logWindow[pos - 1];
      if (prev != null && prev.getCurrLogTerm() != prevLogTerm) {
        logWindow[pos - 1] = null;
      }
    }
  }

  private void checkLogNext(int pos) {
    // check the next entry
    Log log = logWindow[pos];
    boolean nextMismatch = false;
    if (pos < windowCapacity - 1) {
      long nextPrevTerm = prevTerms[pos + 1];
      if (nextPrevTerm != log.getCurrLogTerm()) {
        nextMismatch = true;
      }
    }
    if (nextMismatch) {
      for (int i = pos + 1; i < windowCapacity; i++) {
        if (logWindow[i] != null) {
          logWindow[i] = null;
          if (i == windowLength - 1) {
            windowLength = pos + 1;
          }
        } else {
          break;
        }
      }
    }
  }

  /**
   * Flush window range [0, flushPos) into the LogManager, where flushPos is the first null position
   * in the window.
   *
   * @param result
   * @param leaderCommit
   * @return
   */
  private long flushWindow(AppendEntryResult result, long leaderCommit) {
    long windowPrevLogIndex = firstPosPrevIndex;
    long windowPrevLogTerm = prevTerms[0];

    int flushPos = 0;
    for (; flushPos < windowCapacity; flushPos++) {
      if (logWindow[flushPos] == null) {
        break;
      }
    }

    // flush [0, flushPos)
    List<Log> logs = Arrays.asList(logWindow).subList(0, flushPos);
    logger.debug(
        "Flushing {} entries to log, first {}, last {}",
        logs.size(),
        logs.get(0),
        logs.get(logs.size() - 1));
    long success =
        logManager.maybeAppend(windowPrevLogIndex, windowPrevLogTerm, leaderCommit, logs);
    if (success != -1) {
      moveWindowRightward(flushPos);
    }
    result.status = Response.RESPONSE_STRONG_ACCEPT;
    result.setLastLogIndex(firstPosPrevIndex);
    result.setLastLogTerm(logManager.getLastLogTerm());
    return success;
  }

  private void moveWindowRightward(int step) {
    System.arraycopy(logWindow, step, logWindow, 0, windowCapacity - step);
    System.arraycopy(prevTerms, step, prevTerms, 0, windowCapacity - step);
    for (int i = 1; i <= step; i++) {
      logWindow[windowCapacity - i] = null;
    }
    firstPosPrevIndex = logManager.getLastLogIndex();
  }

  private void moveWindowLeftward(int step) {
    int length = Math.max(windowCapacity - step, 0);
    System.arraycopy(logWindow, 0, logWindow, step, length);
    System.arraycopy(prevTerms, 0, prevTerms, step, length);
    for (int i = 0; i < length; i++) {
      logWindow[i] = null;
    }
    firstPosPrevIndex = logManager.getLastLogIndex();
  }

  @Override
  public AppendEntryResult appendEntries(AppendEntriesRequest request, List<Log> logs) {
    if (logs.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE)
          .setHeader(member.getPartitionGroup().getHeader());
    }

    AppendEntryResult result = null;
    for (Log log : logs) {
      result = appendEntry(request.prevLogIndex, request.prevLogTerm, request.leaderCommit, log);

      if (result.status != Response.RESPONSE_AGREE
          && result.status != Response.RESPONSE_STRONG_ACCEPT
          && result.status != Response.RESPONSE_WEAK_ACCEPT) {
        return result;
      }
      request.prevLogIndex = log.getCurrLogIndex();
      request.prevLogTerm = log.getCurrLogTerm();
    }
    if (request.isSetSubReceivers()) {
      request.entries.forEach(Buffer::rewind);
      member.getLogRelay().offer(request, request.subReceivers);
    }

    return result;
  }

  @Override
  public AppendEntryResult appendEntry(AppendEntryRequest request, Log log) {

    AppendEntryResult result = null;
    long start = System.currentTimeMillis();
    long retryTime = 0;
    long maxRetry = 10000;
    while (result == null
        || result.status == Response.RESPONSE_OUT_OF_WINDOW && retryTime < maxRetry) {
      result = appendEntry(request.prevLogIndex, request.prevLogTerm, request.leaderCommit, log);
      retryTime = System.currentTimeMillis() - start;
      if (result.status == Response.RESPONSE_OUT_OF_WINDOW && retryTime < maxRetry) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    result.setHeader(request.getHeader());

    if (request.isSetSubReceivers() && !request.getSubReceivers().isEmpty()) {
      request.entry.rewind();
      member.getLogRelay().offer(request, request.subReceivers);
    }

    return result;
  }

  private AppendEntryResult appendEntry(
      long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    long appendedPos = 0;

    AppendEntryResult result = new AppendEntryResult();
    synchronized (logManager) {
      int windowPos = (int) (log.getCurrLogIndex() - logManager.getLastLogIndex() - 1);
      if (windowPos < 0) {
        // the new entry may replace an appended entry
        appendedPos = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
        result.status = Response.RESPONSE_STRONG_ACCEPT;
        result.setLastLogIndex(logManager.getLastLogIndex());
        result.setLastLogTerm(logManager.getLastLogTerm());
        moveWindowLeftward(-windowPos);
      } else if (windowPos < windowCapacity) {
        // the new entry falls into the window
        logWindow[windowPos] = log;
        prevTerms[windowPos] = prevLogTerm;
        if (windowLength < windowPos + 1) {
          windowLength = windowPos + 1;
        }
        checkLog(windowPos);
        if (windowPos == 0) {
          appendedPos = flushWindow(result, leaderCommit);
        } else {
          result.status = Response.RESPONSE_WEAK_ACCEPT;
        }

        Statistic.RAFT_WINDOW_LENGTH.add(windowLength);
      } else {
        result.setStatus(Response.RESPONSE_OUT_OF_WINDOW);
        result.setHeader(member.getPartitionGroup().getHeader());
        return result;
      }
    }

    if (appendedPos == -1) {
      // the incoming log points to an illegal position, reject it
      result.status = Response.RESPONSE_LOG_MISMATCH;
    }
    return result;
  }

  @Override
  public void reset() {
    this.firstPosPrevIndex = logManager.getLastLogIndex();
    this.prevTerms[0] = logManager.getLastLogTerm();
    logWindow = new Log[windowCapacity];
    prevTerms = new long[windowCapacity];
    windowLength = 0;
  }

  public static class Factory implements LogAppenderFactory {

    @Override
    public LogAppender create(RaftMember member) {
      return new SlidingWindowLogAppender(member);
    }
  }
}
