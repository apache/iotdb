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
import org.apache.iotdb.consensus.natraft.protocol.log.manager.RaftLogManager;
import org.apache.iotdb.consensus.natraft.utils.Response;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SlidingWindowLogAppender implements LogAppender {

  private static final Logger logger = LoggerFactory.getLogger(SlidingWindowLogAppender.class);

  private int windowCapacity;
  private int windowLength = 0;
  private Entry[] logWindow;
  private long firstPosPrevIndex;
  private long[] prevTerms;

  private RaftMember member;
  private RaftLogManager logManager;
  private RaftConfig config;

  public SlidingWindowLogAppender(RaftMember member, RaftConfig config) {
    this.member = member;
    this.logManager = member.getLogManager();
    windowCapacity = config.getMaxNumOfLogsInMem();
    logWindow = new Entry[windowCapacity];
    prevTerms = new long[windowCapacity];
    this.config = config;
    reset();
  }

  /**
   * After insert an entry into the window, check if its previous and latter entries should be
   * removed if it mismatches.
   */
  private void checkLog(int pos) {
    checkLogPrev(pos);
    checkLogNext(pos);
  }

  private void checkLogPrev(int pos) {
    // check the previous entry
    long prevLogTerm = prevTerms[pos];
    if (pos > 0) {
      Entry prev = logWindow[pos - 1];
      if (prev != null && prev.getCurrLogTerm() != prevLogTerm) {
        logWindow[pos - 1] = null;
      }
    }
  }

  private void checkLogNext(int pos) {
    // check the next entry
    Entry entry = logWindow[pos];
    boolean nextMismatch = false;
    if (pos < windowCapacity - 1) {
      long nextPrevTerm = prevTerms[pos + 1];
      if (nextPrevTerm != entry.getCurrLogTerm()) {
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
    List<Entry> logs = Arrays.asList(logWindow).subList(0, flushPos);
    logger.debug(
        "Flushing {} entries to log, first {}, last {}",
        logs.size(),
        logs.get(0),
        logs.get(logs.size() - 1));

    long startWaitingTime = System.currentTimeMillis();
    long success;
    while (true) {
      // TODO: Consider memory footprint to execute a precise rejection
      if ((logManager.getCommitLogIndex() - logManager.getAppliedIndex())
          <= config.getUnAppliedRaftLogNumForRejectThreshold()) {
        success = logManager.maybeAppend(windowPrevLogIndex, windowPrevLogTerm, logs);
        member.tryUpdateCommitIndex(
            member.getStatus().getTerm().get(), leaderCommit, logManager.getTerm(leaderCommit));
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(10);
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          result.status = Response.RESPONSE_TOO_BUSY;
          return -1;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
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
  public AppendEntryResult appendEntries(
      long prevLogIndex, long prevLogTerm, long leaderCommit, long term, List<Entry> entries) {
    if (entries.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    AppendEntryResult result = null;
    for (Entry entry : entries) {
      result = appendEntry(prevLogIndex, prevLogTerm, leaderCommit, entry);

      if (result.status != Response.RESPONSE_AGREE
          && result.status != Response.RESPONSE_STRONG_ACCEPT
          && result.status != Response.RESPONSE_WEAK_ACCEPT) {
        return result;
      }
      prevLogIndex = entry.getCurrLogIndex();
      prevLogTerm = entry.getCurrLogTerm();
    }

    return result;
  }

  private AppendEntryResult appendEntry(
      long prevLogIndex, long prevLogTerm, long leaderCommit, Entry entry) {
    long appendedPos = 0;

    AppendEntryResult result = new AppendEntryResult();
    synchronized (this) {
      int windowPos = (int) (entry.getCurrLogIndex() - logManager.getLastLogIndex() - 1);
      if (windowPos < 0) {
        // the new entry may replace an appended entry
        appendedPos =
            logManager.maybeAppend(prevLogIndex, prevLogTerm, Collections.singletonList(entry));
        member.tryUpdateCommitIndex(
            member.getStatus().getTerm().get(), leaderCommit, logManager.getTerm(leaderCommit));
        result.status = Response.RESPONSE_STRONG_ACCEPT;
        result.setLastLogIndex(logManager.getLastLogIndex());
        result.setLastLogTerm(logManager.getLastLogTerm());
        moveWindowLeftward(-windowPos);
      } else if (windowPos < windowCapacity) {
        // the new entry falls into the window
        logWindow[windowPos] = entry;
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

      } else {
        result.setStatus(Response.RESPONSE_OUT_OF_WINDOW);
        result.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
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
    logWindow = new Entry[windowCapacity];
    prevTerms = new long[windowCapacity];
    windowLength = 0;
  }

  public static class Factory implements LogAppenderFactory {

    @Override
    public LogAppender create(RaftMember member, RaftConfig config) {
      return new SlidingWindowLogAppender(member, config);
    }
  }
}
