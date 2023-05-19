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
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SlidingWindowLogAppender implements LogAppender {

  private static final Logger logger = LoggerFactory.getLogger(SlidingWindowLogAppender.class);

  private int windowCapacity;
  private int windowLength = 0;
  private Entry[] logWindow;
  private long firstPosPrevIndex;
  private RaftMember member;
  private RaftLogManager logManager;
  private RaftConfig config;
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public SlidingWindowLogAppender(RaftMember member, RaftConfig config) {
    this.member = member;
    this.logManager = member.getLogManager();
    windowCapacity = config.getMaxNumOfLogsInMem();
    logWindow = new Entry[windowCapacity];
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
    long prevLogTerm = logWindow[pos].getPrevTerm();
    if (pos > 0 && logWindow[pos - 1] != null) {
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
    if (pos < windowCapacity - 1 && logWindow[pos + 1] != null) {
      long nextPrevTerm = logWindow[pos + 1].getPrevTerm();
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
  private boolean flushWindow(AppendEntryResult result) {

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
    boolean success;
    while (true) {
      // TODO: Consider memory footprint to execute a precise rejection
      if ((logManager.getCommitLogIndex() - logManager.getAppliedIndex())
          <= config.getUnAppliedRaftLogNumForRejectThreshold()) {
        success = logManager.maybeAppend(logs);
        Statistic.RAFT_RECEIVER_WINDOW_FLUSH_SIZE.add(logs.size());
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(10);
        if (System.currentTimeMillis() - startWaitingTime
            > config.getMaxWaitingTimeWhenInsertBlocked()) {
          result.status = Response.RESPONSE_TOO_BUSY;
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (success) {
      moveWindowRightward(flushPos, logs.get(logs.size() - 1).getCurrLogIndex());
      result.status = Response.RESPONSE_STRONG_ACCEPT;
      return true;
    } else {
      result.status = Response.RESPONSE_LOG_MISMATCH;
      logger.warn("Cannot flush the window to log");
      return false;
    }
  }

  private void moveWindowRightward(int step, long newIndex) {
    System.arraycopy(logWindow, step, logWindow, 0, windowCapacity - step);
    for (int i = 1; i <= step; i++) {
      logWindow[windowCapacity - i] = null;
    }
    firstPosPrevIndex = newIndex;
  }

  private void moveWindowLeftward(int step) {
    int length = Math.max(windowCapacity - step, 0);
    System.arraycopy(logWindow, 0, logWindow, step, length);
    for (int i = 0; i < length; i++) {
      logWindow[i] = null;
    }
    firstPosPrevIndex = logManager.getLastLogIndex();
  }

  @Override
  public AppendEntryResult appendEntries(long leaderCommit, long term, List<Entry> entries) {
    if (entries.isEmpty()) {
      return new AppendEntryResult(Response.RESPONSE_AGREE)
          .setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
    }

    AppendEntryResult result;
    result = appendEntries(leaderCommit, entries);
    return result;
  }

  private AppendEntryResult appendEntry(long leaderCommit, Entry entry) {
    boolean appended = false;

    AppendEntryResult result = new AppendEntryResult();
    long prevLogIndex = entry.getCurrLogIndex() - 1;
    long startTime = Statistic.RAFT_RECEIVER_WAIT_FOR_WINDOW.getOperationStartTime();
    synchronized (this) {
      Statistic.RAFT_RECEIVER_WAIT_FOR_WINDOW.calOperationCostTimeFromStart(startTime);
      int windowPos = (int) (prevLogIndex - firstPosPrevIndex);
      if (windowPos < 0) {
        // the new entry may replace an appended entry
        appended = logManager.maybeAppend(Collections.singletonList(entry));
        moveWindowLeftward(-windowPos);
        result.status = Response.RESPONSE_STRONG_ACCEPT;
      } else if (windowPos < windowCapacity) {
        // the new entry falls into the window
        logWindow[windowPos] = entry;
        if (windowLength < windowPos + 1) {
          windowLength = windowPos + 1;
        }
        checkLog(windowPos);
        if (windowPos == 0) {
          appended = flushWindow(result);
          Statistic.RAFT_FOLLOWER_STRONG_ACCEPT.calOperationCostTimeFromStart(startTime);
        } else {
          result.status = Response.RESPONSE_WEAK_ACCEPT;
          Statistic.RAFT_FOLLOWER_WEAK_ACCEPT.calOperationCostTimeFromStart(startTime);
        }
      } else {
        result.setStatus(Response.RESPONSE_OUT_OF_WINDOW);
        result.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
        return result;
      }
    }
    Statistic.RAFT_RECEIVER_APPEND_ONE_ENTRY_SYNC.calOperationCostTimeFromStart(startTime);

    if (appended) {
      long startTime1 = Statistic.RAFT_RECEIVER_UPDATE_COMMIT_INDEX.getOperationStartTime();
      result.setLastLogIndex(logManager.getLastEntryIndexUnsafe());
      member.tryUpdateCommitIndex(member.getStatus().getTerm().get(), leaderCommit, -1);
      Statistic.RAFT_RECEIVER_UPDATE_COMMIT_INDEX.calOperationCostTimeFromStart(startTime1);
    }
    return result;
  }

  private AppendEntryResult appendEntries(long leaderCommit, List<Entry> entries) {
    boolean appended = false;

    AppendEntryResult result = new AppendEntryResult();
    Collections.reverse(entries);
    long startTime = Statistic.RAFT_RECEIVER_WAIT_FOR_WINDOW.getOperationStartTime();
    synchronized (this) {
      Statistic.RAFT_RECEIVER_WAIT_FOR_WINDOW.calOperationCostTimeFromStart(startTime);
      for (Entry entry : entries) {
        long prevLogIndex = entry.getCurrLogIndex() - 1;
        long entryStartTime = Statistic.RAFT_RECEIVER_WAIT_FOR_WINDOW.getOperationStartTime();
        int windowPos = (int) (prevLogIndex - firstPosPrevIndex);
        if (windowPos < 0) {
          // the new entry may replace an appended entry
          appended = logManager.maybeAppend(Collections.singletonList(entry));
          moveWindowLeftward(-windowPos);
          result.status = Response.RESPONSE_STRONG_ACCEPT;
        } else if (windowPos < windowCapacity) {
          // the new entry falls into the window
          logWindow[windowPos] = entry;
          if (windowLength < windowPos + 1) {
            windowLength = windowPos + 1;
          }
          checkLog(windowPos);
          if (windowPos == 0) {
            appended = flushWindow(result);
            Statistic.RAFT_FOLLOWER_STRONG_ACCEPT.calOperationCostTimeFromStart(entryStartTime);
          } else {
            result.status = Response.RESPONSE_WEAK_ACCEPT;
            Statistic.RAFT_FOLLOWER_WEAK_ACCEPT.calOperationCostTimeFromStart(entryStartTime);
          }
        } else {
          result.setStatus(Response.RESPONSE_OUT_OF_WINDOW);
          result.setGroupId(member.getRaftGroupId().convertToTConsensusGroupId());
          return result;
        }
        Statistic.RAFT_RECEIVER_APPEND_ONE_ENTRY_SYNC.calOperationCostTimeFromStart(entryStartTime);
      }
    }
    Statistic.RAFT_RECEIVER_APPEND_ENTRIES.calOperationCostTimeFromStart(startTime);

    if (appended) {
      long startTime1 = Statistic.RAFT_RECEIVER_UPDATE_COMMIT_INDEX.getOperationStartTime();
      result.setLastLogIndex(logManager.getLastEntryIndexUnsafe());
      member.tryUpdateCommitIndex(member.getStatus().getTerm().get(), leaderCommit, -1);
      Statistic.RAFT_RECEIVER_UPDATE_COMMIT_INDEX.calOperationCostTimeFromStart(startTime1);
    }
    return result;
  }

  public void windowInsertion() {}

  @Override
  public void reset() {
    this.firstPosPrevIndex = logManager.getLastLogIndex();
    logWindow = new Entry[windowCapacity];
    windowLength = 0;
  }

  public static class Factory implements LogAppenderFactory {

    @Override
    public LogAppender create(RaftMember member, RaftConfig config) {
      return new SlidingWindowLogAppender(member, config);
    }
  }
}
