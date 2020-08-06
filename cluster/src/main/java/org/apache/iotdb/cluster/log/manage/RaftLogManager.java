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

package org.apache.iotdb.cluster.log.manage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.GetEntriesWrongParametersException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.StableEntryManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

  // manage uncommitted entries
  private UnCommittedEntryManager unCommittedEntryManager;
  // manage committed entries in memory as a cache
  private CommittedEntryManager committedEntryManager;
  // manage committed entries in disk for safety
  private StableEntryManager stableEntryManager;

  private long commitIndex;
  private LogApplier logApplier;
  // to distinguish managers of different members
  private String name;

  private ScheduledExecutorService deleteLogExecutorService;
  private ScheduledFuture<?> deleteLogFuture;

  private ScheduledExecutorService flushLogExecutorService;

  /**
   * minimum number of committed logs in memory
   */
  private int minNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig()
      .getMinNumOfLogsInMem();

  /**
   * maximum number of committed logs in memory
   */
  private int maxNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig()
      .getMaxNumOfLogsInMem();


  public RaftLogManager(StableEntryManager stableEntryManager, LogApplier applier, String name) {
    this.logApplier = applier;
    this.name = name;
    this.setCommittedEntryManager(new CommittedEntryManager(maxNumOfLogsInMem));
    this.setStableEntryManager(stableEntryManager);
    try {
      this.getCommittedEntryManager().append(stableEntryManager.getAllEntries());
    } catch (TruncateCommittedEntryException e) {
      logger.error("{}: Unexpected error:", name, e);
    }
    long last = getCommittedEntryManager().getLastIndex();
    this.setUnCommittedEntryManager(new UnCommittedEntryManager(last + 1));
    // must have applied entry [compactIndex,last] to state machine
    this.commitIndex = last;

    deleteLogExecutorService = new ScheduledThreadPoolExecutor(1,
        new BasicThreadFactory.Builder().namingPattern("raft-log-delete-%d").daemon(true)
            .build());
    /**
     * deletion check period of the submitted log
     */
    int logDeleteCheckIntervalSecond = ClusterDescriptor.getInstance().getConfig()
        .getLogDeleteCheckIntervalSecond();

    deleteLogFuture = deleteLogExecutorService
        .scheduleAtFixedRate(this::checkDeleteLog, logDeleteCheckIntervalSecond,
            logDeleteCheckIntervalSecond,
            TimeUnit.SECONDS);

    /**
     * flush log to file periodically
     */
    int logFlushTimeIntervalMS = ClusterDescriptor.getInstance().getConfig()
        .getForceRaftLogPeriodInMS();
    if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
      if (flushLogExecutorService == null) {
        flushLogExecutorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("raft-log-write-%d").daemon(true)
                .build());
      }
      flushLogExecutorService
          .scheduleAtFixedRate(this::flushLogPeriodically, logFlushTimeIntervalMS,
              logFlushTimeIntervalMS, TimeUnit.MILLISECONDS);
    }
  }

  public abstract Snapshot getSnapshot();

  public abstract void takeSnapshot() throws IOException;

  /**
   * Update the raftNode's hardState(currentTerm,voteFor) and flush to disk.
   *
   * @param state
   */
  public void updateHardState(HardState state) {
    getStableEntryManager().setHardStateAndFlush(state);
  }

  /**
   * Return the raftNode's hardState(currentTerm,voteFor).
   *
   * @return state
   */
  public HardState getHardState() {
    return getStableEntryManager().getHardState();
  }

  /**
   * Return the raftNode's commitIndex.
   *
   * @return commitIndex
   */
  public long getCommitLogIndex() {
    return commitIndex;
  }

  /**
   * Return the first entry's index which have not been compacted.
   *
   * @return firstIndex
   */
  public long getFirstIndex() {
    return getCommittedEntryManager().getFirstIndex();
  }

  /**
   * Return the last entry's index which have been added into log module.
   *
   * @return lastIndex
   */
  public long getLastLogIndex() {
    long last = getUnCommittedEntryManager().maybeLastIndex();
    if (last != -1) {
      return last;
    }
    return getCommittedEntryManager().getLastIndex();
  }

  /**
   * Returns the term for given index.
   *
   * @param index request entry index
   * @return throw EntryCompactedException if index < dummyIndex, -1 if index > lastIndex or the
   * entry is compacted, otherwise return the entry's term for given index
   * @throws EntryCompactedException
   */
  public long getTerm(long index) throws EntryCompactedException {
    long dummyIndex = getFirstIndex() - 1;
    if (index < dummyIndex) {
      return -1;
    }
    long lastIndex = getLastLogIndex();
    if (index > lastIndex) {
      return -1;
    }
    if (index >= getUnCommittedEntryManager().getFirstUnCommittedIndex()) {
      return getUnCommittedEntryManager().maybeTerm(index);
    }
    return getCommittedEntryManager().maybeTerm(index);
  }

  /**
   * Return the last entry's term. If it goes wrong, there must be an unexpected exception.
   *
   * @return last entry's term
   */
  public long getLastLogTerm() {
    long term = -1;
    try {
      term = getTerm(getLastLogIndex());
    } catch (Exception e) {
      logger.error("{}: unexpected error when getting the last term : {}", name, e.getMessage());
    }
    return term;
  }

  /**
   * Return the commitIndex's term. If it goes wrong, there must be an unexpected exception.
   *
   * @return commitIndex's term
   */
  public long getCommitLogTerm() {
    long term = -1;
    try {
      term = getTerm(getCommitLogIndex());
    } catch (Exception e) {
      logger.error("{}: unexpected error when getting the last term : {}", name, e.getMessage());
    }
    return term;
  }

  /**
   * Used by follower node to support leader's complicated log replication rpc parameters and try to
   * commit entries.
   *
   * @param lastIndex    leader's matchIndex for this follower node
   * @param lastTerm     the entry's term which index is leader's matchIndex for this follower node
   * @param leaderCommit leader's commitIndex
   * @param entries      entries sent from the leader node Note that the leader must ensure
   *                     entries[0].index = lastIndex + 1
   * @return -1 if the entries cannot be appended, otherwise the last index of new entries
   */
  public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, List<Log> entries) {
    if (matchTerm(lastTerm, lastIndex)) {
      long newLastIndex = lastIndex + entries.size();
      long ci = findConflict(entries);
      if (ci <= commitIndex) {
        if (ci != -1) {
          logger
              .error("{}: entry {} conflict with committed entry [commitIndex({})]", name, ci,
                  commitIndex);
        } else {
          if (logger.isDebugEnabled() && !entries.isEmpty()) {
            logger.debug("{}: Appending entries [{} and other {} logs] all exist locally",
                name, entries.get(0), entries.size() - 1);
          }
        }

      } else {
        long offset = lastIndex + 1;
        append(entries.subList((int) (ci - offset), entries.size()));
      }
      try {
        commitTo(Math.min(leaderCommit, newLastIndex), true);
      } catch (LogExecutionException e) {
        // exceptions are ignored on follower side
      }
      return newLastIndex;
    }
    return -1;
  }

  /**
   * Used by follower node to support leader's complicated log replication rpc parameters and try to
   * commit entry.
   *
   * @param lastIndex    leader's matchIndex for this follower node
   * @param lastTerm     the entry's term which index is leader's matchIndex for this follower node
   * @param leaderCommit leader's commitIndex
   * @param entry        entry sent from the leader node
   * @return -1 if the entries cannot be appended, otherwise the last index of new entries
   */
  public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, Log entry) {
    if (matchTerm(lastTerm, lastIndex)) {
      long newLastIndex = lastIndex + 1;
      if (entry.getCurrLogIndex() <= commitIndex) {
        logger
            .error("{}: entry {} conflict with committed entry [commitIndex({})]",
                name, entry.getCurrLogIndex(),
                commitIndex);
      } else {
        append(entry);
      }
      try {
        commitTo(Math.min(leaderCommit, newLastIndex), true);
      } catch (LogExecutionException e) {
        // exceptions are ignored on follower side
      }
      return newLastIndex;
    }
    return -1;
  }

  /**
   * Used by leader node or MaybeAppend to directly append to unCommittedEntryManager. Note that the
   * caller should ensure entries[0].index > committed.
   *
   * @param entries appendingEntries
   * @return the newly generated lastIndex
   */
  public long append(List<Log> entries) {
    if (entries.isEmpty()) {
      return getLastLogIndex();
    }
    long after = entries.get(0).getCurrLogIndex();
    if (after <= commitIndex) {
      logger.error("{}: after({}) is out of range [commitIndex({})]", name, after, commitIndex);
      return -1;
    }
    getUnCommittedEntryManager().truncateAndAppend(entries);
    return getLastLogIndex();
  }

  /**
   * Used by leader node to directly append to unCommittedEntryManager. Note that the caller should
   * ensure entry.index > committed.
   *
   * @param entry appendingEntry
   * @return the newly generated lastIndex
   */
  public long append(Log entry) {
    long after = entry.getCurrLogIndex();
    if (after <= commitIndex) {
      logger.error("{}: after({}) is out of range [commitIndex({})]", name, after, commitIndex);
      return -1;
    }
    getUnCommittedEntryManager().truncateAndAppend(entry);
    return getLastLogIndex();
  }

  /**
   * Used by leader node to try to commit entries.
   *
   * @param leaderCommit leader's commitIndex
   * @param term         the entry's term which index is leaderCommit in leader's log module
   * @return true or false
   */
  public synchronized boolean maybeCommit(long leaderCommit, long term) {
    if (leaderCommit > commitIndex && matchTerm(term, leaderCommit)) {
      try {
        commitTo(leaderCommit, true);
      } catch (LogExecutionException e) {
        // exceptions are ignored on follower side
      }
      return true;
    }
    return false;
  }

  /**
   * Overwrites the contents of this object with those of the given snapshot.
   *
   * @param snapshot leader's snapshot
   */
  public void applyingSnapshot(Snapshot snapshot) {
    logger.info("{}: log module starts to restore snapshot [index: {}, term: {}]",
        name, snapshot.getLastLogIndex(), snapshot.getLastLogTerm());
    try {
      getCommittedEntryManager().compactEntries(snapshot.getLastLogIndex());
      getStableEntryManager().removeCompactedEntries(snapshot.getLastLogIndex());
    } catch (EntryUnavailableException e) {
      getCommittedEntryManager().applyingSnapshot(snapshot);
      getUnCommittedEntryManager().applyingSnapshot(snapshot);
    }
    if (this.commitIndex < snapshot.getLastLogIndex()) {
      this.commitIndex = snapshot.getLastLogIndex();
    }
  }

  /**
   * Determines if the given (lastTerm, lastIndex) log is more up-to-date by comparing the index and
   * term of the last entries in the existing logs. If the logs have last entries with different
   * terms, then the log with the later term is more up-to-date. If the logs end with the same term,
   * then whichever log has the larger lastIndex is more up-to-date. If the logs are the same, the
   * given log is up-to-date.
   *
   * @param lastTerm  candidate's lastTerm
   * @param lastIndex candidate's lastIndex
   * @return true or false
   */
  public boolean isLogUpToDate(long lastTerm, long lastIndex) {
    return lastTerm > getLastLogTerm() || (lastTerm == getLastLogTerm()
        && lastIndex >= getLastLogIndex());
  }

  /**
   * Pack entries from low through high - 1, just like slice (entries[low:high]). firstIndex <= low
   * <= high <= lastIndex.
   *
   * @param low  request index low bound
   * @param high request index upper bound
   * @throws EntryCompactedException
   * @throws GetEntriesWrongParametersException
   */
  public List<Log> getEntries(long low, long high)
      throws EntryCompactedException, GetEntriesWrongParametersException {
    checkBound(low, high);
    List<Log> entries = new ArrayList<>();
    long offset = getUnCommittedEntryManager().getFirstUnCommittedIndex();
    if (low < offset) {
      entries.addAll(getCommittedEntryManager().getEntries(low, Math.min(high, offset)));
    }
    if (high > offset) {
      entries.addAll(getUnCommittedEntryManager().getEntries(Math.max(low, offset), high));
    }
    return entries;
  }

  /**
   * Used by MaybeCommit or MaybeAppend or follower to commit newly committed entries.
   *
   * @param newCommitIndex request commitIndex
   */
  public void commitTo(long newCommitIndex, boolean ignoreExecutionExceptions)
      throws LogExecutionException {
    if (commitIndex >= newCommitIndex) {
      return;
    }

    long lo = getUnCommittedEntryManager().getFirstUnCommittedIndex();
    long hi = newCommitIndex + 1;
    List<Log> entries = new ArrayList<>(getUnCommittedEntryManager()
        .getEntries(lo, hi));
    if (!entries.isEmpty()) {
      if (getCommitLogIndex() >= entries.get(0).getCurrLogIndex()) {
        entries
            .subList(0,
                (int) (getCommitLogIndex() - entries.get(0).getCurrLogIndex() + 1))
            .clear();
      }
      try {
        if (committedEntryManager.getTotalSize() + entries.size() > maxNumOfLogsInMem) {
          synchronized (this) {
            innerDeleteLog();
          }
        }
        getCommittedEntryManager().append(entries);
        if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
          getStableEntryManager().append(entries);
        }
        Log lastLog = entries.get(entries.size() - 1);
        getUnCommittedEntryManager().stableTo(lastLog.getCurrLogIndex());
        commitIndex = lastLog.getCurrLogIndex();
        applyEntries(entries, ignoreExecutionExceptions);
      } catch (TruncateCommittedEntryException e) {
        logger.error("{}: Unexpected error:", name, e);
      } catch (IOException e) {
        throw new LogExecutionException(e);
      }
    }
  }

  /**
   * Returns whether the index and term passed in match.
   *
   * @param term  request entry term
   * @param index request entry index
   * @return true or false
   */
  public boolean matchTerm(long term, long index) {
    long t;
    try {
      t = getTerm(index);
    } catch (Exception e) {
      return false;
    }
    return t == term;
  }

  /**
   * Used by commitTo to apply newly committed entries
   *
   * @param entries                  applying entries
   * @param ignoreExecutionException when set to true, exceptions during applying the logs are
   *                                 ignored, otherwise they are reported to the upper level
   */
  protected void applyEntries(List<Log> entries, boolean ignoreExecutionException)
      throws LogExecutionException {
    for (Log entry : entries) {
      try {
        logApplier.apply(entry);
      } catch (Exception e) {
        if (ignoreExecutionException) {
          logger.error("{}: Cannot apply a log {}, ignored", name, entry, e);
        } else {
          throw new LogExecutionException(e);
        }
      }
    }
  }

  /**
   * Check whether the parameters passed in satisfy the following properties. firstIndex <= low <=
   * high.
   *
   * @param low  request index low bound
   * @param high request index upper bound
   * @throws EntryCompactedException
   * @throws GetEntriesWrongParametersException
   */
  protected void checkBound(long low, long high)
      throws EntryCompactedException, GetEntriesWrongParametersException {
    if (low > high) {
      logger.error("{}: invalid getEntries: parameter: {} > {}", name, low, high);
      throw new GetEntriesWrongParametersException(low, high);
    }
    long first = getFirstIndex();
    if (low < first) {
      logger.error("{}: CheckBound out of index: parameter: {} , lower bound: {} ", name, low,
          high);
      throw new EntryCompactedException(low, first);
    }
  }

  /**
   * findConflict finds the index of the conflict. It returns the first pair of conflicting entries
   * between the existing entries and the given entries, if there are any. If there is no
   * conflicting entries, and the existing entries contains all the given entries, -1 will be
   * returned. If there is no conflicting entries, but the given entries contains new entries, the
   * index of the first new entry will be returned. An entry is considered to be conflicting if it
   * has the same index but a different term. The index of the given entries MUST be continuously
   * increasing.
   *
   * @param entries request entries
   * @return -1 or conflictIndex
   */
  protected long findConflict(List<Log> entries) {
    for (Log entry : entries) {
      if (!matchTerm(entry.getCurrLogTerm(), entry.getCurrLogIndex())) {
        if (entry.getCurrLogIndex() <= getLastLogIndex()) {
          logger.info("found conflict at index {}",
              entry.getCurrLogIndex());
        }
        return entry.getCurrLogIndex();
      }
    }
    return -1;
  }

  @TestOnly
  public RaftLogManager(CommittedEntryManager committedEntryManager,
      StableEntryManager stableEntryManager,
      LogApplier applier) {
    this.setCommittedEntryManager(committedEntryManager);
    this.setStableEntryManager(stableEntryManager);
    this.logApplier = applier;
    long last = committedEntryManager.getLastIndex();
    this.setUnCommittedEntryManager(new UnCommittedEntryManager(last + 1));
    this.commitIndex = last;
  }

  @TestOnly
  public void setMinNumOfLogsInMem(int minNumOfLogsInMem) {
    this.minNumOfLogsInMem = minNumOfLogsInMem;
  }

  public void close() {
    getStableEntryManager().close();
    if (deleteLogExecutorService != null) {
      deleteLogExecutorService.shutdownNow();
      deleteLogFuture.cancel(true);
      try {
        deleteLogExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Close check log thread interrupted");
      }
      deleteLogExecutorService = null;
    }
    if (flushLogExecutorService != null) {
      flushLogExecutorService.shutdown();
      try {
        flushLogExecutorService.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("force flush raft log thread still doesn't exit after 30s.");
        Thread.currentThread().interrupt();
      }
      flushLogExecutorService = null;
    }
  }

  public UnCommittedEntryManager getUnCommittedEntryManager() {
    return unCommittedEntryManager;
  }

  public void setUnCommittedEntryManager(
      UnCommittedEntryManager unCommittedEntryManager) {
    this.unCommittedEntryManager = unCommittedEntryManager;
  }

  public CommittedEntryManager getCommittedEntryManager() {
    return committedEntryManager;
  }

  public void setCommittedEntryManager(
      CommittedEntryManager committedEntryManager) {
    this.committedEntryManager = committedEntryManager;
  }

  public StableEntryManager getStableEntryManager() {
    return stableEntryManager;
  }

  public void setStableEntryManager(StableEntryManager stableEntryManager) {
    this.stableEntryManager = stableEntryManager;
  }


  /**
   * check whether delete the committed log
   */
  public void checkDeleteLog() {
    synchronized (this) {
      if (committedEntryManager.getTotalSize() <= minNumOfLogsInMem) {
        return;
      }
      innerDeleteLog();
    }
  }

  public void flushLogPeriodically() {
    synchronized (this) {
      getStableEntryManager().forceFlushLogBuffer();
    }
  }

  private void innerDeleteLog() {
    long removeSize = committedEntryManager.getTotalSize() - minNumOfLogsInMem;
    long compactIndex = committedEntryManager.getDummyIndex() + removeSize;
    try {
      logger.info(
          "{}: Before compaction index {}-{}, compactIndex {}, removeSize {}, committedLogSize {}",
          name, getFirstIndex(), getLastLogIndex(), compactIndex, removeSize,
          committedEntryManager.getTotalSize());
      getCommittedEntryManager().compactEntries(compactIndex);
      if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
        getStableEntryManager().removeCompactedEntries(compactIndex);
      }
      logger.info("{}: After compaction index {}-{}, committedLogSize {}", name,
          getFirstIndex(), getLastLogIndex(), committedEntryManager.getTotalSize());
    } catch (EntryUnavailableException e) {
      logger.error("{}: regular compact log entries failed, error={}", name, e.getMessage());
    }
  }
}
