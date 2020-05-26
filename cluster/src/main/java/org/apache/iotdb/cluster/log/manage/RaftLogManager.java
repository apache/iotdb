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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

  // manage uncommitted entries
  public UnCommittedEntryManager unCommittedEntryManager;
  // manage committed entries in memory as a cache
  public CommittedEntryManager committedEntryManager;
  // manage committed entries in disk for safety
  public StableEntryManager stableEntryManager;

  private long commitIndex;
  private LogApplier logApplier;

  public RaftLogManager(StableEntryManager stableEntryManager, LogApplier applier) {
    this.logApplier = applier;
    this.committedEntryManager = new CommittedEntryManager();
    this.stableEntryManager = stableEntryManager;
    try {
      this.committedEntryManager.append(stableEntryManager.getAllEntries());
    } catch (TruncateCommittedEntryException e) {
      logger.error("Unexpected error:", e);
    }
    long last = committedEntryManager.getLastIndex();
    this.unCommittedEntryManager = new UnCommittedEntryManager(last + 1);
    // must have applied entry [compactIndex,last] to state machine
    this.commitIndex = last;
  }

  // placeholder method
  public Snapshot getSnapshot() {
    return null;
  }

  // placeholder method
  public void takeSnapshot() throws IOException {

  }

  /**
   * Return the logManager's logApplier.
   *
   * @return logApplier
   */
  public LogApplier getApplier() {
    return logApplier;
  }

  /**
   * Update the raftNode's hardState(currentTerm,voteFor) and flush to disk.
   *
   * @param state
   */
  public void updateHardState(HardState state) {
    stableEntryManager.setHardStateAndFlush(state);
  }

  /**
   * Return the raftNode's hardState(currentTerm,voteFor).
   *
   * @return state
   */
  public HardState getHardState() {
    return stableEntryManager.getHardState();
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
    return committedEntryManager.getFirstIndex();
  }

  /**
   * Return the last entry's index which have been added into log module.
   *
   * @return lastIndex
   */
  public long getLastLogIndex() {
    long last = unCommittedEntryManager.maybeLastIndex();
    if (last != -1) {
      return last;
    }
    return committedEntryManager.getLastIndex();
  }

  /**
   * Returns the term for given index.
   *
   * @param index request entry index
   * @return throw EntryCompactedException if index < dummyIndex, throw EntryUnavailableException if
   * index > lastIndex, otherwise return the entry's term for given index
   * @throws EntryUnavailableException
   * @throws EntryCompactedException
   */
  public long getTerm(long index) throws EntryUnavailableException, EntryCompactedException {
    long dummyIndex = getFirstIndex() - 1;
    if (index < dummyIndex) {
      logger.info("invalid getTerm: parameter: index({}) < compactIndex({})", index,
          dummyIndex);
      throw new EntryCompactedException(index, dummyIndex);
    }
    long lastIndex = getLastLogIndex();
    if (index > lastIndex) {
      logger.info("invalid getTerm: parameter: index({}) > lastIndex({})", index, lastIndex);
      throw new EntryUnavailableException(index, lastIndex);
    }
    if (index >= unCommittedEntryManager.getFirstUnCommittedIndex()) {
      return unCommittedEntryManager.maybeTerm(index);
    }
    return committedEntryManager.maybeTerm(index);
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
      logger.error("unexpected error when getting the last term : {}", e.getMessage());
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
      logger.error("unexpected error when getting the last term : {}", e.getMessage());
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
        logger
            .error("entry {} conflict with committed entry [commitIndex({})]", ci,
                commitIndex);
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
            .error("entry {} conflict with committed entry [commitIndex({})]",
                entry.getCurrLogIndex(),
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
      logger.error("after({}) is out of range [commitIndex({})]", after, commitIndex);
      return -1;
    }
    unCommittedEntryManager.truncateAndAppend(entries);
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
      logger.error("after({}) is out of range [commitIndex({})]", after, commitIndex);
      return -1;
    }
    unCommittedEntryManager.truncateAndAppend(entry);
    return getLastLogIndex();
  }

  /**
   * Used by leader node to try to commit entries.
   *
   * @param leaderCommit leader's commitIndex
   * @param term         the entry's term which index is leaderCommit in leader's log module
   * @return true or false
   */
  public boolean maybeCommit(long leaderCommit, long term) {
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
   * Return whether the entry with certain index is available in log module.
   *
   * @param index request index
   * @return true or false
   */
  public boolean logValid(long index) {
    return index >= getFirstIndex() && index <= getLastLogIndex();
  }

  /**
   * Overwrites the contents of this object with those of the given snapshot.
   *
   * @param snapshot leader's snapshot
   */
  public void applyingSnapshot(Snapshot snapshot) {
    logger.info("log module starts to restore snapshot [index: {}, term: {}]",
        snapshot.getLastLogIndex(), snapshot.getLastLogTerm());
    try {
      committedEntryManager.compactEntries(snapshot.getLastLogIndex());
      stableEntryManager.removeCompactedEntries(snapshot.getLastLogIndex());
    } catch (EntryUnavailableException e) {
      committedEntryManager.applyingSnapshot(snapshot);
      stableEntryManager.applyingSnapshot(snapshot);
      unCommittedEntryManager.applyingSnapshot(snapshot);
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
    long offset = unCommittedEntryManager.getFirstUnCommittedIndex();
    if (low < offset) {
      entries.addAll(committedEntryManager.getEntries(low, Math.min(high, offset)));
    }
    if (high > offset) {
      entries.addAll(unCommittedEntryManager.getEntries(Math.max(low, offset), high));
    }
    return entries;
  }

  /**
   * Used by MaybeCommit or MaybeAppend or follower to commit newly committed entries.
   *
   * @param newCommitIndex request commitIndex
   */
  public void commitTo(long newCommitIndex, boolean ignoreExecutionExceptions) throws LogExecutionException {
    if (commitIndex < newCommitIndex) {
      long lo = unCommittedEntryManager.getFirstUnCommittedIndex();
      long hi = newCommitIndex + 1;
      List<Log> entries = new ArrayList<>(unCommittedEntryManager
          .getEntries(lo, hi));
      if (!entries.isEmpty()) {
        if (getCommitLogIndex() >= entries.get(0).getCurrLogIndex()) {
          entries
              .subList(0,
                  (int) (getCommitLogIndex() - entries.get(0).getCurrLogIndex() + 1))
              .clear();
        }
        try {
          committedEntryManager.append(entries);
          stableEntryManager.append(entries);
          applyEntries(entries, ignoreExecutionExceptions);
          Log lastLog = entries.get(entries.size() - 1);
          unCommittedEntryManager.stableTo(lastLog.getCurrLogIndex());
          commitIndex = lastLog.getCurrLogIndex();
        } catch (TruncateCommittedEntryException e) {
          logger.error("Unexpected error:", e);
        }
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
   * @param entries applying entries
   * @param ignoreExecutionException when set to true, exceptions during applying the logs are
   *                                 ignored, otherwise they are reported to the upper level
   */
  protected void applyEntries(List<Log> entries, boolean ignoreExecutionException) throws LogExecutionException {
    for (Log entry : entries) {
      try {
        logApplier.apply(entry);
      } catch (Exception e) {
        if (ignoreExecutionException) {
          logger.error("Cannot apply a log {} in snapshot, ignored", entry, e);
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
      logger.error("invalid getEntries: parameter: {} > {}", low, high);
      throw new GetEntriesWrongParametersException(low, high);
    }
    long first = getFirstIndex();
    if (low < first) {
      logger.error("CheckBound out of index: parameter: {} , lower bound: {} ", low, high);
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
    this.committedEntryManager = committedEntryManager;
    this.stableEntryManager = stableEntryManager;
    this.logApplier = applier;
    long last = committedEntryManager.getLastIndex();
    this.unCommittedEntryManager = new UnCommittedEntryManager(last + 1);
    this.commitIndex = last;
  }

  public void close() {
    stableEntryManager.close();
  }
}
