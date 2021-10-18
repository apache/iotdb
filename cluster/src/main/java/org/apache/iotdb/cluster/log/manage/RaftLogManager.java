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
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

  /** manage uncommitted entries */
  private UnCommittedEntryManager unCommittedEntryManager;

  /** manage committed entries in memory as a cache */
  private CommittedEntryManager committedEntryManager;

  /** manage committed entries in disk for safety */
  private StableEntryManager stableEntryManager;

  private long commitIndex;

  /**
   * The committed logs whose index is smaller than this are all have been applied, for example,
   * suppose there are 5 committed logs, whose log index is 1,2,3,4,5; if the applied sequence is
   * 1,3,2,5,4, then the maxHaveAppliedCommitIndex according is 1,1,3,3,5. This attributed is only
   * used for asyncLogApplier
   */
  private volatile long maxHaveAppliedCommitIndex;

  private final Object changeApplyCommitIndexCond = new Object();

  /**
   * The committed log whose index is larger than blockAppliedCommitIndex will be blocked. if
   * blockAppliedCommitIndex < 0(default is -1), will not block any operation.
   */
  protected volatile long blockAppliedCommitIndex;

  protected LogApplier logApplier;

  /** to distinguish managers of different members */
  private String name;

  private ScheduledExecutorService deleteLogExecutorService;
  private ScheduledFuture<?> deleteLogFuture;

  private ExecutorService checkLogApplierExecutorService;
  private Future<?> checkLogApplierFuture;

  /** minimum number of committed logs in memory */
  private int minNumOfLogsInMem =
      ClusterDescriptor.getInstance().getConfig().getMinNumOfLogsInMem();

  /** maximum number of committed logs in memory */
  private int maxNumOfLogsInMem =
      ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem();

  private long maxLogMemSize =
      ClusterDescriptor.getInstance().getConfig().getMaxMemorySizeForRaftLog();

  /**
   * Each time new logs are appended, this condition will be notified so logs that have larger
   * indices but arrived earlier can proceed.
   */
  private final Object[] logUpdateConditions = new Object[1024];

  protected List<Log> blockedUnappliedLogList;

  protected RaftLogManager(StableEntryManager stableEntryManager, LogApplier applier, String name) {
    this.logApplier = applier;
    this.name = name;
    LogManagerMeta meta = stableEntryManager.getMeta();
    this.setCommittedEntryManager(new CommittedEntryManager(maxNumOfLogsInMem, meta));
    this.setStableEntryManager(stableEntryManager);
    try {
      this.getCommittedEntryManager().append(stableEntryManager.getAllEntriesAfterAppliedIndex());
    } catch (TruncateCommittedEntryException e) {
      logger.error("{}: Unexpected error:", name, e);
    }
    long first = getCommittedEntryManager().getDummyIndex();
    long last = getCommittedEntryManager().getLastIndex();
    this.setUnCommittedEntryManager(new UnCommittedEntryManager(last + 1));
    this.getUnCommittedEntryManager()
        .truncateAndAppend(stableEntryManager.getAllEntriesAfterCommittedIndex());

    /** must have applied entry [compactIndex,last] to state machine */
    this.commitIndex = last;

    /**
     * due to the log operation is idempotent, so we can just reapply the log from the first index
     * of committed logs
     */
    this.maxHaveAppliedCommitIndex = first;

    this.blockAppliedCommitIndex = -1;

    this.blockedUnappliedLogList = new CopyOnWriteArrayList<>();

    this.deleteLogExecutorService =
        new ScheduledThreadPoolExecutor(
            1,
            new BasicThreadFactory.Builder()
                .namingPattern("raft-log-delete-" + name)
                .daemon(true)
                .build());

    this.checkLogApplierExecutorService =
        Executors.newSingleThreadExecutor(
            new BasicThreadFactory.Builder()
                .namingPattern("check-log-applier-" + name)
                .daemon(true)
                .build());

    /** deletion check period of the submitted log */
    int logDeleteCheckIntervalSecond =
        ClusterDescriptor.getInstance().getConfig().getLogDeleteCheckIntervalSecond();

    if (logDeleteCheckIntervalSecond > 0) {
      this.deleteLogFuture =
          deleteLogExecutorService.scheduleAtFixedRate(
              this::checkDeleteLog,
              logDeleteCheckIntervalSecond,
              logDeleteCheckIntervalSecond,
              TimeUnit.SECONDS);
    }

    this.checkLogApplierFuture = checkLogApplierExecutorService.submit(this::checkAppliedLogIndex);

    /** flush log to file periodically */
    if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
      this.applyAllCommittedLogWhenStartUp();
    }

    for (int i = 0; i < logUpdateConditions.length; i++) {
      logUpdateConditions[i] = new Object();
    }
  }

  public Snapshot getSnapshot() {
    return getSnapshot(-1);
  }

  public abstract Snapshot getSnapshot(long minLogIndex);

  /**
   * IMPORTANT!!!
   *
   * <p>The subclass's takeSnapshot() must call this method to insure that all logs have been
   * applied before take snapshot
   *
   * <p>
   *
   * @throws IOException timeout exception
   */
  public void takeSnapshot() throws IOException {
    if (commitIndex <= 0) {
      return;
    }
    long startTime = System.currentTimeMillis();
    if (blockAppliedCommitIndex < 0) {
      return;
    }
    logger.info(
        "{}: before take snapshot, blockAppliedCommitIndex={}, maxHaveAppliedCommitIndex={}, commitIndex={}",
        name,
        blockAppliedCommitIndex,
        maxHaveAppliedCommitIndex,
        commitIndex);
    while (blockAppliedCommitIndex > maxHaveAppliedCommitIndex) {
      long waitTime = System.currentTimeMillis() - startTime;
      if (waitTime > ClusterDescriptor.getInstance().getConfig().getCatchUpTimeoutMS()) {
        logger.error(
            "{}: wait all log applied time out, time cost={}, blockAppliedCommitIndex={}, maxHaveAppliedCommitIndex={},commitIndex={}",
            name,
            waitTime,
            blockAppliedCommitIndex,
            maxHaveAppliedCommitIndex,
            commitIndex);
        throw new IOException("wait all log applied time out");
      }
    }
  }

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
   *     entry is compacted, otherwise return the entry's term for given index
   * @throws EntryCompactedException
   */
  public long getTerm(long index) throws EntryCompactedException {
    long dummyIndex = getFirstIndex() - 1;
    if (index < dummyIndex) {
      // search in disk
      if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
        List<Log> logsInDisk = getStableEntryManager().getLogs(index, index);
        if (logsInDisk.isEmpty()) {
          return -1;
        } else {
          return logsInDisk.get(0).getCurrLogTerm();
        }
      }
      return -1;
    }

    long lastIndex = getLastLogIndex();
    if (index > lastIndex) {
      return -1;
    }

    if (index >= getUnCommittedEntryManager().getFirstUnCommittedIndex()) {
      long term = getUnCommittedEntryManager().maybeTerm(index);
      if (term != -1) {
        return term;
      }
    }

    // search in memory
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
   * @param lastIndex leader's matchIndex for this follower node
   * @param lastTerm the entry's term which index is leader's matchIndex for this follower node
   * @param leaderCommit leader's commitIndex
   * @param entries entries sent from the leader node Note that the leader must ensure
   *     entries[0].index = lastIndex + 1
   * @return -1 if the entries cannot be appended, otherwise the last index of new entries
   */
  public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, List<Log> entries) {
    if (matchTerm(lastTerm, lastIndex)) {
      long newLastIndex = lastIndex + entries.size();
      long ci = findConflict(entries);
      if (ci <= commitIndex) {
        if (ci != -1) {
          logger.error(
              "{}: entry {} conflict with committed entry [commitIndex({})]",
              name,
              ci,
              commitIndex);
        } else {
          if (logger.isDebugEnabled() && !entries.isEmpty()) {
            logger.debug(
                "{}: Appending entries [{} and other {} logs] all exist locally",
                name,
                entries.get(0),
                entries.size() - 1);
          }
        }

      } else {
        long offset = lastIndex + 1;
        append(entries.subList((int) (ci - offset), entries.size()));
      }
      try {
        commitTo(Math.min(leaderCommit, newLastIndex));
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
   * @param lastIndex leader's matchIndex for this follower node
   * @param lastTerm the entry's term which index is leader's matchIndex for this follower node
   * @param leaderCommit leader's commitIndex
   * @param entry entry sent from the leader node
   * @return -1 if the entries cannot be appended, otherwise the last index of new entries
   */
  public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, Log entry) {
    if (matchTerm(lastTerm, lastIndex)) {
      long newLastIndex = lastIndex + 1;
      if (entry.getCurrLogIndex() <= commitIndex) {
        logger.debug(
            "{}: entry {} conflict with committed entry [commitIndex({})]",
            name,
            entry.getCurrLogIndex(),
            commitIndex);
      } else {
        append(entry);
      }
      try {
        commitTo(Math.min(leaderCommit, newLastIndex));
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
    Object logUpdateCondition =
        getLogUpdateCondition(entries.get(entries.size() - 1).getCurrLogIndex());
    synchronized (logUpdateCondition) {
      logUpdateCondition.notifyAll();
    }
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
    Object logUpdateCondition = getLogUpdateCondition(entry.getCurrLogIndex());
    synchronized (logUpdateCondition) {
      logUpdateCondition.notifyAll();
    }
    return getLastLogIndex();
  }

  /**
   * Used by leader node to try to commit entries.
   *
   * @param leaderCommit leader's commitIndex
   * @param term the entry's term which index is leaderCommit in leader's log module
   * @return true or false
   */
  public synchronized boolean maybeCommit(long leaderCommit, long term) {
    if (leaderCommit > commitIndex && matchTerm(term, leaderCommit)) {
      try {
        commitTo(leaderCommit);
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
  public void applySnapshot(Snapshot snapshot) {
    logger.info(
        "{}: log module starts to restore snapshot [index: {}, term: {}]",
        name,
        snapshot.getLastLogIndex(),
        snapshot.getLastLogTerm());
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

    // as the follower receives a snapshot, the logs persisted is not complete, so remove them
    getStableEntryManager().clearAllLogs(commitIndex);

    synchronized (changeApplyCommitIndexCond) {
      if (this.maxHaveAppliedCommitIndex < snapshot.getLastLogIndex()) {
        this.maxHaveAppliedCommitIndex = snapshot.getLastLogIndex();
      }
    }
  }

  /**
   * Determines if the given (lastTerm, lastIndex) log is more up-to-date by comparing the index and
   * term of the last entries in the existing logs. If the logs have last entries with different
   * terms, then the log with the later term is more up-to-date. If the logs end with the same term,
   * then whichever log has the larger lastIndex is more up-to-date. If the logs are the same, the
   * given log is up-to-date.
   *
   * @param lastTerm candidate's lastTerm
   * @param lastIndex candidate's lastIndex
   * @return true or false
   */
  public boolean isLogUpToDate(long lastTerm, long lastIndex) {
    return lastTerm > getLastLogTerm()
        || (lastTerm == getLastLogTerm() && lastIndex >= getLastLogIndex());
  }

  /**
   * Pack entries from low through high - 1, just like slice (entries[low:high]). firstIndex <= low
   * <= high <= lastIndex.
   *
   * @param low request index low bound
   * @param high request index upper bound
   */
  public List<Log> getEntries(long low, long high) {
    if (low >= high) {
      return Collections.emptyList();
    }
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
  public void commitTo(long newCommitIndex) throws LogExecutionException {
    if (commitIndex >= newCommitIndex) {
      return;
    }
    long startTime = Statistic.RAFT_SENDER_COMMIT_GET_LOGS.getOperationStartTime();
    long lo = getUnCommittedEntryManager().getFirstUnCommittedIndex();
    long hi = newCommitIndex + 1;
    List<Log> entries = new ArrayList<>(getUnCommittedEntryManager().getEntries(lo, hi));
    Statistic.RAFT_SENDER_COMMIT_GET_LOGS.calOperationCostTimeFromStart(startTime);

    if (entries.isEmpty()) {
      return;
    }

    long commitLogIndex = getCommitLogIndex();
    long firstLogIndex = entries.get(0).getCurrLogIndex();
    if (commitLogIndex >= firstLogIndex) {
      logger.warn(
          "Committing logs that has already been committed: {} >= {}",
          commitLogIndex,
          firstLogIndex);
      entries
          .subList(0, (int) (getCommitLogIndex() - entries.get(0).getCurrLogIndex() + 1))
          .clear();
    }

    boolean needToCompactLog = false;
    int numToReserveForNew = minNumOfLogsInMem;
    if (committedEntryManager.getTotalSize() + entries.size() > maxNumOfLogsInMem) {
      needToCompactLog = true;
      numToReserveForNew = maxNumOfLogsInMem - entries.size();
    }

    long newEntryMemSize = 0;
    for (Log entry : entries) {
      if (entry.getByteSize() == 0) {
        logger.debug(
            "{} should not go here, must be send to the follower, "
                + "so the log has been serialized exclude single node mode",
            entry);
        entry.setByteSize((int) RamUsageEstimator.sizeOf(entry));
      }
      newEntryMemSize += entry.getByteSize();
    }
    int sizeToReserveForNew = minNumOfLogsInMem;
    if (newEntryMemSize + committedEntryManager.getEntryTotalMemSize() > maxLogMemSize) {
      needToCompactLog = true;
      sizeToReserveForNew =
          committedEntryManager.maxLogNumShouldReserve(maxLogMemSize - newEntryMemSize);
    }

    if (needToCompactLog) {
      int numForNew = Math.min(numToReserveForNew, sizeToReserveForNew);
      int sizeToReserveForConfig = minNumOfLogsInMem;
      startTime = Statistic.RAFT_SENDER_COMMIT_DELETE_EXCEEDING_LOGS.getOperationStartTime();
      synchronized (this) {
        innerDeleteLog(Math.min(sizeToReserveForConfig, numForNew));
      }
      Statistic.RAFT_SENDER_COMMIT_DELETE_EXCEEDING_LOGS.calOperationCostTimeFromStart(startTime);
    }

    startTime = Statistic.RAFT_SENDER_COMMIT_APPEND_AND_STABLE_LOGS.getOperationStartTime();
    try {
      // Operations here are so simple that the execution could be thought
      // success or fail together approximately.
      // TODO: make it real atomic
      getCommittedEntryManager().append(entries);
      Log lastLog = entries.get(entries.size() - 1);
      getUnCommittedEntryManager().stableTo(lastLog.getCurrLogIndex());
      commitIndex = lastLog.getCurrLogIndex();

      if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
        // Cluster could continue provide service when exception is thrown here
        getStableEntryManager().append(entries, maxHaveAppliedCommitIndex);
      }
    } catch (TruncateCommittedEntryException e) {
      // fatal error, node won't recover from the error anymore
      // TODO: let node quit the raft group once encounter the error
      logger.error("{}: Unexpected error:", name, e);
    } catch (IOException e) {
      // The exception will block the raft service continue accept log.
      // TODO: Notify user that the persisted logs before these entries(include) are corrupted.
      // TODO: An idea is that we can degrade the service by disable raft log persistent for
      // TODO: the group. It needs fine-grained control for the config of Raft log persistence.
      logger.error("{}: persistent raft log error:", name, e);
      throw new LogExecutionException(e);
    } finally {
      Statistic.RAFT_SENDER_COMMIT_APPEND_AND_STABLE_LOGS.calOperationCostTimeFromStart(startTime);
    }

    startTime = Statistic.RAFT_SENDER_COMMIT_APPLY_LOGS.getOperationStartTime();
    applyEntries(entries);
    Statistic.RAFT_SENDER_COMMIT_APPLY_LOGS.calOperationCostTimeFromStart(startTime);

    long unappliedLogSize = commitLogIndex - maxHaveAppliedCommitIndex;
    if (unappliedLogSize > ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem()) {
      logger.debug(
          "There are too many unapplied logs [{}], wait for a while to avoid memory overflow",
          unappliedLogSize);
      try {
        Thread.sleep(
            unappliedLogSize - ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Returns whether the index and term passed in match.
   *
   * @param term request entry term
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
   */
  void applyEntries(List<Log> entries) {
    for (Log entry : entries) {
      applyEntry(entry);
    }
  }

  public void applyEntry(Log entry) {
    // For add/remove logs in data groups, this log will be applied immediately when it is
    // appended to the raft log.
    // In this case, it will apply a log that has been applied.
    if (entry.isApplied()) {
      return;
    }
    if (blockAppliedCommitIndex > 0 && entry.getCurrLogIndex() > blockAppliedCommitIndex) {
      blockedUnappliedLogList.add(entry);
      return;
    }
    try {
      logApplier.apply(entry);
    } catch (Exception e) {
      entry.setException(e);
      entry.setApplied(true);
    }
  }

  /**
   * Check whether the parameters passed in satisfy the following properties. firstIndex <= low <=
   * high.
   *
   * @param low request index low bound
   * @param high request index upper bound
   * @throws EntryCompactedException
   * @throws GetEntriesWrongParametersException
   */
  void checkBound(long low, long high)
      throws EntryCompactedException, GetEntriesWrongParametersException {
    if (low > high) {
      logger.error("{}: invalid getEntries: parameter: {} > {}", name, low, high);
      throw new GetEntriesWrongParametersException(low, high);
    }
    long first = getFirstIndex();
    if (low < first) {
      logger.error(
          "{}: CheckBound out of index: parameter: {} , lower bound: {} ", name, low, high);
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
  long findConflict(List<Log> entries) {
    for (Log entry : entries) {
      if (!matchTerm(entry.getCurrLogTerm(), entry.getCurrLogIndex())) {
        if (entry.getCurrLogIndex() <= getLastLogIndex()) {
          logger.info("found conflict at index {}", entry.getCurrLogIndex());
        }
        return entry.getCurrLogIndex();
      }
    }
    return -1;
  }

  @TestOnly
  protected RaftLogManager(
      CommittedEntryManager committedEntryManager,
      StableEntryManager stableEntryManager,
      LogApplier applier) {
    this.setCommittedEntryManager(committedEntryManager);
    this.setStableEntryManager(stableEntryManager);
    this.logApplier = applier;
    long first = committedEntryManager.getFirstIndex();
    long last = committedEntryManager.getLastIndex();
    this.setUnCommittedEntryManager(new UnCommittedEntryManager(last + 1));
    this.commitIndex = last;
    this.maxHaveAppliedCommitIndex = first;
    this.blockAppliedCommitIndex = -1;
    this.blockedUnappliedLogList = new CopyOnWriteArrayList<>();
    this.checkLogApplierExecutorService =
        Executors.newSingleThreadExecutor(
            new BasicThreadFactory.Builder()
                .namingPattern("check-log-applier-" + name)
                .daemon(true)
                .build());
    this.checkLogApplierFuture = checkLogApplierExecutorService.submit(this::checkAppliedLogIndex);
    for (int i = 0; i < logUpdateConditions.length; i++) {
      logUpdateConditions[i] = new Object();
    }
  }

  @TestOnly
  void setMinNumOfLogsInMem(int minNumOfLogsInMem) {
    this.minNumOfLogsInMem = minNumOfLogsInMem;
  }

  @TestOnly
  public void setMaxHaveAppliedCommitIndex(long maxHaveAppliedCommitIndex) {
    this.checkLogApplierExecutorService.shutdownNow();
    try {
      this.checkLogApplierExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    this.maxHaveAppliedCommitIndex = maxHaveAppliedCommitIndex;
  }

  public void close() {
    getStableEntryManager().close();
    if (deleteLogExecutorService != null) {
      deleteLogExecutorService.shutdownNow();
      if (deleteLogFuture != null) {
        deleteLogFuture.cancel(true);
      }

      try {
        deleteLogExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Close delete log thread interrupted");
      }
      deleteLogExecutorService = null;
    }

    if (checkLogApplierExecutorService != null) {
      checkLogApplierExecutorService.shutdownNow();
      checkLogApplierFuture.cancel(true);
      try {
        checkLogApplierExecutorService.awaitTermination(20, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Close check log applier thread interrupted");
      }
      checkLogApplierExecutorService = null;
    }

    if (logApplier != null) {
      logApplier.close();
    }
  }

  UnCommittedEntryManager getUnCommittedEntryManager() {
    return unCommittedEntryManager;
  }

  private void setUnCommittedEntryManager(UnCommittedEntryManager unCommittedEntryManager) {
    this.unCommittedEntryManager = unCommittedEntryManager;
  }

  CommittedEntryManager getCommittedEntryManager() {
    return committedEntryManager;
  }

  private void setCommittedEntryManager(CommittedEntryManager committedEntryManager) {
    this.committedEntryManager = committedEntryManager;
  }

  public StableEntryManager getStableEntryManager() {
    return stableEntryManager;
  }

  private void setStableEntryManager(StableEntryManager stableEntryManager) {
    this.stableEntryManager = stableEntryManager;
  }

  public long getMaxHaveAppliedCommitIndex() {
    return maxHaveAppliedCommitIndex;
  }

  /** check whether delete the committed log */
  void checkDeleteLog() {
    try {
      synchronized (this) {
        if (committedEntryManager.getTotalSize() <= minNumOfLogsInMem) {
          return;
        }
        innerDeleteLog(minNumOfLogsInMem);
      }
    } catch (Exception e) {
      logger.error("{}, error occurred when checking delete log", name, e);
    }
  }

  private void innerDeleteLog(int sizeToReserve) {
    long removeSize = (long) committedEntryManager.getTotalSize() - sizeToReserve;
    if (removeSize <= 0) {
      return;
    }

    long compactIndex =
        Math.min(committedEntryManager.getDummyIndex() + removeSize, maxHaveAppliedCommitIndex - 1);
    try {
      logger.debug(
          "{}: Before compaction index {}-{}, compactIndex {}, removeSize {}, committedLogSize "
              + "{}, maxAppliedLog {}",
          name,
          getFirstIndex(),
          getLastLogIndex(),
          compactIndex,
          removeSize,
          committedEntryManager.getTotalSize(),
          maxHaveAppliedCommitIndex);
      getCommittedEntryManager().compactEntries(compactIndex);
      if (ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence()) {
        getStableEntryManager().removeCompactedEntries(compactIndex);
      }
      logger.debug(
          "{}: After compaction index {}-{}, committedLogSize {}",
          name,
          getFirstIndex(),
          getLastLogIndex(),
          committedEntryManager.getTotalSize());
    } catch (EntryUnavailableException e) {
      logger.error("{}: regular compact log entries failed, error={}", name, e.getMessage());
    }
  }

  public Object getLogUpdateCondition(long logIndex) {
    return logUpdateConditions[(int) (logIndex % logUpdateConditions.length)];
  }

  void applyAllCommittedLogWhenStartUp() {
    long lo = maxHaveAppliedCommitIndex;
    long hi = getCommittedEntryManager().getLastIndex() + 1;
    if (lo >= hi) {
      logger.info(
          "{}: the maxHaveAppliedCommitIndex={}, lastIndex={}, no need to reapply",
          name,
          maxHaveAppliedCommitIndex,
          hi);
      return;
    }

    List<Log> entries = new ArrayList<>(getCommittedEntryManager().getEntries(lo, hi));
    applyEntries(entries);
  }

  public void checkAppliedLogIndex() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        doCheckAppliedLogIndex();
      } catch (Exception e) {
        logger.error("{}, an exception occurred when checking the applied log index", name, e);
      }
    }
    logger.info(
        "{}, the check-log-applier thread {} is interrupted",
        name,
        Thread.currentThread().getName());
  }

  void doCheckAppliedLogIndex() {
    long nextToCheckIndex = maxHaveAppliedCommitIndex + 1;
    try {
      if (nextToCheckIndex > commitIndex
          || nextToCheckIndex > getCommittedEntryManager().getLastIndex()
          || (blockAppliedCommitIndex > 0 && blockAppliedCommitIndex < nextToCheckIndex)) {
        // avoid spinning
        Thread.sleep(5);
        return;
      }
      Log log = getCommittedEntryManager().getEntry(nextToCheckIndex);
      if (log == null || log.getCurrLogIndex() != nextToCheckIndex) {
        logger.warn(
            "{}, get log error when checking the applied log index, log={}, nextToCheckIndex={}",
            name,
            log,
            nextToCheckIndex);
        return;
      }
      synchronized (log) {
        while (!log.isApplied() && maxHaveAppliedCommitIndex < log.getCurrLogIndex()) {
          // wait until the log is applied or a newer snapshot is installed
          log.wait(5);
        }
      }
      synchronized (changeApplyCommitIndexCond) {
        // maxHaveAppliedCommitIndex may change if a snapshot is applied concurrently
        maxHaveAppliedCommitIndex = Math.max(maxHaveAppliedCommitIndex, nextToCheckIndex);
      }
      logger.debug(
          "{}: log={} is applied, nextToCheckIndex={}, commitIndex={}, maxHaveAppliedCommitIndex={}",
          name,
          log,
          nextToCheckIndex,
          commitIndex,
          maxHaveAppliedCommitIndex);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("{}: do check applied log index is interrupt", name);
    } catch (EntryCompactedException e) {
      synchronized (changeApplyCommitIndexCond) {
        // maxHaveAppliedCommitIndex may change if a snapshot is applied concurrently
        maxHaveAppliedCommitIndex = Math.max(maxHaveAppliedCommitIndex, nextToCheckIndex);
      }
      logger.debug(
          "{}: compacted log is assumed applied, nextToCheckIndex={}, commitIndex={}, "
              + "maxHaveAppliedCommitIndex={}",
          name,
          nextToCheckIndex,
          commitIndex,
          maxHaveAppliedCommitIndex);
    }
  }

  /**
   * Clear the fence that blocks the application of new logs, and continue to apply the cached
   * unapplied logs.
   */
  public void resetBlockAppliedCommitIndex() {
    this.blockAppliedCommitIndex = -1;
    this.reapplyBlockedLogs();
  }

  /**
   * Set a fence to prevent newer logs, which have larger indexes than `blockAppliedCommitIndex`,
   * from being applied, so the underlying state machine may remain unchanged for a while for
   * snapshots. New committed logs will be cached until `resetBlockAppliedCommitIndex()` is called.
   *
   * @param blockAppliedCommitIndex
   */
  public void setBlockAppliedCommitIndex(long blockAppliedCommitIndex) {
    this.blockAppliedCommitIndex = blockAppliedCommitIndex;
  }

  /** Apply the committed logs that were previously blocked by `blockAppliedCommitIndex` if any. */
  private void reapplyBlockedLogs() {
    if (!blockedUnappliedLogList.isEmpty()) {
      applyEntries(blockedUnappliedLogList);
      logger.info("{}: reapply {} number of logs", name, blockedUnappliedLogList.size());
    }
    blockedUnappliedLogList.clear();
  }

  public String getName() {
    return name;
  }

  public long getBlockAppliedCommitIndex() {
    return blockAppliedCommitIndex;
  }

  public RaftLogManager(LogApplier logApplier) {
    this.logApplier = logApplier;
  }
}
