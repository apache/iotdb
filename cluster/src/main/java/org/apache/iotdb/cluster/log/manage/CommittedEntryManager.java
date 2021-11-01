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

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.manage.serializable.LogManagerMeta;
import org.apache.iotdb.db.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CommittedEntryManager {

  private static final Logger logger = LoggerFactory.getLogger(CommittedEntryManager.class);

  // memory cache for logs which have been persisted in disk.
  private List<Log> entries;

  private long entryTotalMemSize;

  /**
   * Note that it is better to use applyingSnapshot to update dummy entry immediately after this
   * instance is created.
   */
  CommittedEntryManager(int maxNumOfLogInMem) {
    entries = Collections.synchronizedList(new ArrayList<>(maxNumOfLogInMem));
    entries.add(new EmptyContentLog(-1, -1));
    entryTotalMemSize = 0;
  }

  CommittedEntryManager(int maxNumOfLogInMem, LogManagerMeta meta) {
    entries = Collections.synchronizedList(new ArrayList<>(maxNumOfLogInMem));
    entries.add(
        new EmptyContentLog(
            meta.getMaxHaveAppliedCommitIndex() == -1
                ? -1
                : meta.getMaxHaveAppliedCommitIndex() - 1,
            meta.getLastLogTerm()));
    entryTotalMemSize = 0;
  }

  /**
   * Overwrite the contents of this object with those of the given snapshot. Note that this function
   * is only used if you want to override all the contents, otherwise please use
   * compactEntries(snapshot.lastIndex()).
   *
   * @param snapshot snapshot
   */
  void applyingSnapshot(Snapshot snapshot) {
    long localIndex = getDummyIndex();
    long snapIndex = snapshot.getLastLogIndex();
    if (localIndex >= snapIndex) {
      logger.info("requested snapshot is older than the existing snapshot");
      return;
    }
    entries.clear();
    entries.add(new EmptyContentLog(snapshot.getLastLogIndex(), snapshot.getLastLogTerm()));
  }

  /**
   * Return the last entry's index which have been compacted.
   *
   * @return dummyIndex
   */
  Long getDummyIndex() {
    return entries.get(0).getCurrLogIndex();
  }

  /**
   * Return the first entry's index which have not been compacted.
   *
   * @return firstIndex
   */
  Long getFirstIndex() {
    return getDummyIndex() + 1;
  }

  /**
   * Return the last entry's index which have been committed and persisted.
   *
   * @return getLastIndex
   */
  Long getLastIndex() {
    return getDummyIndex() + entries.size() - 1;
  }

  /**
   * Return the entries's size
   *
   * @return entries's size
   */
  int getTotalSize() {
    // the first one is a sentry
    return entries.size() - 1;
  }

  /**
   * Return the entry's term for given index. Note that the called should ensure index <=
   * entries[entries.size()-1].index.
   *
   * @param index request entry index
   * @return -1 if index > entries[entries.size()-1].index, throw EntryCompactedException if index <
   *     dummyIndex, or return the entry's term for given index
   * @throws EntryCompactedException
   */
  public long maybeTerm(long index) throws EntryCompactedException {
    Log log = getEntry(index);
    if (log == null) {
      return -1;
    }
    return log.getCurrLogTerm();
  }

  /**
   * Pack entries from low through high - 1, just like slice (entries[low:high]). dummyIndex < low
   * <= high. Note that caller must ensure low <= high.
   *
   * @param low request index low bound
   * @param high request index upper bound
   */
  public List<Log> getEntries(long low, long high) {
    if (low > high) {
      logger.debug("invalid getEntries: parameter: {} > {}", low, high);
      return Collections.emptyList();
    }
    long dummyIndex = getDummyIndex();
    if (low <= dummyIndex) {
      logger.debug(
          "entries low ({}) is out of bound dummyIndex ({}), adjust parameter 'low' to {}",
          low,
          dummyIndex,
          dummyIndex);
      low = dummyIndex + 1;
    }
    long lastIndex = getLastIndex();
    if (high > lastIndex + 1) {
      logger.debug(
          "entries high ({}) is out of bound lastIndex ({}), adjust parameter 'high' to {}",
          high,
          lastIndex,
          lastIndex);
      high = lastIndex + 1;
    }
    return entries.subList((int) (low - dummyIndex), (int) (high - dummyIndex));
  }

  /**
   * Return the entry's log for given index. Note that the called should ensure index <=
   * entries[entries.size()-1].index.
   *
   * @param index request entry index
   * @return null if index > entries[entries.size()-1].index, throw EntryCompactedException if index
   *     < dummyIndex, or return the entry's log for given index
   * @throws EntryCompactedException
   */
  Log getEntry(long index) throws EntryCompactedException {
    long dummyIndex = getDummyIndex();
    if (index < dummyIndex) {
      logger.debug(
          "invalid committedEntryManager getEntry: parameter: index({}) < compactIndex({})",
          index,
          dummyIndex);
      throw new EntryCompactedException(index, dummyIndex);
    }
    if ((int) (index - dummyIndex) >= entries.size()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "invalid committedEntryManager getEntry : parameter: index({}) > lastIndex({})",
            index,
            getLastIndex());
      }
      return null;
    }
    return entries.get((int) (index - dummyIndex));
  }

  /**
   * Discards all log entries prior to compactIndex.
   *
   * @param compactIndex request compactIndex
   * @throws EntryUnavailableException
   */
  void compactEntries(long compactIndex) throws EntryUnavailableException {
    long dummyIndex = getDummyIndex();
    if (compactIndex < dummyIndex) {
      logger.info(
          "entries before request index ({}) have been compacted, and the compactIndex is ({})",
          compactIndex,
          dummyIndex);
      return;
    }
    if (compactIndex > getLastIndex()) {
      logger.info("compact ({}) is out of bound lastIndex ({})", compactIndex, getLastIndex());
      throw new EntryUnavailableException(compactIndex, getLastIndex());
    }
    int index = (int) (compactIndex - dummyIndex);
    for (int i = 1; i <= index; i++) {
      entryTotalMemSize -= entries.get(i).getByteSize();
    }
    // The following two lines of code should be tightly linked,
    // because the check apply thread will read the entry also, and there will be concurrency
    // problems,
    // but please rest assured that we have done concurrency security check in the check apply
    // thread.
    // They are put together just to reduce the probability of concurrency.
    entries.set(
        0,
        new EmptyContentLog(
            entries.get(index).getCurrLogIndex(), entries.get(index).getCurrLogTerm()));
    entries.subList(1, index + 1).clear();
  }

  /**
   * Append committed entries. This method will truncate conflict entries if it finds
   * inconsistencies.
   *
   * @param appendingEntries request entries
   * @throws TruncateCommittedEntryException
   */
  public void append(List<Log> appendingEntries) throws TruncateCommittedEntryException {
    if (appendingEntries.isEmpty()) {
      return;
    }
    long offset = appendingEntries.get(0).getCurrLogIndex() - getDummyIndex();
    if (entries.size() - offset == 0) {
      for (int i = 0; i < appendingEntries.size(); i++) {
        entryTotalMemSize += appendingEntries.get(i).getByteSize();
      }
      entries.addAll(appendingEntries);
    } else if (entries.size() - offset > 0) {
      throw new TruncateCommittedEntryException(
          appendingEntries.get(0).getCurrLogIndex(), getLastIndex());
    } else {
      logger.error(
          "missing log entry [last: {}, append at: {}]",
          getLastIndex(),
          appendingEntries.get(0).getCurrLogIndex());
    }
  }

  @TestOnly
  CommittedEntryManager(List<Log> entries) {
    this.entries = entries;
  }

  @TestOnly
  List<Log> getAllEntries() {
    return entries;
  }

  public long getEntryTotalMemSize() {
    return entryTotalMemSize;
  }

  public void setEntryTotalMemSize(long entryTotalMemSize) {
    this.entryTotalMemSize = entryTotalMemSize;
  }

  /**
   * check how many logs could be reserved in memory.
   *
   * @param maxMemSize the max memory size for old committed log
   * @return max num to reserve old committed log
   */
  public int maxLogNumShouldReserve(long maxMemSize) {
    long totalSize = 0;
    for (int i = entries.size() - 1; i >= 1; i--) {
      if (totalSize + entries.get(i).getByteSize() > maxMemSize) {
        return entries.size() - 1 - i;
      }
      totalSize += entries.get(i).getByteSize();
    }
    return entries.size() - 1;
  }
}
