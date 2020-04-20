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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommittedEntryManager {

    private static final Logger logger = LoggerFactory.getLogger(CommittedEntryManager.class);

    // memory cache for logs which have been persisted in disk.
    private List<Log> entries;

    /**
     * Note that it is better to use applyingSnapshot to update dummy entry immediately after this
     * instance is created.
     */
    public CommittedEntryManager() {
        entries = new ArrayList<Log>() {{
            add(new EmptyContentLog(-1, -1));
        }};
    }

    /**
     * Overwrite the contents of this object with those of the given snapshot. Note that this function
     * is only used if you want to override all the contents, otherwise please use
     * compactEntries(snapshot.lastIndex()).
     *
     * @param snapshot snapshot
     */
    public void applyingSnapshot(Snapshot snapshot) {
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
    public Long getDummyIndex() {
        return entries.get(0).getCurrLogIndex();
    }

    /**
     * Return the first entry's index which have not been compacted.
     *
     * @return firstIndex
     */
    public Long getFirstIndex() {
        return getDummyIndex() + 1;
    }

    /**
     * Return the last entry's index which have been committed and persisted.
     *
     * @return getLastIndex
     */
    public Long getLastIndex() {
        return getDummyIndex() + entries.size() - 1;
    }

    /**
     * Return the entry's term for given index. Note that the called should ensure index <=
     * entries[entries.size()-1].index.
     *
     * @param index request entry index
     * @return -1 if index > entries[entries.size()-1].index, throw EntryCompactedException if index <
     * dummyIndex, or return the entry's term for given index
     * @throws EntryCompactedException
     */
    public long maybeTerm(long index) throws EntryCompactedException {
        long dummyIndex = getDummyIndex();
        if (index < dummyIndex) {
            logger.info(
                "invalid committedEntryManager maybeTerm: parameter: index({}) < compactIndex({})",
                index, dummyIndex);
            throw new EntryCompactedException(index, dummyIndex);
        }
        if ((int) (index - dummyIndex) >= entries.size()) {
            logger.debug(
                "invalid committedEntryManager maybeTerm : parameter: index({}) > lastIndex({})",
                index, getLastIndex());
            return -1;
        }
        return entries.get((int) (index - dummyIndex)).getCurrLogTerm();
    }

    /**
     * Pack entries from low through high - 1, just like slice (entries[low:high]). dummyIndex < low
     * <= high. Note that caller must ensure low <= high.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     * @throws EntryCompactedException
     */
    public List<Log> getEntries(long low, long high) throws EntryCompactedException {
        if (low > high) {
            logger.debug("invalid getEntries: parameter: {} > {}", low, high);
        }
        long dummyIndex = getDummyIndex();
        if (low <= dummyIndex || entries.size() == 1) {
            logger.info(
                "entries before request index ({}) have been compacted, and the compactIndex is ({})",
                low, dummyIndex);
            throw new EntryCompactedException(low, dummyIndex);
        }
        long lastIndex = getLastIndex();
        if (high > lastIndex + 1) {
            logger.debug(
                "entries high ({}) is out of bound lastIndex ({}), adjust parameter 'high' to {}",
                high, lastIndex, lastIndex);
            high = lastIndex + 1;
        }
        return entries.subList((int) (low - dummyIndex), (int) (high - dummyIndex));
    }

    /**
     * Discards all log entries prior to compactIndex.
     *
     * @param compactIndex request compactIndex
     * @throws EntryUnavailableException
     */
    public void compactEntries(long compactIndex) throws EntryUnavailableException {
        long dummyIndex = getDummyIndex();
        if (compactIndex <= dummyIndex) {
            logger.info(
                "entries before request index ({}) have been compacted, and the compactIndex is ({})",
                compactIndex, dummyIndex);
            return;
        }
        if (compactIndex > getLastIndex()) {
            logger
                .info("compact ({}) is out of bound lastIndex ({})", compactIndex, getLastIndex());
            throw new EntryUnavailableException(compactIndex, getLastIndex());
        }
        int index = (int) (compactIndex - dummyIndex);
        entries.set(0, new EmptyContentLog(entries.get(index).getCurrLogTerm(),
            entries.get(index).getCurrLogIndex()));
        entries.subList(1, index + 1).clear();
    }

    /**
     * Append committed entries. This method will truncate conflict entries if it finds
     * inconsistencies.
     *
     * @param appendingEntries request entries
     */
    public void append(List<Log> appendingEntries) {
        if (appendingEntries.size() == 0) {
            return;
        }
        long localFirstIndex = getFirstIndex();
        long appendingLastIndex =
            appendingEntries.get(0).getCurrLogIndex() + appendingEntries.size() - 1;
        if (appendingLastIndex < localFirstIndex) {
            return;
        }
        if (localFirstIndex > appendingEntries.get(0).getCurrLogIndex()) {
            appendingEntries
                .subList(0, (int) (localFirstIndex - appendingEntries.get(0).getCurrLogIndex()))
                .clear();
        }
        long offset = appendingEntries.get(0).getCurrLogIndex() - getDummyIndex();
        if (entries.size() - offset == 0) {
            entries.addAll(appendingEntries);
        } else if (entries.size() - offset > 0) {
            // maybe not throw a exception is better.It depends on the caller's implementation.
//            logger.error("The logs which first index is {} are going to truncate committed logs", appendingEntries.get(0).getCurrLogIndex());
//            throw new TruncateCommittedEntryException(appendingEntries.get(0).getCurrLogIndex(),getLastIndex());
            entries.subList((int) offset, entries.size()).clear();
            entries.addAll(appendingEntries);
        } else {
            logger.error("missing log entry [last: {}, append at: {}]", getLastIndex(),
                appendingEntries.get(0).getCurrLogIndex());
        }
    }

    @TestOnly
    public CommittedEntryManager(List<Log> entries) {
        this.entries = entries;
    }

    @TestOnly
    public List<Log> getAllEntries() {
        return entries;
    }
}
