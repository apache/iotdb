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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CommittedEntryManager {

    private static final Logger logger = LoggerFactory.getLogger(CommittedEntryManager.class);

    //the state that raft nodes must persist such as voteFor, currentTerm
    private HardState hardState;
    //memory cache for persistent logs
    private List<Log> entries;

    /**
     * Note that it is better to use applyingSnapshot to update dummy as soon as such an instance is created.
     */
    public CommittedEntryManager() {
        entries = new ArrayList<Log>() {{
            add(new PhysicalPlanLog(-1, -1));
        }};
    }

    /**
     * setHardState set the hardState.
     */
    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    /**
     * getHardState return the raftNode hardState.
     *
     * @return hardState
     */
    public HardState getHardState() {
        return hardState;
    }

    /**
     * ApplySnapshot overwrites the contents of this object with
     * those of the given snapshot.
     * Note that this function is only used if you want to override all the contents, otherwise please use compactEntries.
     *
     * @param snapshot snapshot
     */
    public void applyingSnapshot(RaftSnapshot snapshot) {
        long localIndex = getDummyIndex();
        long snapIndex = snapshot.getLastIndex();
        if (localIndex >= snapIndex) {
            logger.info("requested snapshot is older than the existing snapshot");
            return;
        }
        entries.clear();
        Log dummy = new PhysicalPlanLog(snapshot.getLastIndex(), snapshot.getLastTerm());
        entries.add(dummy);
    }

    /**
     * getDummyIndex return the last entry's index which have been compacted.
     *
     * @return dummyIndex
     */
    public Long getDummyIndex() {
        return entries.get(0).getCurrLogIndex();
    }

    /**
     * getFirstIndex return the first entry's index which have not been compacted.
     *
     * @return firstIndex
     */
    public Long getFirstIndex() {
        return getDummyIndex() + 1;
    }

    /**
     * getLastIndex return the last entry's index which have been committed and persisted.
     *
     * @return getLastIndex
     */
    public Long getLastIndex() {
        return entries.get(0).getCurrLogIndex() + entries.size() - 1;
    }

    /**
     * maybeTerm returns the term for given index.
     *
     * @param index request entry index
     * @return set to -1 if the entry for this index cannot be found, or return the entry's term.
     * @throws EntryCompactedException
     */
    public long maybeTerm(long index) throws EntryCompactedException {
        long offset = entries.get(0).getCurrLogIndex();
        if (index < offset) {
            logger.info("invalid committedEntryManager maybeTerm: parameter: index({}) < firstIndex({})", index, offset);
            throw new EntryCompactedException(index, offset);
        }
        if ((int) (index - offset) >= entries.size()) {
            return -1;
        }
        return entries.get((int) (index - offset)).getCurrLogTerm();
    }

    /**
     * getEntries pack entries from low through high - 1, just like slice (entries[low:high]).
     * dummyIndex < low < high.
     * Note that caller must ensure low < high.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     * @throws EntryCompactedException
     */
    public List<Log> getEntries(long low, long high) throws EntryCompactedException {
        if (low > high) {
            logger.error("invalid getEntries: parameter: {} > {}", low, high);
        }
        long dummyIndex = getDummyIndex();
        if (low <= dummyIndex || entries.size() == 1) {
            logger.info("entries before request index ({}) have been compacted, and the compactIndex is ({})", low, dummyIndex);
            throw new EntryCompactedException(low, dummyIndex);
        }
        long lastIndex = getLastIndex();
        if (high > lastIndex + 1) {
            logger.error("entries high ({}) is out of bound lastIndex ({}),adjust 'high' to {}", high, lastIndex, lastIndex);
            high = lastIndex + 1;
        }
        return entries.subList((int) (low - dummyIndex), (int) (high - dummyIndex));
    }

    /**
     * compactEntries discards all log entries prior to compactIndex.
     *
     * @param compactIndex request compactIndex
     * @throws EntryUnavailableException
     */
    public void compactEntries(long compactIndex) throws EntryUnavailableException {
        long dummyIndex = getDummyIndex();
        if (compactIndex <= dummyIndex) {
            logger.info("entries before request index ({}) have been compacted, and the compactIndex is ({})", compactIndex, dummyIndex);
            return;
        }
        if (compactIndex > getLastIndex()) {
            logger.error("compact ({}) is out of bound lastIndex ({})", compactIndex, getLastIndex());
            throw new EntryUnavailableException(compactIndex, getLastIndex());
        }
        int index = (int) (compactIndex - dummyIndex);
        PhysicalPlanLog dummy = new PhysicalPlanLog(entries.get(index).getCurrLogTerm(), entries.get(index).getCurrLogIndex());
        entries.set(0, dummy);
        entries.subList(1, index + 1).clear();
    }

    /**
     * append append committed entries.It will truncate conflict entries if it find inconsistencies.
     * maybe not throw a exception is better.It depends on the caller's implementation.
     *
     * @param appendingEntries request entries
     */
    public void append(List<Log> appendingEntries) {
        if (appendingEntries.size() == 0) {
            return;
        }
        long localFirstIndex = getFirstIndex();
        long appendingLastIndex = appendingEntries.get(0).getCurrLogIndex() + appendingEntries.size() - 1;
        if (appendingLastIndex < localFirstIndex) {
            return;
        }
        if (localFirstIndex > appendingEntries.get(0).getCurrLogIndex()) {
            appendingEntries.subList(0, (int) (localFirstIndex - appendingEntries.get(0).getCurrLogIndex())).clear();
        }
        long offset = appendingEntries.get(0).getCurrLogIndex() - getDummyIndex();
        if (entries.size() - offset == 0) {
            entries.addAll(appendingEntries);
        } else if (entries.size() - offset > 0) {
//            logger.error("The logs which first index is {} are going to truncate committed logs", appendingEntries.get(0).getCurrLogIndex());
//            throw new TruncateCommittedEntryException(appendingEntries.get(0).getCurrLogIndex(),getLastIndex());
            entries.subList((int) offset, entries.size()).clear();
            entries.addAll(appendingEntries);
        } else {
            logger.error("missing log entry [last: {}, append at: {}]", getLastIndex(), appendingEntries.get(0).getCurrLogIndex());
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