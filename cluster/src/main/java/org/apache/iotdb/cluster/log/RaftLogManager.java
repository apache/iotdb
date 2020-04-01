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

import org.apache.iotdb.cluster.exception.*;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftLogManager {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

    //manage uncommitted entries
    UnCommittedEntryManager unCommittedEntryManager;
    //manage committed entries in memory as a cache
    CommittedEntryManager committedEntryManager;
    //manage committed entries in disk for safety
    StableEntryManager stableEntryManager;

    private long committed;
    private long applied;

    public RaftLogManager(CommittedEntryManager committedEntryManager, StableEntryManager stableEntryManager) {
        this.committedEntryManager = committedEntryManager;
        this.stableEntryManager = stableEntryManager;
        long last = committedEntryManager.getLastIndex();
        this.unCommittedEntryManager = new UnCommittedEntryManager(last + 1);
        //must apply [first,last] to state machine
        this.committed = last;
        this.applied = last;
    }

    /**
     * getCommitIndex return the log module's commitIndex.
     *
     * @return commitIndex
     */
    public long getCommitIndex() {
        return committed;
    }

    /**
     * setCommitIndex set the log module's commitIndex.
     * @param commitIndex request commitIndex
     */
    public void setCommitIndex(long commitIndex) {
        this.committed = commitIndex;
    }

    /**
     * getApplyIndex return the log module's applyIndex.
     *
     * @return applyIndex
     */
    public long getApplyIndex() {
        return applied;
    }

    /**
     * getFirstIndex return the first entry index which have not been compacted.
     *
     * @return firstIndex
     */
    public long getFirstIndex() {
        return committedEntryManager.getFirstIndex();
    }

    /**
     * getFirstIndex return the last entry index which been added into log module.
     *
     * @return lastIndex
     */
    public long getLastIndex() {
        long last = unCommittedEntryManager.maybeLastIndex();
        if (last != -1) {
            return last;
        }
        return committedEntryManager.getLastIndex();
    }


    /**
     * getTerm returns the term for given index.
     *
     * @param index request entry index
     * @return throw EntryCompactedException if index < dummyIndex, throw EntryUnavailableException if index > lastIndex, otherwise return the entry's term.
     * @throws EntryUnavailableException
     * @throws EntryCompactedException
     */
    public long getTerm(long index) throws EntryUnavailableException, EntryCompactedException {
        long dummyIndex = getFirstIndex() - 1;
        if (index < dummyIndex) {
            logger.info("invalid getTerm: parameter: index({}) < firstIndex({})", index, dummyIndex);
            throw new EntryCompactedException(index, dummyIndex);
        }
        long lastIndex = getLastIndex();
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
     * getTerm returns the last entry's term.
     * If it goes wrong, there must be an unexpected exception.
     *
     * @return last entry's term.
     */
    public long getLastTerm() {
        long term = -1;
        try {
            term = getTerm(getLastIndex());
        } catch (Exception e) {
            logger.error("unexpected error when getting the last term : {}", e.getMessage());
        }
        return term;
    }

    /**
     * maybeAppend is only used by follower node to support leader's complicated log replication rpcs
     *
     * @param lastIndex leader's matchIndex for this follower node
     * @param lastTerm the entry's term which index is leader's matchIndex for this follower node
     * @param leaderCommit leader's commitIndex
     * @param entries entries sent from the leader node.Note that the leader must ensure entries[0].index = lastIndex + 1
     * @return returns -1 if the entries cannot be appended. Otherwise, it returns last index of new entries
     */
    public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, List<Log> entries) {
        if (matchTerm(lastTerm, lastIndex)) {
            long newLastIndex = lastIndex + entries.size();
            long ci = findConflict(entries);
            if (ci == 0 || ci <= committed) {
                logger.error("entry {} conflict with committed entry [committed({})]", ci, committed);
            } else {
                long offset = lastIndex + 1;
                append(entries.subList((int) (ci - offset), entries.size()));
            }
            commitTo(Math.min(leaderCommit, newLastIndex));
            return newLastIndex;
        }
        return -1;
    }

    /**
     * append is used by leader node or MaybeAppend to directly append to unCommittedEntryManager.
     *
     * @param entries Note that the caller should ensure entries[0].index = committed
     * @return returns the newly generated lastIndex
     */
    public long append(List<Log> entries) {
        if (entries.size() == 0) {
            return getLastIndex();
        }
        long after = entries.get(0).getCurrLogIndex();
        if (after <= committed) {
            logger.error("after({}) is out of range [committed({})]", after, committed);
        }
        unCommittedEntryManager.truncateAndAppend(entries);
        return getLastIndex();
    }

    /**
     * commitTo is used by leader node or MaybeAppend to persist committed entries from unCommittedEntryManager to stableEntryManager and committedEntryManager.
     *
     * @param commitIndex request commitIndex
     * @return returns the local commitIndex
     */
    public long commitTo(long commitIndex) {
        if (committed < commitIndex) {
            List<Log> entries = unCommittedEntryManager.getEntries(unCommittedEntryManager.getFirstUnCommittedIndex(), commitIndex + 1);
            stableEntryManager.append(entries);
            committedEntryManager.append(entries);
            unCommittedEntryManager.stableTo(commitIndex, entries.get(entries.size() - 1).getCurrLogTerm());
            committed = commitIndex;
        }
        return committed;
    }

    /**
     * logValid return whether the entry with certain index is available in log module.
     *
     * @param index request index
     * @return return true or false
     */
    public boolean logValid(long index) {
        return index >= committedEntryManager.getFirstIndex();
    }


    /**
     * isLogUpToDate determines if the given (lastIndex,term) log is more up-to-date
     * by comparing the index and term of the last entries in the existing logs.
     * If the logs have last entries with different terms, then the log with the
     * later term is more up-to-date. If the logs end with the same term, then
     * whichever log has the larger lastIndex is more up-to-date. If the logs are
     * the same, the given log is up-to-date.
     *
     * @param lastTerm candidate's lastTerm
     * @param lastIndex candidate's lastIndex
     * @return return true or false
     */
    public boolean isLogUpToDate(long lastTerm, long lastIndex) {
        return lastTerm > getLastTerm() || (lastTerm == getLastTerm() && lastIndex >= getLastIndex());
    }

    /**
     * getEntries pack entries from low through high - 1, just like slice (entries[low:high]).
     * firstIndex <= low < high <= lastIndex.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     * @throws EntryCompactedException
     * @throws GetEntriesWrongParametersException
     */
    public List<Log> getEntries(long low, long high) throws EntryCompactedException, GetEntriesWrongParametersException {
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
     * checkBound check whether the parameters passed in satisfy the following properties.
     * firstIndex <= low < high.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     * @throws EntryCompactedException
     * @throws GetEntriesWrongParametersException
     */
    protected void checkBound(long low, long high) throws EntryCompactedException, GetEntriesWrongParametersException {
        if (low > high) {
            logger.error("invalid getEntries: parameter: {} > {}", low, high);
            throw new GetEntriesWrongParametersException(low, high);
        }
        long first = getFirstIndex();
        if (low < first) {
            logger.error("CheckBound out of index: parameter: {} , lower bound: {} ", low, high);
            throw new EntryCompactedException(low, first);
        }
        long upper = getLastIndex() + 1;
        if (high > upper) {
            logger.info("CheckBound out of index: parameter: {} , upper bound: {} ", first, upper);
        }
    }

//    public Log getLogByIndex(long index) throws EntryCompactedException {
//        Log log = null;
//        if (index > committed) {
//            log = unCommittedEntryManager.getEntries(index, index + 1).get(0);
//        } else {
//            log = committedEntryManager.getEntries(index, index + 1).get(0);
//        }
//        return log;
//    }

    /**
     * matchTerm returns whether the parameters passed in match.
     *
     * @param term request entry term
     * @param index request entry index
     * @return true or false
     */
    protected boolean matchTerm(long term, long index) {
        long t;
        try {
            t = getTerm(index);
        } catch (Exception e) {
            return false;
        }
        return t == term;
    }

    /**
     *  findConflict finds the index of the conflict.
     *  It returns the first pair of conflicting entries between the existing
     *  entries and the given entries, if there are any.
     *  If there is no conflicting entries, and the existing entries contains
     *  all the given entries, zero will be returned.
     *  If there is no conflicting entries, but the given entries contains new
     *  entries, the index of the first new entry will be returned.
     *  An entry is considered to be conflicting if it has the same index but
     *  a different term.
     *  The index of the given entries MUST be continuously increasing.
     *
     * @param entries request entries
     * @return 0 or conflictIndex
     */
    protected long findConflict(List<Log> entries) {
        for (Log entry : entries) {
            if (!matchTerm(entry.getCurrLogTerm(), entry.getCurrLogIndex())) {
                if (entry.getCurrLogIndex() <= getLastIndex()) {
                    logger.info("found conflict at index {}",
                            entry.getCurrLogIndex());
                }
                return entry.getCurrLogIndex();
            }
        }
        return 0;
    }

    @TestOnly
    public RaftLogManager() {
        this.committedEntryManager = new CommittedEntryManager();
        this.stableEntryManager = new StableEntryManager();
        long last = committedEntryManager.getLastIndex();
        this.unCommittedEntryManager = new UnCommittedEntryManager(last + 1);
        this.committed = -1;
        this.applied = -1;
    }
}