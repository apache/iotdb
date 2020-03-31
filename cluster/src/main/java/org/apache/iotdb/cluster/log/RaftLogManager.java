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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftLogManager {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

    UnCommittedEntryManager unCommittedEntryManager;
    CommittedEntryManager committedEntryManager;
    StableEntryManager stableEntryManager;

    long committed;
    long applied;

    public RaftLogManager(CommittedEntryManager committedEntryManager, StableEntryManager stableEntryManager) {
        this.committedEntryManager = committedEntryManager;
        this.stableEntryManager = stableEntryManager;
        long first = committedEntryManager.getFirstIndex();
        long last = committedEntryManager.getLastIndex();
        this.unCommittedEntryManager = new UnCommittedEntryManager(last + 1);
        this.committed = first - 1;
        this.applied = first - 1;
    }

    public long getTerm(long index) {
        long dummyIndex = getFirstIndex() - 1;
        if (index < dummyIndex || index > getLastIndex()) {
            return -1;
        }
        long term = unCommittedEntryManager.maybeTerm(index);
        if (term != -1) {
            return term;
        }
        return committedEntryManager.getTerm(index);
    }

    public long getFirstIndex() {
        return committedEntryManager.getFirstIndex();
    }

    public long getLastIndex() {
        long last = unCommittedEntryManager.maybeLastIndex();
        if (last != -1) {
            return last;
        }
        return committedEntryManager.getLastIndex();
    }

    public long getLastTerm() {
        long term = getTerm(getLastIndex());
        if (term == -1) {
            logger.error("unexpected error when getting the last term");
        }
        return term;
    }

    public long maybeAppend(long lastIndex, long lastTerm, long leaderCommit, List<Log> entries) throws TruncateCommittedEntryException {
        if (matchTerm(lastTerm, lastIndex)) {
            long newLastIndex = lastIndex + entries.size();
            long ci = findConflict(entries);
            if (ci == 0 || ci <= committed) {
                logger.error("entry {} conflict with committed entry [committed({})]", ci, committed);
                throw new TruncateCommittedEntryException(ci, committed);
            } else {
                long offset = lastIndex + 1;
                append(entries.subList((int) (ci - offset), entries.size()));
            }
            commitTo(Math.min(leaderCommit, newLastIndex));
            return newLastIndex;
        }
        return -1;
    }

    public long commitTo(long commitIndex) {
        if (committed < commitIndex) {
            List<Log> entries = null;
            try {
                entries = unCommittedEntryManager.getEntries(unCommittedEntryManager.getFirstUnCommittedIndex(), commitIndex);
            } catch (Exception e) {
                logger.error(e.toString());
            }
            stableEntryManager.append(entries);
            committedEntryManager.append(entries);
            unCommittedEntryManager.stableTo(commitIndex, entries.get(entries.size() - 1).getCurrLogTerm());
            committed = commitIndex;
        }
        return committed;
    }

    public long getCommitIndex() {
        return committed;
    }

    public boolean logValid(long index) {
        return index >= committedEntryManager.getFirstIndex();
    }

    public List<Log> getEntries(long low, long high) throws EntryUnavailableException, EntryCompactedException, GetEntriesWrongParametersException {
        checkBound(low, high);
        if (low == high) {
            return null;
        }
        List<Log> entries = new ArrayList<>();
        long offset = unCommittedEntryManager.getFirstUnCommittedIndex();
        if (low < offset) {
            entries.addAll(committedEntryManager.getEntries(low, Math.min(high, offset)));
        }
        if (high > offset) {
            List<Log> unCommittedEntries = unCommittedEntryManager.getEntries(Math.max(low, offset), high);
            if (unCommittedEntries != null) {
                entries.addAll(unCommittedEntries);
            }
        }
        return entries;
    }

    public Log getLogByIndex(long index) throws EntryCompactedException, EntryUnavailableException, GetEntriesWrongParametersException {
        Log log = null;
        if (index > committed) {
            log = unCommittedEntryManager.getEntries(index, index + 1).get(0);
        } else {
            log = committedEntryManager.getEntries(index, index + 1).get(0);
        }
        return log;
    }


    public boolean isLogUpToDate(long term, long lastIndex) {
        return term > getLastTerm() || (term == getLastTerm() && lastIndex >= getLastIndex());
    }

    private long append(List<Log> entries) throws TruncateCommittedEntryException {
        if (entries.size() == 0) {
            return getLastIndex();
        }
        long after = entries.get(0).getCurrLogIndex() - 1;
        if (after < committed) {
            logger.error("after({}) is out of range [committed({})]", after, committed);
        }
        unCommittedEntryManager.truncateAndAppend(entries);
        return getLastIndex();
    }

    private boolean matchTerm(long term, long index) {
        long t = getTerm(index);
        if (t == -1) {
            return false;
        }
        return t == term;
    }

    private long findConflict(List<Log> entries) {
        for (Log entry : entries) {
            if (!matchTerm(entry.getCurrLogTerm(), entry.getCurrLogIndex())) {
                if (entry.getCurrLogIndex() <= getLastIndex()) {
                    logger.info("found conflict at index {} [existing term: {}, conflicting term: {}]",
                            entry.getCurrLogIndex(), getTerm(entry.getCurrLogIndex()), entry.getCurrLogTerm());
                }
                return entry.getCurrLogIndex();
            }
        }
        return 0;
    }

    private void checkBound(long low, long high) throws EntryCompactedException, EntryUnavailableException, GetEntriesWrongParametersException {
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
            logger.error("CheckBound out of index: parameter: {} , upper bound: {} ", first, upper);
            throw new EntryUnavailableException(high, upper);
        }
    }
}