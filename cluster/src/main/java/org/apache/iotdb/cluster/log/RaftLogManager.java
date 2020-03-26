package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
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

    public long commitTo(long commitIndex) {
        if (committed < commitIndex) {
            List<Log> entries = unCommittedEntryManager.getEntries(unCommittedEntryManager.getFirstUnCommittedIndex(), commitIndex);
            stableEntryManager.append(entries);
            committedEntryManager.append(entries);
            unCommittedEntryManager.stableTo(commitIndex, entries.get(entries.size() - 1).getCurrLogTerm());
            committed = commitIndex;
        }
        return committed;
    }

    public long getCommitIndex(){
        return committed;
    }

    public boolean logValid(long index) {
        return index >= committedEntryManager.getFirstIndex();
    }

    public List<Log> getEntries(long low, long high) throws EntryUnavailableException,EntryCompactedException {
        checkBound(low, high);
        if (low == high) {
            return null;
        }
        List<Log> entries = null;
        if (low < unCommittedEntryManager.getFirstUnCommittedIndex()) {
            entries = committedEntryManager.getEntries(low, Math.min(high, unCommittedEntryManager.getFirstUnCommittedIndex()));
        }
        if (high > unCommittedEntryManager.getFirstUnCommittedIndex()) {
            List<Log> unCommittedEntries = unCommittedEntryManager.getEntries(Math.max(low, unCommittedEntryManager.getFirstUnCommittedIndex()), high);
            if (entries == null) {
                entries = new ArrayList<>();
            }
            entries.addAll(unCommittedEntries);
        }
        return entries;
    }

    public void checkBound(long low, long high) throws EntryCompactedException,EntryUnavailableException {
        if (low > high) {
            logger.error("invalid unstable.slice {} > {}", low, high);
        }
        long first = getFirstIndex();
        if (low < first) {
            throw new EntryCompactedException();
        }
        long upper = getLastIndex() + 1;
        if (high > upper) {
            logger.error("CheckBound out of index: parameter: {}/{} , boundary: {}/{} ", low, high, first, upper);
            throw new EntryUnavailableException();
        }
    }

    public long append(List<Log> entries) {
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

    public boolean matchTerm(long term, long index) {
        long t = getTerm(index);
        if (t == -1) {
            return false;
        }
        return t == term;
    }


    public Log getLogByIndex(long index)throws EntryCompactedException,EntryUnavailableException {
        Log log = null;
        if (index > committed) {
            log = unCommittedEntryManager.getEntries(index, index + 1).get(0);
        } else {
            log = committedEntryManager.getEntries(index, index + 1).get(0);
        }
        return log;
    }


    public boolean isUpToDate(long term, long lastIndex) {
        return term > getLastTerm() || (term == getLastTerm() && lastIndex >= getLastIndex());
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
}