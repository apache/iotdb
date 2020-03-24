package org.apache.iotdb.cluster.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RaftLogManager {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogManager.class);

    UnstableEntryManager unstable;
    StableEntryCacheManager stableCache;
    StableEntryManager stable;

    long committed;
    long applied;

    public RaftLogManager(StableEntryCacheManager stableCache, StableEntryManager stable) {
        this.stableCache = stableCache;
        this.stable = stable;
        long first = stableCache.getFirstIndex();
        long last = stableCache.getLastIndex();
        this.unstable = new UnstableEntryManager(last + 1);
        this.committed = first - 1;
        this.applied = first - 1;
    }


    public long getTerm(long index) {
        long dummyIndex = getFirstIndex() - 1;
        if (index < dummyIndex || index > getLastIndex()) {
            return -1;
        }
        long term = unstable.maybeTerm(index);
        if (term != -1) {
            return term;
        }
        try {
            term = stableCache.getTerm(index);
        } catch (Exception e) {
            return -1;
        }
        return term;
    }

    public long getFirstIndex() {
        return stableCache.getFirstIndex();
    }

    public long getLastIndex() {
        long last = unstable.maybeLastIndex();
        if (last != -1) {
            return last;
        }
        return stableCache.getLastIndex();
    }

    public long getLastTerm() {
        long term = getTerm(getLastIndex());
        if (term == -1) {
            logger.error("unexpected error when getting the last term");
        }
        return term;
    }

    public boolean matchTerm(long term, long index) {
        long t = getTerm(index);
        if (t == -1) {
            return false;
        }
        return t == term;
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

    public long append(List<Log> entries) {
        if (entries.size() == 0) {
            return getLastIndex();
        }
        long after = entries.get(0).getCurrLogIndex() - 1;
        if (after < committed) {
            logger.error("after({}) is out of range [committed({})]", after, committed);
        }
        unstable.truncateAndAppend(entries);
        return getLastIndex();
    }
}