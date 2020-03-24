package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.StableEntryCompactedException;
import org.apache.iotdb.cluster.exception.StableEntrySnapOutOfDateException;
import org.apache.iotdb.cluster.exception.StableEntryUnavailable;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StableEntryCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(StableEntryCacheManager.class);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private HardState hardState;
    private Snapshot snapshot;
    private List<Log> entries;

    public StableEntryCacheManager(HardState hardState) {
        this.hardState = hardState;
        entries = new ArrayList<Log>() {{
            add(new PhysicalPlanLog());
        }};
    }

    public void setHardState(HardState hardState) {
        try {
            lock.writeLock().lock();
            this.hardState = hardState;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public HardState getHardState() {
        HardState state;
        try {
            lock.readLock().lock();
            state = hardState;
        } finally {
            lock.readLock().unlock();
        }
        return state;
    }

    public List<Log> getEntries(long low, long high) throws StableEntryCompactedException, StableEntryUnavailable {
        List<Log> ans;
        try {
            lock.readLock().lock();
            long offset = entries.get(0).getCurrLogIndex();
            if (low < offset) {
                throw new StableEntryCompactedException();
            }
            if (high > lastIndex() + 1) {
                logger.error("entries' high({}) is out of bound lastindex({})", high, lastIndex());
            }
            if (entries.size() == 1) {
                throw new StableEntryUnavailable();
            }
            ans = entries.subList((int) (low - offset), (int) (high - offset));
        } finally {
            lock.readLock().unlock();
        }
        return ans;
    }

    public Long getFirstIndex() {
        Long index;
        try {
            lock.readLock().lock();
            index = firstIndex();
        } finally {
            lock.readLock().unlock();
        }
        return index;
    }

    public Long getLastIndex() {
        Long index;
        try {
            lock.readLock().lock();
            index = lastIndex();
        } finally {
            lock.readLock().unlock();
        }
        return index;
    }

    public Snapshot getSnapshot() {
        Snapshot snap;
        try {
            lock.readLock().lock();
            snap = snapshot;
        } finally {
            lock.readLock().unlock();
        }
        return snap;
    }

    public long getTerm(long index) throws StableEntryCompactedException, StableEntryUnavailable {
        long term;
        try {
            lock.readLock().lock();
            long offset = entries.get(0).getCurrLogIndex();
            if (index < offset) {
                throw new StableEntryCompactedException();
            }
            if ((int) (index - offset) > entries.size()) {
                throw new StableEntryUnavailable();
            }
            term = entries.get((int) (index - offset)).getCurrLogTerm();
        } finally {
            lock.readLock().unlock();
        }
        return term;
    }

    public void applySnapshot(Snapshot snap) throws StableEntrySnapOutOfDateException {
        try {
            lock.writeLock().lock();
            long msIndex = snapshot.getLastLogIndex();
            long snapIndex = snap.getLastLogIndex();
            if (msIndex >= snapIndex) {
                throw new StableEntrySnapOutOfDateException();
            }
            snapshot = snap;
            entries.clear();
            Log dummy = new PhysicalPlanLog();
            dummy.setCurrLogIndex(snap.getLastLogIndex());
            dummy.setCurrLogTerm(snap.getLastLogTerm());
            entries = new ArrayList<Log>() {{
                add(dummy);
            }};
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void Compact(long compactIndex) throws StableEntryCompactedException {
        try {
            lock.writeLock().lock();
            long offset = entries.get(0).getCurrLogIndex();
            if (compactIndex < offset) {
                throw new StableEntryCompactedException();
            }
            if (compactIndex > lastIndex()) {
                logger.error("compact {} is out of bound lastindex({})", compactIndex, lastIndex());
            }
            int i = (int) (compactIndex - offset);
            Log dummy = new PhysicalPlanLog();
            dummy.setCurrLogIndex(entries.get(i).getCurrLogIndex());
            dummy.setCurrLogTerm(entries.get(i).getCurrLogTerm());
            List<Log> newEntries = new ArrayList<Log>() {{
                add(dummy);
            }};
            newEntries.addAll(entries.subList(i + 1, entries.size()));
            entries = newEntries;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void Append(List<Log> entries) {
        try {
            lock.writeLock().lock();
            if (entries.size() == 0) {
                return;
            }
            long first = firstIndex();
            long last = entries.get(0).getCurrLogIndex() + entries.size() - 1;
            if (last < first) {
                return;
            }
            if (first > entries.get(0).getPreviousLogIndex()) {
                entries = entries.subList((int) (first - entries.get(0).getPreviousLogIndex()), entries.size());
            }
            long offset = entries.get(0).getCurrLogIndex() - this.entries.get(0).getCurrLogIndex();
            if (this.entries.size() - offset == 0) {
                this.entries.addAll(entries);
            } else if (this.entries.size() - offset > 0) {
                List<Log> newEntries = new ArrayList<>();
                newEntries.addAll(this.entries.subList(0, (int) (offset)));
                newEntries.addAll(entries);
                entries.addAll(newEntries);
            } else {
                logger.error("missing log entry [last: {}, append at: {}]", lastIndex(), entries.get(0).getCurrLogIndex());
            }

        } finally {
            lock.writeLock().unlock();
        }
    }


    private long firstIndex() {
        return entries.get(0).getCurrLogIndex() + 1;
    }

    private long lastIndex() {
        return entries.get(0).getCurrLogIndex() + entries.size() - 1;
    }

}