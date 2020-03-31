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

    private HardState hardState;
    private RaftSnapshot snapshot;
    private List<Log> entries;

    public CommittedEntryManager(RaftSnapshot snapshot) {
        this.snapshot = snapshot;
        PhysicalPlanLog dummy = new PhysicalPlanLog(snapshot.getLastIndex(), snapshot.getLastTerm());
        entries = new ArrayList<Log>() {{
            add(dummy);
        }};
    }

    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public HardState getHardState() {
        return hardState;
    }

    public RaftSnapshot getSnapshot() {
        return snapshot;
    }

    public void applyingSnapshot(RaftSnapshot snap) {
        long localIndex = snapshot.getLastIndex();
        long snapIndex = snap.getLastIndex();
        if (localIndex >= snapIndex) {
            logger.info("requested index is older than the existing snapshot");
            return;
        }
        snapshot = snap;
        entries.clear();
        Log dummy = new PhysicalPlanLog(snap.getLastIndex(), snap.getLastTerm());
        entries.add(dummy);
    }

    public Long getDummyIndex() {
        return entries.get(0).getCurrLogIndex();
    }

    public Long getFirstIndex() {
        return getDummyIndex() + 1;
    }

    public Long getLastIndex() {
        return entries.get(0).getCurrLogIndex() + entries.size() - 1;
    }

    public long getTerm(long index) {
        long offset = entries.get(0).getCurrLogIndex();
        if (index < offset) {
            return -1;
        }
        if ((int) (index - offset) >= entries.size()) {
            return -1;
        }
        return entries.get((int) (index - offset)).getCurrLogTerm();
    }

    public List<Log> getEntries(long low, long high) throws EntryCompactedException {
        long offset = entries.get(0).getCurrLogIndex();
        if (low <= offset || entries.size() == 1) {
            logger.error("entries before request index ({}) have been compacted, and the compactIndex is ({})", low, offset);
            throw new EntryCompactedException(low, offset);
        }
        if (high > getLastIndex() + 1) {
            logger.error("entries high ({}) is out of bound lastIndex ({})", high, getLastIndex());
            high = getLastIndex() + 1;
        }
        return entries.subList((int) (low - offset), (int) (high - offset));
    }

    public void compactEntries(long compactIndex) throws EntryUnavailableException {
        long offset = entries.get(0).getCurrLogIndex();
        if (compactIndex <= offset) {
            logger.error("entries before request index ({}) have been compacted, and the compactIndex is ({})", compactIndex, offset);
            return;
        }
        if (compactIndex > getLastIndex()) {
            logger.error("compact ({}) is out of bound lastIndex ({})", compactIndex, getLastIndex());
            throw new EntryUnavailableException(compactIndex, getLastIndex());
        }
        int index = (int) (compactIndex - offset);
        PhysicalPlanLog dummy = new PhysicalPlanLog(entries.get(index).getCurrLogTerm(), entries.get(index).getCurrLogIndex());
        entries.set(0, dummy);
        entries.subList(1, index + 1).clear();
    }

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
            //maybe not throw a exception is better.It depends on the caller's implementation.
//            logger.error("The logs which first index is {} are going to truncate committed logs", appendingEntries.get(0).getCurrLogIndex());
//            throw new TruncateCommittedEntryException(appendingEntries.get(0).getCurrLogIndex(),getLastIndex());
            entries.subList((int) offset, entries.size()).clear();
            entries.addAll(appendingEntries);
        } else {
            logger.error("missing log entry [last: {}, append at: {}]", getLastIndex(), appendingEntries.get(0).getCurrLogIndex());
        }
    }

    @TestOnly
    public CommittedEntryManager(List<Log> entries,RaftSnapshot snapshot) {
        this.entries = entries;
        this.snapshot = snapshot;
    }

    @TestOnly
    public List<Log> getAllEntries() {
        return entries;
    }
}