package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CommittedEntryManager {

    private static final Logger logger = LoggerFactory.getLogger(CommittedEntryManager.class);

    private HardState hardState;
    private Snapshot snapshot;
    private List<Log> entries;

    public CommittedEntryManager(HardState hardState) {
        this.hardState = hardState;
        PhysicalPlanLog dummy = new PhysicalPlanLog();
        dummy.setCurrLogIndex(-1);
        dummy.setCurrLogTerm(-1);
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

    public Long getFirstIndex() {
        return entries.get(0).getCurrLogIndex() + 1;
    }

    public Long getLastIndex() {
        return entries.get(0).getCurrLogIndex() + entries.size() - 1;
    }

//    public Snapshot getSnapshot() {
//        return snapshot;
//    }

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

    public List<Log> getEntries(long low, long high) throws EntryCompactedException, EntryUnavailableException {
        List<Log> ans;
        long offset = entries.get(0).getCurrLogIndex();
        if (low < offset) {
            throw new EntryCompactedException();
        }
        if (high > getLastIndex() + 1) {
            logger.error("entries high ({}) is out of bound lastIndex ({})", high, getLastIndex());
        }
        if (entries.size() == 1) {
            throw new EntryUnavailableException();
        }
        ans = entries.subList((int) (low - offset), (int) (high - offset));
        return ans;
    }

    public void applySnapshot(Snapshot snap) {
        long localIndex = snapshot.getLastLogIndex();
        long snapIndex = snap.getLastLogIndex();
        if (localIndex >= snapIndex) {
            logger.info("requested index is older than the existing snapshot");
            return;
        }
        snapshot = snap;
        entries.clear();
        Log dummy = new PhysicalPlanLog();
        dummy.setCurrLogIndex(snap.getLastLogIndex());
        dummy.setCurrLogTerm(snap.getLastLogTerm());
        entries = new ArrayList<Log>() {{
            add(dummy);
        }};
    }

    public void compactEntries(long compactIndex) throws EntryCompactedException {
        long offset = entries.get(0).getCurrLogIndex();
        if (compactIndex < offset) {
            throw new EntryCompactedException();
        }
        if (compactIndex > getLastIndex()) {
            logger.error("compact ({}) is out of bound lastIndex ({})", compactIndex, getLastIndex());
        }
        int index = (int) (compactIndex - offset);
        Log dummy = new PhysicalPlanLog();
        dummy.setCurrLogIndex(entries.get(index).getCurrLogIndex());
        dummy.setCurrLogTerm(entries.get(index).getCurrLogTerm());
        List<Log> newEntries = new ArrayList<Log>() {{
            add(dummy);
        }};
        newEntries.addAll(entries.subList(index + 1, entries.size()));
        entries = newEntries;
    }

    public void append(List<Log> entries) {
        if (entries.size() == 0) {
            return;
        }
        long first = getFirstIndex();
        long last = entries.get(0).getCurrLogIndex() + entries.size() - 1;
        if (last < first) {
            return;
        }
        if (first > entries.get(0).getCurrLogIndex()) {
            entries = entries.subList((int) (first - entries.get(0).getCurrLogIndex()), entries.size());
        }
        long offset = entries.get(0).getCurrLogIndex() - this.entries.get(0).getCurrLogIndex();
        if (this.entries.size() - offset == 0) {
            this.entries.addAll(entries);
        } else if (this.entries.size() - offset > 0) {
            List<Log> newEntries = new ArrayList<>();
            newEntries.addAll(this.entries.subList(0, (int) (offset)));
            newEntries.addAll(entries);
           this.entries = newEntries;
        } else {
            logger.error("missing log entry [last: {}, append at: {}]", getLastIndex(), entries.get(0).getCurrLogIndex());
        }

    }
}