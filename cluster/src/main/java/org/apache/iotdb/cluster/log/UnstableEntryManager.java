package org.apache.iotdb.cluster.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UnstableEntryManager {
    private static final Logger logger = LoggerFactory.getLogger(UnstableEntryManager.class);
    // all entries that have not yet been written to stableEntryManager.
    // TODO ArrayList's capacity can't be accessed easily, so Maybe t's necessary to use another data structure to avoid invalid memory footprint
    private List<Log> entries;
    private long offset;

    public UnstableEntryManager(long offset) {
        this.offset = offset;
        this.entries = new ArrayList<>();
    }

    /**
     * maybeLastIndex returns the last index if it has at least one unstable entry.
     *
     * @return set to -1 if unstable entries are empty, or return the last index.
     */
    public long maybeLastIndex() {
        int entryNum = entries.size();
        if (entryNum != 0) {
            return offset + entryNum - 1;
        }
        return -1;
    }

    public long maybeTerm(long index) {
        if (index < offset) {
            return -1;
        }
        long last = maybeLastIndex();
        if (last == -1 || index > last) {
            return -1;
        }
        return entries.get((int) (index - offset)).getCurrLogTerm();
    }

    public void stableTo(long index, long term) {
        long localTerm = maybeTerm(index);
        if (localTerm == -1) {
            return;
        }
        if (localTerm == term && index >= offset) {
            entries = entries.subList((int) (index + 1 - offset), entries.size());
            offset = index + 1;
            //shrinkEntriesArray();
        }
    }

    public void shrinkEntriesArray() {
        final int lenMultiple = 2;
        if (entries.size() == 0) {
            return;
        }
    }

    public void restoreSnap(long index) {
        if (index >= offset + entries.size()) {
            offset = index + 1;
            entries.clear();
            entries = new ArrayList<>();
        }
    }

    public void truncateAndAppend(List<Log> ents) {
        long after = ents.get(0).getCurrLogIndex();
        long len = after - offset;
        if (len == entries.size()) {
            entries.addAll(ents);
        } else if (len <= 0) {
            logger.error("The logs which first index is {} are going to truncate committed logs", after);
        } else {
            logger.info("truncate the  entries before index {}", after);
            List<Log> newEntries = new ArrayList<>();
            entries.addAll(slice(offset, after));
            entries.addAll(ents);
            entries = newEntries;
        }
    }

    public List<Log> slice(long low, long high) {
        checkBound(low, high);
        return entries.subList((int) (low - offset), (int) (high - offset));
    }

    public void checkBound(long low, long high) {
        if (low > high) {
            logger.error("invalid unstable.slice {} > {}", low, high);
        }
        long upper = offset + entries.size();
        if (low < offset || high > upper) {
            logger.error("CheckBound out of index: parameter: {}/{} , boundary: {}/{} ", low, high, offset, upper);
        }
    }
}