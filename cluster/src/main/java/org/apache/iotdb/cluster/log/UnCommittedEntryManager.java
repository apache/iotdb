package org.apache.iotdb.cluster.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UnCommittedEntryManager {
    private static final Logger logger = LoggerFactory.getLogger(UnCommittedEntryManager.class);
    // TODO ArrayList's capacity can't be accessed easily, so Maybe t's necessary to use another data structure to avoid invalid memory footprint
    // all entries that have not been committed.
    private List<Log> entries;
    // the first uncommitted entry index.
    private long offset;

    public UnCommittedEntryManager(long offset) {
        this.offset = offset;
        this.entries = new ArrayList<>();
    }

    /**
     * getFirstUnCommittedIndex return the first uncommitted index.
     *
     * @return offset.
     */
    public long getFirstUnCommittedIndex() {
        return offset;
    }


    /**
     * maybeLastIndex returns the last index if it has at least one uncommitted entry.
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

    /**
     * maybeTerm returns the term for given index.
     * @param index
     * @return set to -1 if the entry for this index cannot be found, or return the entry's term.
     */
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

    /**
     * stableTo remove useless prefix entries as long as these entries has been committed,
     * It is often called for snapshot persistence and log persistence.
     * @param index
     * @param term
     */
    public void stableTo(long index, long term) {
        long entryTerm = maybeTerm(index);
        // if index < offset, term is matched with committed entries or snapshot.
        // only update the uncommitted entries if term is matched with
        // an uncommitted entry.
        if (entryTerm == term) {
            entries = entries.subList((int) (index + 1 - offset), entries.size());
            offset = index + 1;
            //shrinkEntriesArray();
        }
    }

    /**
     * shrinkEntriesArray discards the underlying array used by the entries subList
     * if most of it isn't being used. This avoids holding references to a bunch of
     * potentially large entries that aren't needed anymore.
     */
    public void shrinkEntriesArray() {
        final int lenMultiple = 2;
        if (entries.size() == 0) {
            return;
        }
    }

    /**
     * truncateAndAppend append uncommitted entries.It will truncate conflict entries if it find inconsistencies.
     * @param entries
     */
    public void truncateAndAppend(List<Log> entries) {
        long after = entries.get(0).getCurrLogIndex();
        long len = after - offset;
        if (len == this.entries.size()) {
            // after is the next index in the entries
            // directly append
            this.entries.addAll(entries);
        } else if (len <= 0) {
            // The log is being truncated to before our current offset
            // portion, which is committed entries
            logger.error("The logs which first index is {} are going to truncate committed logs", after);
        } else {
            // truncate to after and copy to newEntries
            // then append
            logger.info("truncate the entries before index {}", after);
            List<Log> newEntries = new ArrayList<>();
            newEntries.addAll(getEntries(offset, after));
            newEntries.addAll(entries);
            this.entries = newEntries;
        }
    }

    /**
     * getEntries pack entries from low to high - 1, just like slice (entries[low:high]).
     * @param low
     * @param high
     */
    public List<Log> getEntries(long low, long high) {
        checkBound(low, high);
        return entries.subList((int) (low - offset), (int) (high - offset));
    }

    /**
     * checkBound check whether the parameters passed meet the following properties.
     * offset <= low <= high <= offset+entries.size().
     * @param low
     * @param high
     */
    public void checkBound(long low, long high) {
        if (low > high) {
            logger.error("invalid unstable.getEntries {} > {}", low, high);
        }
        long upper = offset + entries.size();
        if (low < offset || high > upper) {
            logger.error("CheckBound out of index: parameter: {}/{} , boundary: {}/{} ", low, high, offset, upper);
        }
    }
}