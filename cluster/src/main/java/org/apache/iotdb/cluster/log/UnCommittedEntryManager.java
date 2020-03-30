package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryStabledException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.GetEntriesWrongParametersException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UnCommittedEntryManager {
    private static final Logger logger = LoggerFactory.getLogger(UnCommittedEntryManager.class);
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
     *
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
     * It is often called after snapshot persistence and log persistence.
     *
     * @param index
     * @param term
     */
    public void stableTo(long index, long term) {
        long entryTerm = maybeTerm(index);
        // only update the uncommitted entries if term is matched with
        // an uncommitted entry.
        if (entryTerm == term) {
            entries.subList(0, (int) (index + 1 - offset)).clear();
            offset = index + 1;
        }
    }

    /**
     * truncateAndAppend append uncommitted entries.It will truncate conflict entries if it find inconsistencies.
     *
     * @param entries
     * @throws TruncateCommittedEntryException
     */
    public void truncateAndAppend(List<Log> entries) throws TruncateCommittedEntryException {
        long after = entries.get(0).getCurrLogIndex();
        long len = after - offset;
        if (len == this.entries.size()) {
            // after is the next index in the entries
            // directly append
            this.entries.addAll(entries);
        } else if (len < 0) {
            // The log is being truncated to before our current offset
            // portion, which is committed entries
            // throws exception
            logger.error("The logs which first index is {} are going to truncate committed logs", after);
            throw new TruncateCommittedEntryException();
        } else {
            // truncate wrong entries
            // then append
            logger.info("truncate the entries after index {}", after);
            this.entries.subList((int) (after - offset), this.entries.size()).clear();
            this.entries.addAll(entries);
        }
    }

    /**
     * getEntries pack entries from low to high - 1, just like slice (entries[low:high]).
     *
     * @param low
     * @param high
     * @throws GetEntriesWrongParametersException
     * @throws EntryStabledException
     * @throws EntryUnavailableException
     */
    public List<Log> getEntries(long low, long high) throws GetEntriesWrongParametersException, EntryStabledException, EntryUnavailableException {
        checkBound(low, high);
        return entries.subList((int) (low - offset), (int) (high - offset));
    }

    /**
     * checkBound check whether the parameters passed meet the following properties.
     * offset <= low <= high <= offset+entries.size().
     *
     * @param low
     * @param high
     * @throws GetEntriesWrongParametersException
     * @throws EntryStabledException
     * @throws EntryUnavailableException
     */
    private void checkBound(long low, long high) throws GetEntriesWrongParametersException, EntryStabledException, EntryUnavailableException {
        if (low >= high) {
            logger.error("invalid getEntries: parameter: {} >= {}", low, high);
            throw new GetEntriesWrongParametersException(low, high);
        }
        long upper = offset + entries.size();
        if (low < offset) {
            logger.error("invalid getEntries: parameter: {}/{} , boundary: {}/{}", low, high, offset, upper);
            throw new EntryStabledException();
        }
        if (high > upper) {
            logger.error("invalid getEntries: parameter: {}/{} , boundary: {}/{}", low, high, offset, upper);
            throw new EntryUnavailableException();
        }
    }

    @TestOnly
    public UnCommittedEntryManager(long offset, List<Log> entries) {
        this.offset = offset;
        this.entries = entries;
    }

    @TestOnly
    public List<Log> getEntries() {
        return entries;
    }
}