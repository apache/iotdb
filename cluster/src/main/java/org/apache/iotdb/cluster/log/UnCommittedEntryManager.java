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

import org.apache.iotdb.cluster.exception.EntryUnavailableException;
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
     * @return set to -1 if entries are empty, or return the last entry index.
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
     * @param index request entry index
     * @return set to -1 if index < index,throw exception if index > last, or return the entry's term.
     * @throws EntryUnavailableException
     */
    public long maybeTerm(long index) throws EntryUnavailableException {
        if (index < offset) {
            logger.error("invalid unCommittedEntryManager maybeTerm: parameter: index({}) < offset({})", index, offset);
            return -1;
        }
        long last = maybeLastIndex();
        if (last == -1 || index > last) {
            long boundary = last == -1 ? offset - 1 : last;
            logger.info("invalid unCommittedEntryManager maybeTerm: parameter: index({}) > lastIndex({})", index, boundary);
            throw new EntryUnavailableException(index, boundary);
        }
        return entries.get((int) (index - offset)).getCurrLogTerm();
    }

    /**
     * stableTo remove useless prefix entries as long as these entries has been committed and persisted,
     * It is often called after snapshot persistence and log persistence.
     *
     * @param index request entry index
     * @param term  request entry term
     */
    public void stableTo(long index, long term) {
        try {
            long entryTerm = maybeTerm(index);
            // only update the uncommitted entries if term is matched with
            // an uncommitted entry.
            if (entryTerm == term) {
                entries.subList(0, (int) (index + 1 - offset)).clear();
                offset = index + 1;
            }
        } catch (EntryUnavailableException e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * truncateAndAppend append uncommitted entries.It will truncate conflict entries if it find inconsistencies.
     * Node that the caller must handle the case when len > entries.size() and ensure not to truncate entries which are managed by CommittedEntryManager.
     *
     * @param appendingEntries request entries
     */
    public void truncateAndAppend(List<Log> appendingEntries) {
        long after = appendingEntries.get(0).getCurrLogIndex();
        long len = after - offset;
        if (len < 0) {
            // The log is being truncated to before our current offset
            // portion, which is committed entries
            offset = after;
            entries = appendingEntries;
            logger.error("The logs which first index is {} are going to truncate committed logs", after);
        } else if (len == entries.size()) {
            // after is the next index in the entries
            // directly append
            entries.addAll(appendingEntries);
        } else {
            // truncate wrong entries
            // then append
            logger.info("truncate the entries after index {}", after);
            int truncateIndex = (int) (after - offset);
            if (truncateIndex < entries.size()) {
                entries.subList(truncateIndex, entries.size()).clear();
            }
            entries.addAll(appendingEntries);
        }
    }

    /**
     * getEntries pack entries from low to high, just like slice (entries[low:high]).
     * offset <= low < high <= offset + entries.size().
     * Note that caller must ensure low < high.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     */
    public List<Log> getEntries(long low, long high) {
        if (low > high) {
            logger.error("invalid unCommittedEntryManager getEntries: parameter: {} > {}", low, high);
        }
        long upper = offset + entries.size();
        if (low > upper) {
            logger.error("unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}]", low, high, offset, upper);
            return new ArrayList<>();
        }
        if (low < offset) {
            logger.error("unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}]", low, high, offset, upper);
            low = offset;
        }
        if (high > upper) {
            logger.info("unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}] , adjust 'high' to {}", low, high, offset, upper, upper);
            //not throw a exception to support getEntries(low, Integer.MAX_VALUE);
            high = upper;
        }
        return entries.subList((int) (low - offset), (int) (high - offset));
    }

    @TestOnly
    public UnCommittedEntryManager(long offset, List<Log> entries) {
        this.offset = offset;
        this.entries = entries;
    }

    @TestOnly
    public List<Log> getAllEntries() {
        return entries;
    }
}