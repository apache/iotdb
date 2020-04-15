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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.db.utils.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Return the first uncommitted index.
     *
     * @return offset
     */
    public long getFirstUnCommittedIndex() {
        return offset;
    }


    /**
     * Return last entry's index if this instance has at least one uncommitted entry.
     *
     * @return -1 if entries are empty, or last entry's index
     */
    public long maybeLastIndex() {
        int entryNum = entries.size();
        if (entryNum != 0) {
            return offset + entryNum - 1;
        }
        return -1;
    }

    /**
     * Return the entry's term for given index. Note that the called should ensure index >= offset.
     *
     * @param index request entry index
     * @return -1 if index < offset, throw EntryUnavailableException if index > last or entries is
     * empty, or return the entry's term for given index
     * @throws EntryUnavailableException
     */
    public long maybeTerm(long index) throws EntryUnavailableException {
        if (index < offset) {
            logger.debug(
                "invalid unCommittedEntryManager maybeTerm : parameter: index({}) < offset({})",
                index, offset);
            return -1;
        }
        long last = maybeLastIndex();
        if (last == -1 || index > last) {
            long boundary = last == -1 ? offset - 1 : last;
            logger.info(
                "unCommittedEntryManager maybeTerm out of bound : parameter: index({}) > lastIndex({})",
                index, boundary);
            throw new EntryUnavailableException(index, boundary);
        }
        return entries.get((int) (index - offset)).getCurrLogTerm();
    }

    /**
     * Remove useless prefix entries as long as these entries has been committed and persisted. This
     * method is only called after persisting newly committed entries.
     *
     * @param index request entry's index
     * @param term  request entry's term
     */
    public void stableTo(long index, long term) {
        try {
            long entryTerm = maybeTerm(index);
            // only update the uncommitted entries if term is matched with an uncommitted entry.
            if (entryTerm == term) {
                entries.subList(0, (int) (index + 1 - offset)).clear();
                offset = index + 1;
            }
        } catch (EntryUnavailableException e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * Update offset and clear entries because leader's snapshot is more up-to-date. This method is
     * only called for applying snapshot from leader.
     *
     * @param snapshot leader's snapshot
     */
    public void applyingSnapshot(Snapshot snapshot) {
        this.offset = snapshot.getLastLogIndex() + 1;
        this.entries.clear();
    }

    /**
     * TruncateAndAppend uncommitted entries. This method will truncate conflict entries if it finds
     * inconsistencies. Note that the caller should ensure appendingEntries[0].index <=
     * entries[entries.size()-1].index + 1. Note that the caller should ensure not to truncate entries
     * which have been committed.
     *
     * @param appendingEntries request entries
     */
    public void truncateAndAppend(List<Log> appendingEntries) {
        long after = appendingEntries.get(0).getCurrLogIndex();
        long len = after - offset;
        if (len < 0) {
            // the logs are being truncated to before our current offset portion, which is committed entries
            // unconditional obedience to the leader's request. Maybe throw a exception here is better
//            offset = after;
//            entries = appendingEntries;
            logger.error("The logs which first index is {} are going to truncate committed logs",
                after);
        } else if (len == entries.size()) {
            // after is the next index in the entries
            // directly append
            entries.addAll(appendingEntries);
        } else {
            // clear conflict entries
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
     * Pack entries from low through high - 1, just like slice (entries[low:high]). offset <= low <=
     * high. Note that caller must ensure low <= high.
     *
     * @param low  request index low bound
     * @param high request index upper bound
     */
    public List<Log> getEntries(long low, long high) {
        if (low > high) {
            logger
                .debug("invalid unCommittedEntryManager getEntries: parameter: low({}) > high({})",
                    low, high);
        }
        long upper = offset + entries.size();
        if (low > upper) {
            // don't throw a exception to support
            // getEntries(low, Integer.MAX_VALUE) if low is larger than lastIndex.
            logger.info(
                "unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}] , return empty ArrayList",
                low, high, offset, upper);
            return Collections.emptyList();
        }
        if (low < offset) {
            logger.debug("unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}]", low,
                high, offset, upper);
            low = offset;
        }
        if (high > upper) {
            logger.info(
                "unCommittedEntryManager getEntries[{},{}) out of bound : [{},{}] , adjust parameter 'high' to {}",
                low, high, offset, upper, upper);
            // don't throw a exception to support getEntries(low, Integer.MAX_VALUE).
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