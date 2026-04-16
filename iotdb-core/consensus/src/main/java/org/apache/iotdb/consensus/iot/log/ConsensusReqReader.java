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

package org.apache.iotdb.consensus.iot.log;

import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** This interface provides search interface for consensus requests via index. */
public interface ConsensusReqReader {

  /** this insert node doesn't need to participate in iot consensus. */
  long DEFAULT_SEARCH_INDEX = -1;

  /** iot consensus cannot delete any insert nodes. */
  long DEFAULT_SAFELY_DELETED_SEARCH_INDEX = Long.MIN_VALUE;

  void setSafelyDeletedSearchIndex(long safelyDeletedSearchIndex);

  /**
   * Gets the consensus requests iterator from the specified start position.
   *
   * @param startIndex index of the start consensus request
   * @return the consensus requests iterator from the specified start position.
   */
  ReqIterator getReqIterator(long startIndex);

  /** This iterator provides blocking and non-blocking interfaces to read consensus request. */
  interface ReqIterator {
    /** Like {@link Iterator#hasNext()} */
    boolean hasNext();

    /**
     * Like {@link Iterator#next()}.
     *
     * @throws java.util.NoSuchElementException if the iteration has no more elements, wait a moment
     *     or call {@link this#waitForNextReady} for more elements.
     */
    IndexedConsensusRequest next();

    /**
     * Wait for the next element in the iteration ready, blocked until next element is available.
     *
     * @throws InterruptedException
     */
    void waitForNextReady() throws InterruptedException;

    /**
     * Wait for the next element in the iteration ready, blocked until next element is available or
     * a specified amount of time has elapsed.
     *
     * @throws InterruptedException
     * @throws TimeoutException
     */
    void waitForNextReady(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * Skips to target position of next element in the iteration <br>
     * . Notice: The correctness of forward skipping should be guaranteed by the caller.
     *
     * @param targetIndex target position of next element in the iteration.
     */
    void skipTo(long targetIndex);
  }

  /** Get current search index. */
  long getCurrentSearchIndex();

  /** Get current wal file version. */
  long getCurrentWALFileVersion();

  /** Get total size of wal files. */
  long getTotalSize();

  /**
   * Get disk usage of this specific WAL node (region-local), as opposed to {@link #getTotalSize()}
   * which returns the global WAL disk usage across all WAL nodes.
   */
  default long getRegionDiskUsage() {
    return getTotalSize();
  }

  /**
   * Calculate the search index boundary that, if used as safelyDeletedSearchIndex, would free at
   * least {@code bytesToFree} bytes of WAL files from the oldest files of this WAL node.
   *
   * @param bytesToFree the minimum number of bytes to free
   * @return the startSearchIndex of the WAL file just after the freed range, or {@link
   *     #DEFAULT_SAFELY_DELETED_SEARCH_INDEX} if no files need to be freed
   */
  default long getSearchIndexToFreeAtLeast(long bytesToFree) {
    // Default implementation: if any freeing is needed, allow deleting everything.
    return bytesToFree > 0 ? Long.MAX_VALUE : DEFAULT_SAFELY_DELETED_SEARCH_INDEX;
  }

  /**
   * Set the minimum WAL file versionId that must be retained for subscription consumers. Files with
   * versionId >= this value will not be deleted, regardless of their WALFileStatus. This protects
   * Follower WAL files (CONTAINS_NONE_SEARCH_INDEX) from being deleted while subscriptions need
   * them.
   *
   * @param minVersionId the minimum versionId to retain; Long.MAX_VALUE means no retention
   */
  default void setSubscriptionRetainedMinVersionId(long minVersionId) {
    // no-op by default
  }

  /**
   * Calculate the minimum WAL file versionId to retain such that freeing all files with versionId
   * below that value would release at least {@code bytesToFree} bytes.
   *
   * @param bytesToFree the minimum number of bytes to free
   * @return the versionId boundary; files with versionId < this can be freed
   */
  default long getVersionIdToFreeAtLeast(long bytesToFree) {
    return bytesToFree > 0 ? Long.MAX_VALUE : 0;
  }

  /**
   * Calculate the search index boundary that, if used as safelyDeletedSearchIndex, would free the
   * oldest rolled WAL files whose lastModified time is earlier than {@code cutoffTimeMs}. The
   * currently written WAL file is never considered deletable by this method.
   *
   * @param cutoffTimeMs files strictly older than this timestamp may be freed
   * @return the search index boundary of the first retained file, or Long.MIN_VALUE + 1 when no
   *     rolled WAL files are old enough to be freed
   */
  default long getSearchIndexToFreeBeforeTimestamp(long cutoffTimeMs) {
    return Long.MIN_VALUE + 1;
  }

  /**
   * Calculate the minimum retained WAL versionId after freeing the oldest rolled WAL files whose
   * lastModified time is earlier than {@code cutoffTimeMs}.
   *
   * @param cutoffTimeMs files strictly older than this timestamp may be freed
   * @return the versionId boundary of the first retained file, or 0 when no rolled WAL files are
   *     old enough to be freed
   */
  default long getVersionIdToFreeBeforeTimestamp(long cutoffTimeMs) {
    return 0;
  }
}
