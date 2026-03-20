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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALByteBufReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator for reading WAL entries for consensus subscription using V3 metadata.
 *
 * <p>Unlike the standard PlanNodeIterator which uses searchIndex for positioning and cannot see
 * Follower-replicated entries (searchIndex=-1), this iterator uses V3 metadata arrays (epochs[],
 * syncIndices[]) to provide (epoch, syncIndex) ordering keys for ALL entries — both Leader entries
 * (searchIndex > 0) and Follower entries (searchIndex = -1).
 *
 * <p>Leader entries with the same searchIndex (multi-fragment InsertTabletNode) are grouped into a
 * single IndexedConsensusRequest, matching PlanNodeIterator's behavior.
 *
 * <p>Follower entries are treated as standalone (each is a complete logical write).
 *
 * <p>The iterator skips non-searchable WAL entries (checkpoints, signals, etc.) and the
 * currently-writing WAL file (last file by versionId).
 */
public class SubscriptionWALIterator implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionWALIterator.class);

  /**
   * Offset of searchIndex in WAL entry body: WALEntryType(1B) + memTableId(8B) + PlanNodeType(2B)
   */
  private static final int SEARCH_INDEX_OFFSET =
      WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES;

  private final File logDirectory;
  private final long startSearchIndex;

  // File-level state
  private File[] walFiles;
  private int currentFileIndex = -1;
  private WALByteBufReader currentReader;

  // Multi-fragment accumulation buffer (for Leader entries with same searchIndex)
  private long pendingSearchIndex = Long.MIN_VALUE;
  private long pendingEpoch;
  private long pendingSyncIndex;
  private final List<IConsensusRequest> pendingRequests = new ArrayList<>();

  // Pre-fetched next result
  private IndexedConsensusRequest nextReady;

  // Position tracking: last returned entry's ordering key
  private long lastReturnedEpoch = -1;
  private long lastReturnedSyncIndex = -1;

  public SubscriptionWALIterator(final File logDirectory) {
    this(logDirectory, Long.MIN_VALUE);
  }

  public SubscriptionWALIterator(final File logDirectory, final long startSearchIndex) {
    this.logDirectory = logDirectory;
    this.startSearchIndex = startSearchIndex;
    refreshFileList();
  }

  private void refreshFileList() {
    walFiles = WALFileUtils.listAllWALFiles(logDirectory);
    if (walFiles == null) {
      walFiles = new File[0];
    }
    WALFileUtils.ascSortByVersionId(walFiles);
  }

  /** Returns true if there are more entries to read. */
  public boolean hasNext() {
    if (nextReady != null) {
      return true;
    }
    try {
      nextReady = advance();
    } catch (final IOException e) {
      LOGGER.warn("SubscriptionWALIterator: error reading WAL", e);
      return false;
    }
    return nextReady != null;
  }

  /** Returns the next IndexedConsensusRequest with correct epoch and syncIndex. */
  public IndexedConsensusRequest next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final IndexedConsensusRequest result = nextReady;
    lastReturnedEpoch = result.getEpoch();
    lastReturnedSyncIndex = result.getSyncIndex();
    nextReady = null;
    return result;
  }

  /** Returns the epoch of the last returned entry. */
  public long getLastReturnedEpoch() {
    return lastReturnedEpoch;
  }

  /** Returns the syncIndex of the last returned entry. */
  public long getLastReturnedSyncIndex() {
    return lastReturnedSyncIndex;
  }

  /**
   * Refreshes the WAL file list and repositions to continue from the current file. Call this
   * periodically to pick up newly sealed WAL files.
   */
  public void refresh() {
    final long currentVersionId =
        (currentFileIndex >= 0 && currentFileIndex < walFiles.length)
            ? WALFileUtils.parseVersionId(walFiles[currentFileIndex].getName())
            : -1;

    refreshFileList();

    if (currentVersionId >= 0) {
      // Find the file with the same or next versionId
      currentFileIndex = -1;
      for (int i = 0; i < walFiles.length; i++) {
        if (WALFileUtils.parseVersionId(walFiles[i].getName()) >= currentVersionId) {
          currentFileIndex = i;
          break;
        }
      }
      if (currentFileIndex < 0) {
        currentFileIndex = walFiles.length;
      }
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrentReader();
    nextReady = null;
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
  }

  /**
   * Advances the iterator to produce the next IndexedConsensusRequest. Handles file transitions,
   * entry filtering, and multi-fragment grouping.
   */
  private IndexedConsensusRequest advance() throws IOException {
    while (true) {
      // Try reading from current reader
      if (currentReader != null && currentReader.hasNext()) {
        final ByteBuffer buffer = currentReader.next();
        final WALEntryType type = WALEntryType.valueOf(buffer.get());
        buffer.clear();

        // Skip non-searchable entries (checkpoints, signals, etc.)
        if (!type.needSearch()) {
          continue;
        }

        final long epoch = currentReader.getCurrentEntryEpoch();
        final long syncIndex = currentReader.getCurrentEntrySyncIndex();

        // Read searchIndex from entry body
        buffer.position(SEARCH_INDEX_OFFSET);
        final long bodySearchIndex = buffer.getLong();
        buffer.clear();

        if (bodySearchIndex >= 0) {
          // Leader entry — may need grouping with same-searchIndex fragments
          if (bodySearchIndex == pendingSearchIndex) {
            // Same logical write, accumulate fragment
            pendingRequests.add(new IoTConsensusRequest(buffer));
          } else {
            // Different searchIndex — flush pending group, start new one
            final IndexedConsensusRequest flushed = flushPending();
            startPending(bodySearchIndex, epoch, syncIndex, buffer);
            if (flushed != null && !shouldSkip(flushed)) {
              return flushed;
            }
          }
        } else {
          // Follower entry (searchIndex = -1): standalone, no grouping
          final IndexedConsensusRequest flushed = flushPending();
          final IndexedConsensusRequest standalone =
              new IndexedConsensusRequest(
                  bodySearchIndex,
                  syncIndex,
                  Collections.singletonList(new IoTConsensusRequest(buffer)));
          standalone.setEpoch(epoch);

          if (flushed != null && !shouldSkip(flushed)) {
            // Must return flushed first; cache standalone as nextReady
            if (!shouldSkip(standalone)) {
              nextReady = standalone;
            }
            return flushed;
          }
          if (!shouldSkip(standalone)) {
            return standalone;
          }
        }
      } else {
        // Current reader exhausted or not yet opened — try next file
        closeCurrentReader();
        currentFileIndex++;

        // Don't read the currently-writing file (last file by versionId)
        if (currentFileIndex >= walFiles.length - 1) {
          // End of sealed files; flush any remaining pending entries
          final IndexedConsensusRequest flushed = flushPending();
          // Reset to allow refresh() to pick up new files
          currentFileIndex = Math.max(0, walFiles.length - 1);
          if (flushed != null && shouldSkip(flushed)) {
            continue;
          }
          return flushed; // null if nothing pending
        }

        try {
          currentReader = new WALByteBufReader(walFiles[currentFileIndex]);
        } catch (final IOException e) {
          LOGGER.warn(
              "SubscriptionWALIterator: failed to open WAL file {}, skipping",
              walFiles[currentFileIndex].getName(),
              e);
          // currentReader remains null, loop will advance to next file
        }
      }
    }
  }

  private void startPending(
      final long searchIndex, final long epoch, final long syncIndex, final ByteBuffer buffer) {
    pendingSearchIndex = searchIndex;
    pendingEpoch = epoch;
    pendingSyncIndex = syncIndex;
    pendingRequests.clear();
    pendingRequests.add(new IoTConsensusRequest(buffer));
  }

  private IndexedConsensusRequest flushPending() {
    if (pendingRequests.isEmpty()) {
      return null;
    }
    final IndexedConsensusRequest result =
        new IndexedConsensusRequest(
            pendingSearchIndex, pendingSyncIndex, new ArrayList<>(pendingRequests));
    result.setEpoch(pendingEpoch);
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    return result;
  }

  private boolean shouldSkip(final IndexedConsensusRequest request) {
    return request.getSearchIndex() >= 0 && request.getSearchIndex() < startSearchIndex;
  }

  private void closeCurrentReader() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }
  }
}
