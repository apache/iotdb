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
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ProgressWALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Writer-based WAL iterator for the new subscription progress model.
 *
 * <p>This iterator reads writer-local ordering metadata from WAL footer arrays instead of relying
 * on the entry body to carry complete subscription ordering information.
 */
public class ProgressWALIterator implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressWALIterator.class);

  private static final int SEARCH_INDEX_OFFSET =
      WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES;
  private static final long HEADER_ONLY_WAL_FILE_BYTES =
      Math.max(
          WALFileVersion.V2.getVersionBytes().length, WALFileVersion.V3.getVersionBytes().length);

  private final File logDirectory;
  private final long startSearchIndex;
  private final WALNode liveWalNode;
  private File[] walFiles;
  private int currentFileIndex = -1;
  private ProgressWALReader currentReader;
  private long currentReaderVersionId = -1L;
  private boolean currentReaderUsesLiveSnapshot = false;
  private int consumedEntryCountInCurrentFile = 0;
  private final Set<Long> skippedBrokenWalVersionIds = new HashSet<>();

  private long pendingSearchIndex = Long.MIN_VALUE;
  private long pendingLocalSeq = Long.MIN_VALUE;
  private long pendingPhysicalTime;
  private int pendingNodeId;
  private long pendingWriterEpoch;
  private final List<IConsensusRequest> pendingRequests = new ArrayList<>();

  private IndexedConsensusRequest nextReady;

  public ProgressWALIterator(final File logDirectory) {
    this(logDirectory, Long.MIN_VALUE);
  }

  public ProgressWALIterator(final File logDirectory, final long startSearchIndex) {
    this(logDirectory, startSearchIndex, null);
  }

  public ProgressWALIterator(final WALNode liveWalNode) {
    this(liveWalNode, Long.MIN_VALUE);
  }

  public ProgressWALIterator(final WALNode liveWalNode, final long startSearchIndex) {
    this(liveWalNode.getLogDirectory(), startSearchIndex, liveWalNode);
  }

  private ProgressWALIterator(
      final File logDirectory, final long startSearchIndex, final WALNode liveWalNode) {
    this.logDirectory = logDirectory;
    this.startSearchIndex = startSearchIndex;
    this.liveWalNode = liveWalNode;
    refreshFileList();
  }

  private void refreshFileList() {
    final File[] discoveredWalFiles = WALFileUtils.listAllWALFiles(logDirectory);
    if (discoveredWalFiles == null) {
      walFiles = new File[0];
      return;
    }
    WALFileUtils.ascSortByVersionId(discoveredWalFiles);
    final List<File> filteredWalFiles = new ArrayList<>(discoveredWalFiles.length);
    for (int i = 0; i < discoveredWalFiles.length; i++) {
      final File walFile = discoveredWalFiles[i];
      final boolean isLastWalFile = i == discoveredWalFiles.length - 1;
      if (!isLastWalFile && shouldSkipWalFile(walFile)) {
        continue;
      }
      filteredWalFiles.add(walFile);
    }
    walFiles = filteredWalFiles.toArray(new File[0]);
  }

  private boolean shouldSkipWalFile(final File walFile) {
    final long versionId = WALFileUtils.parseVersionId(walFile.getName());
    return skippedBrokenWalVersionIds.contains(versionId) || isHeaderOnlyWalFile(walFile);
  }

  private boolean isHeaderOnlyWalFile(final File walFile) {
    return walFile.length() <= HEADER_ONLY_WAL_FILE_BYTES;
  }

  public void refresh() {
    final long currentVersionId =
        (currentFileIndex >= 0 && currentFileIndex < walFiles.length)
            ? WALFileUtils.parseVersionId(walFiles[currentFileIndex].getName())
            : -1;

    refreshFileList();

    if (currentVersionId >= 0) {
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

  public boolean hasNext() {
    if (nextReady != null) {
      return true;
    }
    try {
      nextReady = advance();
    } catch (IOException e) {
      LOGGER.warn("ProgressWALIterator: error reading WAL", e);
      return false;
    }
    return nextReady != null;
  }

  public IndexedConsensusRequest next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final IndexedConsensusRequest result = nextReady;
    nextReady = null;
    return result;
  }

  @Override
  public void close() throws IOException {
    closeCurrentReader();
    nextReady = null;
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    pendingLocalSeq = Long.MIN_VALUE;
    resetCurrentFileTracking();
  }

  private IndexedConsensusRequest advance() throws IOException {
    while (true) {
      if (currentReader != null && currentReader.hasNext()) {
        try {
          final ByteBuffer buffer = currentReader.next();
          consumedEntryCountInCurrentFile = currentReader.getCurrentEntryIndex() + 1;
          final WALEntryType type = WALEntryType.valueOf(buffer.get());
          buffer.clear();
          if (!type.needSearch()) {
            continue;
          }

          final long localSeq = currentReader.getCurrentEntryLocalSeq();
          final long physicalTime = currentReader.getCurrentEntryPhysicalTime();
          final int nodeId = currentReader.getCurrentEntryNodeId();
          final long writerEpoch = currentReader.getCurrentEntryWriterEpoch();

          buffer.position(SEARCH_INDEX_OFFSET);
          final long bodySearchIndex = buffer.getLong();
          buffer.clear();

          if (isSamePendingRequest(localSeq, nodeId, writerEpoch)) {
            if (pendingSearchIndex < 0 && bodySearchIndex >= 0) {
              pendingSearchIndex = bodySearchIndex;
            }
            pendingRequests.add(new IoTConsensusRequest(buffer));
            continue;
          }

          final IndexedConsensusRequest flushed = flushPending();
          startPending(bodySearchIndex, localSeq, physicalTime, nodeId, writerEpoch, buffer);
          if (flushed != null && !shouldSkip(flushed)) {
            return flushed;
          }
          continue;
        } catch (final EOFException eofException) {
          if (!currentReaderUsesLiveSnapshot) {
            throw eofException;
          }
          // Live snapshot metadata may get ahead of the bytes currently visible in the file. Treat
          // EOF as "this snapshot is exhausted for now" instead of terminating the iterator.
          final IndexedConsensusRequest flushed = flushPending();
          if (flushed != null && !shouldSkip(flushed)) {
            closeCurrentReader();
            return flushed;
          }
          if (reopenLiveSnapshotReader()) {
            continue;
          }
          return null;
        }
      }

      if (currentReaderUsesLiveSnapshot) {
        final IndexedConsensusRequest flushed = flushPending();
        if (flushed != null && !shouldSkip(flushed)) {
          return flushed;
        }
        if (reopenLiveSnapshotReader()) {
          continue;
        }
        return null;
      }

      if (currentReader != null) {
        closeCurrentReader();
        final IndexedConsensusRequest flushed = flushPending();
        resetCurrentFileTracking();
        if (flushed != null && !shouldSkip(flushed)) {
          return flushed;
        }
        continue;
      }

      if (!openNextReader()) {
        final IndexedConsensusRequest flushed = flushPending();
        if (flushed != null && !shouldSkip(flushed)) {
          return flushed;
        }
        return null;
      }
    }
  }

  private boolean openNextReader() throws IOException {
    while (++currentFileIndex < walFiles.length) {
      if (openReaderAtIndex(currentFileIndex, 0)) {
        return true;
      }
    }
    return false;
  }

  private boolean reopenLiveSnapshotReader() throws IOException {
    if (liveWalNode == null || currentReaderVersionId < 0) {
      return false;
    }

    closeCurrentReader();
    refresh();

    final long currentLiveVersionId = liveWalNode.getCurrentWALFileVersion();
    if (currentLiveVersionId == currentReaderVersionId) {
      final WALMetaData snapshot = liveWalNode.getCurrentWALMetaDataSnapshot();
      if (snapshot.getBuffersSize().size() <= consumedEntryCountInCurrentFile) {
        return false;
      }
      final int fileIndex = findFileIndexByVersion(currentReaderVersionId);
      if (fileIndex < 0) {
        return false;
      }
      return openReaderAtIndex(fileIndex, consumedEntryCountInCurrentFile);
    }

    final int previousFileIndex = findFileIndexByVersion(currentReaderVersionId);
    if (previousFileIndex < 0) {
      return openFirstReaderAfterVersion(currentReaderVersionId);
    }
    if (openReaderAtIndex(previousFileIndex, consumedEntryCountInCurrentFile)) {
      return true;
    }
    return openFirstReaderAfterVersion(currentReaderVersionId);
  }

  private boolean openReaderAtIndex(final int fileIndex, final int skipEntries) throws IOException {
    return openReaderAtIndex(fileIndex, skipEntries, true);
  }

  private boolean openReaderAtIndex(
      final int fileIndex, final int skipEntries, final boolean allowNearLiveRetry)
      throws IOException {
    final File walFile = walFiles[fileIndex];
    final long versionId = WALFileUtils.parseVersionId(walFile.getName());
    final boolean useLiveSnapshot =
        liveWalNode != null && versionId == liveWalNode.getCurrentWALFileVersion();

    try {
      final ProgressWALReader reader =
          useLiveSnapshot
              ? new ProgressWALReader(walFile, liveWalNode.getCurrentWALMetaDataSnapshot())
              : new ProgressWALReader(walFile);
      if (!skipEntries(reader, skipEntries)) {
        reader.close();
        currentReader = null;
        currentReaderVersionId = versionId;
        currentReaderUsesLiveSnapshot = useLiveSnapshot;
        consumedEntryCountInCurrentFile = skipEntries;
        return useLiveSnapshot;
      }
      currentReader = reader;
      currentFileIndex = fileIndex;
      currentReaderVersionId = versionId;
      currentReaderUsesLiveSnapshot = useLiveSnapshot;
      consumedEntryCountInCurrentFile = skipEntries;
      return true;
    } catch (final IOException e) {
      if (isNearLiveWalVersion(versionId)) {
        LOGGER.debug(
            "ProgressWALIterator: failed to open near-live WAL file {}, retrying without blacklisting",
            walFile.getName(),
            e);
        if (allowNearLiveRetry) {
          refresh();
          final int refreshedIndex = findFileIndexByVersion(versionId);
          if (refreshedIndex >= 0) {
            return openReaderAtIndex(refreshedIndex, skipEntries, false);
          }
        }
        return false;
      }
      skippedBrokenWalVersionIds.add(versionId);
      LOGGER.warn(
          "ProgressWALIterator: failed to open WAL file {}, skipping", walFile.getName(), e);
      return false;
    }
  }

  private boolean skipEntries(final ProgressWALReader reader, final int skipEntries)
      throws IOException {
    int skipped = 0;
    while (skipped < skipEntries) {
      if (!reader.hasNext()) {
        return false;
      }
      reader.next();
      skipped++;
    }
    return true;
  }

  private int findFileIndexByVersion(final long versionId) {
    for (int i = 0; i < walFiles.length; i++) {
      if (WALFileUtils.parseVersionId(walFiles[i].getName()) == versionId) {
        return i;
      }
    }
    return -1;
  }

  private boolean openFirstReaderAfterVersion(final long versionId) throws IOException {
    for (int i = 0; i < walFiles.length; i++) {
      if (WALFileUtils.parseVersionId(walFiles[i].getName()) > versionId
          && openReaderAtIndex(i, 0)) {
        return true;
      }
    }
    resetCurrentFileTracking();
    return false;
  }

  private boolean isNearLiveWalVersion(final long versionId) {
    if (liveWalNode == null) {
      return false;
    }
    return versionId >= Math.max(0L, liveWalNode.getCurrentWALFileVersion() - 1L);
  }

  private boolean isSamePendingRequest(
      final long localSeq, final int nodeId, final long writerEpoch) {
    return !pendingRequests.isEmpty()
        && pendingLocalSeq == localSeq
        && pendingNodeId == nodeId
        && pendingWriterEpoch == writerEpoch;
  }

  private void startPending(
      final long searchIndex,
      final long localSeq,
      final long physicalTime,
      final int nodeId,
      final long writerEpoch,
      final ByteBuffer buffer) {
    pendingSearchIndex = searchIndex;
    pendingLocalSeq = localSeq;
    pendingPhysicalTime = physicalTime;
    pendingNodeId = nodeId;
    pendingWriterEpoch = writerEpoch;
    pendingRequests.clear();
    pendingRequests.add(new IoTConsensusRequest(buffer));
  }

  private IndexedConsensusRequest flushPending() {
    if (pendingRequests.isEmpty()) {
      return null;
    }
    final IndexedConsensusRequest result =
        new IndexedConsensusRequest(
            pendingSearchIndex, pendingLocalSeq, new ArrayList<>(pendingRequests));
    result
        .setPhysicalTime(pendingPhysicalTime)
        .setNodeId(pendingNodeId)
        .setWriterEpoch(pendingWriterEpoch);
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    pendingLocalSeq = Long.MIN_VALUE;
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

  private void resetCurrentFileTracking() {
    currentReaderVersionId = -1L;
    currentReaderUsesLiveSnapshot = false;
    consumedEntryCountInCurrentFile = 0;
  }
}
