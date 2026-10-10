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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.BrokenWALFileException;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Writer-based WAL iterator for the new subscription progress model.
 *
 * <p>This iterator reads writer-local ordering metadata from WAL footer arrays instead of relying
 * on the entry body to carry complete subscription ordering information.
 */
public class ProgressWALIterator implements Closeable, Iterator<IndexedConsensusRequest> {

  @FunctionalInterface
  public interface WriterProgressCoverage {

    boolean isCovered(long physicalTime, int nodeId, long localSeq);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressWALIterator.class);
  private static final int MAX_SKIPPED_FILE_DETAILS = 8;

  private static final int SEARCH_INDEX_OFFSET =
      WALInfoEntry.FIXED_SERIALIZED_SIZE + PlanNodeType.BYTES;
  private static final long HEADER_ONLY_WAL_FILE_BYTES =
      Math.max(
          WALFileVersion.V2.getVersionBytes().length, WALFileVersion.V3.getVersionBytes().length);

  private final File logDirectory;
  private long minimumSearchIndex;
  private final WALNode liveWalNode;
  private File[] walFiles;
  private long[] walFileVersionIds;
  private int currentFileIndex = -1;
  private ProgressWALReader currentReader;
  private long currentReaderVersionId = -1L;
  private boolean currentReaderUsesLiveSnapshot = false;
  private int consumedEntryCountInCurrentFile = 0;
  private final Set<Long> skippedBrokenWalVersionIds = new HashSet<>();
  private final List<String> skippedBrokenWalFileDetails = new ArrayList<>();
  private long skippedBrokenWalEntryCount;
  private boolean retryCurrentReader;
  private long eofRetryVersionId = -1L;
  private int eofRetryEntryOffset = -1;
  private long eofRetryFileLength = -1L;
  private long eofRetryFileModified = -1L;
  private IOException lastError;
  private boolean incompleteScan = false;
  private String incompleteScanDetail;

  private long pendingSearchIndex = Long.MIN_VALUE;
  private long pendingLocalSeq = Long.MIN_VALUE;
  private long pendingPhysicalTime;
  private int pendingNodeId;
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
    this.minimumSearchIndex = startSearchIndex;
    this.liveWalNode = liveWalNode;
    refreshFileList();
  }

  private void refreshFileList() {
    final File[] discoveredWalFiles = WALFileUtils.listAllWALFiles(logDirectory);
    if (discoveredWalFiles == null) {
      walFiles = new File[0];
      walFileVersionIds = new long[0];
      return;
    }
    WALFileUtils.ascSortByVersionId(discoveredWalFiles);
    final File[] filteredWalFiles = new File[discoveredWalFiles.length];
    final long[] filteredWalFileVersionIds = new long[discoveredWalFiles.length];
    int filteredWalFileCount = 0;
    for (int i = 0; i < discoveredWalFiles.length; i++) {
      final File walFile = discoveredWalFiles[i];
      final long versionId = WALFileUtils.parseVersionId(walFile.getName());
      final boolean isLastWalFile = i == discoveredWalFiles.length - 1;
      if (!isLastWalFile && shouldSkipWalFile(walFile, versionId)) {
        continue;
      }
      filteredWalFiles[filteredWalFileCount] = walFile;
      filteredWalFileVersionIds[filteredWalFileCount] = versionId;
      filteredWalFileCount++;
    }
    walFiles = Arrays.copyOf(filteredWalFiles, filteredWalFileCount);
    walFileVersionIds = Arrays.copyOf(filteredWalFileVersionIds, filteredWalFileCount);
  }

  private boolean shouldSkipWalFile(final File walFile, final long versionId) {
    return skippedBrokenWalVersionIds.contains(versionId) || isHeaderOnlyWalFile(walFile);
  }

  static boolean isHeaderOnlyWalFile(final File walFile) {
    return walFile.length() <= HEADER_ONLY_WAL_FILE_BYTES;
  }

  public void refresh() {
    final boolean exhaustedKnownFiles = currentFileIndex >= walFiles.length;
    final long currentVersionId =
        (currentFileIndex >= 0 && currentFileIndex < walFiles.length)
            ? walFileVersionIds[currentFileIndex]
            : exhaustedKnownFiles && walFileVersionIds.length > 0
                ? walFileVersionIds[walFileVersionIds.length - 1]
                : -1;

    refreshFileList();

    if (currentVersionId >= 0) {
      final int refreshedIndex = Arrays.binarySearch(walFileVersionIds, currentVersionId);
      currentFileIndex = refreshedIndex >= 0 ? refreshedIndex : -refreshedIndex - 2;
    } else if (exhaustedKnownFiles) {
      currentFileIndex = -1;
    }
  }

  public boolean hasNext() {
    while (true) {
      if (nextReady != null) {
        if (!shouldSkip(nextReady)) {
          return true;
        }
        nextReady = null;
      }
      try {
        nextReady = advance();
        if (nextReady != null) {
          lastError = null;
        }
      } catch (IOException e) {
        lastError = e;
        LOGGER.warn(
            DataNodePipeMessages.PIPE_LOG_PROGRESSWALITERATOR_ERROR_READING_WAL_2DB46D41, e);
      }
      if (nextReady == null) {
        return false;
      }
    }
  }

  /**
   * Advances the local search-index lower bound without rebuilding the iterator. Whole WAL files
   * are skipped only when every writer progress tuple in them is already covered by queue state.
   */
  public void advanceTo(
      final long targetSearchIndex, final WriterProgressCoverage writerProgressCoverage) {
    if (targetSearchIndex <= minimumSearchIndex) {
      return;
    }
    minimumSearchIndex = targetSearchIndex;

    if (writerProgressCoverage == null || !canDiscardBufferedRequests(writerProgressCoverage)) {
      return;
    }

    refreshForNewerLiveWalFile();
    int targetFileIndex = locateTargetFile(targetSearchIndex);
    if (targetFileIndex < 0
        && liveWalNode != null
        && targetSearchIndex <= liveWalNode.getCurrentSearchIndex()) {
      refresh();
      targetFileIndex = locateTargetFile(targetSearchIndex);
    }
    if (targetFileIndex < 0 || targetFileIndex <= currentFileIndex) {
      return;
    }

    final int firstFileToSkip = Math.max(0, currentFileIndex);
    try {
      for (int fileIndex = firstFileToSkip; fileIndex < targetFileIndex; fileIndex++) {
        if (!isWalFileCovered(fileIndex, writerProgressCoverage)) {
          return;
        }
      }

      closeCurrentReader();
      nextReady = null;
      pendingRequests.clear();
      pendingSearchIndex = Long.MIN_VALUE;
      pendingLocalSeq = Long.MIN_VALUE;
      currentFileIndex = targetFileIndex - 1;
      resetCurrentFileTracking();
    } catch (final IOException ignored) {
      // Fast-forward is opportunistic. Sequential replay remains the correctness fallback.
    }
  }

  private void refreshForNewerLiveWalFile() {
    if (walFileVersionIds.length == 0
        || (liveWalNode != null
            && walFileVersionIds[walFileVersionIds.length - 1]
                < liveWalNode.getCurrentWALFileVersion())) {
      refresh();
    }
  }

  private int locateTargetFile(final long targetSearchIndex) {
    if (walFiles.length == 0
        || (liveWalNode != null && targetSearchIndex > liveWalNode.getCurrentSearchIndex())) {
      return -1;
    }
    return WALFileUtils.binarySearchFileBySearchIndex(walFiles, targetSearchIndex);
  }

  private boolean canDiscardBufferedRequests(final WriterProgressCoverage writerProgressCoverage) {
    return (nextReady == null || isCovered(nextReady, writerProgressCoverage))
        && (pendingRequests.isEmpty()
            || isCovered(
                pendingSearchIndex,
                pendingPhysicalTime,
                pendingNodeId,
                pendingLocalSeq,
                writerProgressCoverage));
  }

  private boolean isCovered(
      final IndexedConsensusRequest request, final WriterProgressCoverage writerProgressCoverage) {
    return isCovered(
        request.getSearchIndex(),
        request.getPhysicalTime(),
        request.getNodeId(),
        request.getProgressLocalSeq(),
        writerProgressCoverage);
  }

  private boolean isCovered(
      final long searchIndex,
      final long physicalTime,
      final int nodeId,
      final long localSeq,
      final WriterProgressCoverage writerProgressCoverage) {
    if (searchIndex >= 0 && searchIndex < minimumSearchIndex) {
      return true;
    }
    return nodeId >= 0
        && physicalTime >= 0
        && localSeq >= 0
        && writerProgressCoverage.isCovered(physicalTime, nodeId, localSeq);
  }

  private boolean isWalFileCovered(
      final int fileIndex, final WriterProgressCoverage writerProgressCoverage) throws IOException {
    final File walFile = walFiles[fileIndex];
    if (WALFileVersion.getVersion(walFile) != WALFileVersion.V3) {
      return false;
    }

    final WALMetaData metadata;
    final long versionId = walFileVersionIds[fileIndex];
    if (liveWalNode != null && versionId == liveWalNode.getCurrentWALFileVersion()) {
      metadata = liveWalNode.getCurrentWALMetaDataSnapshot();
    } else {
      try (final ProgressWALReader reader = new ProgressWALReader(walFile)) {
        metadata = reader.getMetaData();
      }
    }

    final List<Integer> bufferSizes = metadata.getBuffersSize();
    final List<Long> physicalTimes = metadata.getPhysicalTimes();
    final List<Short> nodeIds = metadata.getNodeIds();
    final List<Long> localSeqs = metadata.getLocalSeqs();
    if (physicalTimes.size() != bufferSizes.size()
        || nodeIds.size() != bufferSizes.size()
        || localSeqs.size() != bufferSizes.size()) {
      return false;
    }

    for (int entryIndex = 0; entryIndex < bufferSizes.size(); entryIndex++) {
      final long physicalTime = physicalTimes.get(entryIndex);
      final int nodeId = nodeIds.get(entryIndex);
      final long localSeq = localSeqs.get(entryIndex);
      if (nodeId < 0
          || physicalTime < 0
          || localSeq < 0
          || !writerProgressCoverage.isCovered(physicalTime, nodeId, localSeq)) {
        return false;
      }
    }
    return true;
  }

  public IndexedConsensusRequest next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final IndexedConsensusRequest result = nextReady;
    nextReady = null;
    return result;
  }

  public boolean hasReadError() {
    return lastError != null;
  }

  public IOException getLastError() {
    return lastError;
  }

  public boolean hasSkippedBrokenWalFiles() {
    return !skippedBrokenWalVersionIds.isEmpty();
  }

  int getSkippedBrokenWalFileCount() {
    return skippedBrokenWalVersionIds.size();
  }

  long getSkippedBrokenWalEntryCount() {
    return skippedBrokenWalEntryCount;
  }

  boolean isWaitingForReadableWal() {
    return retryCurrentReader;
  }

  String getSkippedBrokenWalFileDetails(final int firstFileIndex) {
    final int firstAvailableIndex =
        getSkippedBrokenWalFileCount() - skippedBrokenWalFileDetails.size();
    final int start = Math.max(0, firstFileIndex - firstAvailableIndex);
    return (firstFileIndex < firstAvailableIndex ? "...; " : "")
        + String.join(
            "; ", skippedBrokenWalFileDetails.subList(start, skippedBrokenWalFileDetails.size()));
  }

  public boolean hasIncompleteScan() {
    return incompleteScan || hasReadError() || hasSkippedBrokenWalFiles();
  }

  public String getIncompleteScanDetail() {
    if (incompleteScanDetail != null) {
      return incompleteScanDetail;
    }
    if (lastError != null) {
      return lastError.getMessage();
    }
    if (!skippedBrokenWalVersionIds.isEmpty()) {
      return "encountered broken retained WAL files during replay scan";
    }
    return "replay scan did not complete";
  }

  @Override
  public void close() throws IOException {
    closeCurrentReader();
    nextReady = null;
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    pendingLocalSeq = Long.MIN_VALUE;
    lastError = null;
    incompleteScan = false;
    incompleteScanDetail = null;
    resetCurrentFileTracking();
  }

  private IndexedConsensusRequest advance() throws IOException {
    while (true) {
      if (retryCurrentReader) {
        refresh();
        final int retryFileIndex = findFileIndexByVersion(currentReaderVersionId);
        if (retryFileIndex < 0) {
          // Do not let a temporarily invisible file make replay silently move to its successor.
          return null;
        }
        if (!openReaderAtIndex(retryFileIndex, consumedEntryCountInCurrentFile)) {
          if (retryCurrentReader) {
            return null;
          }
          continue;
        }
        retryCurrentReader = false;
      }
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
          final long searchIndex =
              getSearchIndexOrMetadataFallback(buffer, currentReader.getCurrentEntrySearchIndex());
          buffer.clear();

          if (isSamePendingRequest(physicalTime, nodeId, localSeq)) {
            if (pendingSearchIndex < 0 && searchIndex >= 0) {
              pendingSearchIndex = searchIndex;
            }
            pendingRequests.add(new IoTConsensusRequest(buffer));
            continue;
          }

          final IndexedConsensusRequest flushed = flushPending();
          startPending(searchIndex, physicalTime, nodeId, localSeq, buffer);
          if (flushed != null && !shouldSkip(flushed)) {
            return flushed;
          }
          continue;
        } catch (final EOFException eofException) {
          if (!isConfirmedRetainedWalTruncation()) {
            deferCurrentReader(eofException);
            return null;
          }
          final IndexedConsensusRequest flushed = skipUnreadableCurrentWalFile(eofException);
          if (flushed != null && !shouldSkip(flushed)) {
            return flushed;
          }
          continue;
        } catch (final IOException readException) {
          if (!(readException instanceof BrokenWALFileException)
              && !isCurrentEntryAboveSizeLimit()) {
            deferCurrentReader(readException);
            return null;
          }
          final IndexedConsensusRequest flushed = skipUnreadableCurrentWalFile(readException);
          if (flushed != null && !shouldSkip(flushed)) {
            return flushed;
          }
          continue;
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
        if (retryCurrentReader) {
          return null;
        }
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
      if (retryCurrentReader) {
        return false;
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
      return openReaderAtIndex(fileIndex, consumedEntryCountInCurrentFile, true, snapshot);
    }

    final int previousFileIndex = findFileIndexByVersion(currentReaderVersionId);
    if (previousFileIndex < 0) {
      return openFirstReaderAfterVersion(currentReaderVersionId);
    }
    final long versionToReopen = currentReaderVersionId;
    if (openReaderAtIndex(previousFileIndex, consumedEntryCountInCurrentFile)) {
      return true;
    }
    if (retryCurrentReader) {
      return false;
    }
    return openFirstReaderAfterVersion(versionToReopen);
  }

  private boolean openReaderAtIndex(final int fileIndex, final int skipEntries) throws IOException {
    return openReaderAtIndex(fileIndex, skipEntries, true, null);
  }

  private boolean openReaderAtIndex(
      final int fileIndex, final int skipEntries, final boolean allowNearLiveRetry)
      throws IOException {
    return openReaderAtIndex(fileIndex, skipEntries, allowNearLiveRetry, null);
  }

  private boolean openReaderAtIndex(
      final int fileIndex,
      final int skipEntries,
      final boolean allowNearLiveRetry,
      final WALMetaData liveMetaDataSnapshot)
      throws IOException {
    final File walFile = walFiles[fileIndex];
    final long versionId = walFileVersionIds[fileIndex];
    currentFileIndex = fileIndex;
    final boolean useLiveSnapshot =
        liveWalNode != null && versionId == liveWalNode.getCurrentWALFileVersion();

    ProgressWALReader reader = null;
    try {
      reader =
          useLiveSnapshot
              ? new ProgressWALReader(
                  walFile,
                  liveMetaDataSnapshot != null
                      ? liveMetaDataSnapshot
                      : liveWalNode.getCurrentWALMetaDataSnapshot())
              : new ProgressWALReader(walFile);
      if (!skipEntries(reader, skipEntries)) {
        reader.close();
        markIncompleteScan(
            String.format(
                DataNodePipeMessages
                    .MESSAGE_FAILED_TO_REOPEN_WAL_FILE_ARG_AT_ENTRY_OFFSET_ARG_ITERATOR_COULD_NOT_SKIP_TO_THE_REQUESTED_POSITION_332B3AD9,
                walFile.getName(),
                skipEntries),
            null);
        currentReaderVersionId = versionId;
        consumedEntryCountInCurrentFile = skipEntries;
        retryCurrentReader = true;
        return false;
      }
      currentReader = reader;
      currentFileIndex = fileIndex;
      currentReaderVersionId = versionId;
      currentReaderUsesLiveSnapshot = useLiveSnapshot;
      consumedEntryCountInCurrentFile = skipEntries;
      retryCurrentReader = false;
      return true;
    } catch (final IOException e) {
      if (reader != null) {
        try {
          reader.close();
        } catch (final IOException closeException) {
          e.addSuppressed(closeException);
        }
      }
      if (isNearLiveWalVersion(versionId)
          || !(e instanceof BrokenWALFileException)
          || (liveWalNode == null && fileIndex == walFiles.length - 1)) {
        LOGGER.debug(
            DataNodePipeMessages
                .PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_NEAR_LIVE_WAL_FILE_RETRYING_5AEB94AC,
            walFile.getName(),
            e);
        if (allowNearLiveRetry) {
          refresh();
          final int refreshedIndex = findFileIndexByVersion(versionId);
          if (refreshedIndex >= 0) {
            if (openReaderAtIndex(refreshedIndex, skipEntries, false)) {
              return true;
            }
          }
        }
        markIncompleteScan(
            String.format(
                DataNodePipeMessages
                    .MESSAGE_WAL_FILE_ARG_VERSIONID_ARG_ENTRYOFFSET_ARG_IS_TEMPORARILY_UNREADABLE_REPLAY_WILL_RETRY_WITHOUT_SKIPPING_ARG_EA11FBDD,
                walFile.getAbsolutePath(),
                versionId,
                skipEntries,
                summarizeException(e)),
            e);
        currentReaderVersionId = versionId;
        consumedEntryCountInCurrentFile = skipEntries;
        retryCurrentReader = true;
        return false;
      }
      recordSkippedBrokenWalFile(versionId, walFile, e, null, skipEntries);
      pendingRequests.clear();
      pendingSearchIndex = Long.MIN_VALUE;
      pendingLocalSeq = Long.MIN_VALUE;
      resetCurrentFileTracking();
      return false;
    }
  }

  private void recordSkippedBrokenWalFile(
      final long versionId,
      final File walFile,
      final IOException error,
      final WALMetaData metadata,
      final int firstSkippedEntryOffset) {
    if (!skippedBrokenWalVersionIds.add(versionId)) {
      return;
    }

    final int entryCount = metadata == null ? -1 : metadata.getBuffersSize().size();
    final long skippedEntries =
        entryCount < 0 ? -1L : Math.max(0L, (long) entryCount - firstSkippedEntryOffset);
    if (skippedEntries > 0L) {
      skippedBrokenWalEntryCount += skippedEntries;
    }
    // Filename boundaries are file-level search-index bounds, not an entry count. Footer entry
    // ordinals also include request fragments and entries from other writers.
    final long firstSearchIndex = WALFileUtils.parseStartSearchIndex(walFile.getName());
    final int fileIndex = findFileIndexByVersion(versionId);
    final long lastSearchIndex =
        fileIndex >= 0 && fileIndex + 1 < walFiles.length
            ? WALFileUtils.parseStartSearchIndex(walFiles[fileIndex + 1].getName())
            : -1L;
    final String detail =
        String.format(
            DataNodePipeMessages
                .MESSAGE_FILE_ARG_VERSIONID_ARG_FILESEARCHINDEXRANGE_ARG_ARG_ENTRYRANGE_ARG_ARG_SKIPPEDENTRIES_ARG_ERROR_ARG_0D6D77B0,
            walFile.getAbsolutePath(),
            versionId,
            formatKnownValue(firstSearchIndex),
            formatKnownValue(lastSearchIndex),
            firstSkippedEntryOffset,
            formatKnownValue(entryCount),
            formatKnownValue(skippedEntries),
            summarizeException(error));
    if (skippedBrokenWalFileDetails.size() == MAX_SKIPPED_FILE_DETAILS) {
      skippedBrokenWalFileDetails.remove(0);
    }
    skippedBrokenWalFileDetails.add(detail);
    LOGGER.warn(
        DataNodePipeMessages
            .LOG_PROGRESSWALITERATOR_SKIPPED_UNREADABLE_RETAINED_WAL_FILE_ARG_HISTORICAL_SUBSCRIPTION_DATA_MAY_BE_LOST_AE0DBAB1,
        detail,
        error);
  }

  private static String formatKnownValue(final long value) {
    return value < 0 ? DataNodePipeMessages.MESSAGE_UNKNOWN_AD921D60 : String.valueOf(value);
  }

  private boolean isConfirmedRetainedWalTruncation() {
    final File walFile = walFiles[currentFileIndex];
    final boolean sealed =
        liveWalNode != null
            ? !isNearLiveWalVersion(currentReaderVersionId)
            : currentFileIndex + 1 < walFiles.length;
    final boolean confirmed =
        !currentReaderUsesLiveSnapshot
            && sealed
            && eofRetryVersionId == currentReaderVersionId
            && eofRetryEntryOffset == consumedEntryCountInCurrentFile
            && eofRetryFileLength == walFile.length()
            && eofRetryFileModified == walFile.lastModified();
    eofRetryVersionId = currentReaderVersionId;
    eofRetryEntryOffset = consumedEntryCountInCurrentFile;
    eofRetryFileLength = walFile.length();
    eofRetryFileModified = walFile.lastModified();
    return confirmed;
  }

  private void deferCurrentReader(final IOException error) throws IOException {
    // Keep the incomplete request and the offset of the first unread entry. Flushing or reopening
    // at offset zero here would either deliver a partial request or duplicate its fragments.
    markIncompleteScan(
        String.format(
            DataNodePipeMessages
                .MESSAGE_WAL_FILE_ARG_VERSIONID_ARG_ENTRYOFFSET_ARG_IS_TEMPORARILY_UNREADABLE_REPLAY_WILL_RETRY_WITHOUT_SKIPPING_ARG_EA11FBDD,
            walFiles[currentFileIndex].getAbsolutePath(),
            currentReaderVersionId,
            consumedEntryCountInCurrentFile,
            summarizeException(error)),
        error);
    retryCurrentReader = true;
    closeCurrentReader();
  }

  private boolean isCurrentEntryAboveSizeLimit() {
    final int entryIndex = currentReader.getCurrentEntryIndex();
    final List<Integer> sizes = currentReader.getMetaData().getBuffersSize();
    // An entry above the supported limit is deterministically unreadable, even after reopening.
    // Preserve the existing large-entry escape path, but report the resulting data gap explicitly.
    return entryIndex >= 0
        && entryIndex < sizes.size()
        && sizes.get(entryIndex)
            > IoTDBDescriptor.getInstance().getConfig().getWalEntrySizeLimitInByte();
  }

  private IndexedConsensusRequest skipUnreadableCurrentWalFile(final IOException error) {
    final int failedFileIndex = currentFileIndex;
    final File walFile =
        failedFileIndex >= 0 && failedFileIndex < walFiles.length
            ? walFiles[failedFileIndex]
            : null;
    final long versionId =
        failedFileIndex >= 0 && failedFileIndex < walFileVersionIds.length
            ? walFileVersionIds[failedFileIndex]
            : currentReaderVersionId;
    final WALMetaData failedFileMetadata =
        currentReader == null ? null : currentReader.getMetaData();
    final int firstSkippedEntryOffset =
        Math.max(
            0,
            consumedEntryCountInCurrentFile
                - (isFailedEntryPartOfPendingRequest() ? pendingRequests.size() : 0));
    final IndexedConsensusRequest flushed =
        isFailedEntryPartOfPendingRequest() ? null : flushPending();

    try {
      closeCurrentReader();
    } catch (final IOException closeException) {
      error.addSuppressed(closeException);
    }
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    pendingLocalSeq = Long.MIN_VALUE;
    resetCurrentFileTracking();

    if (walFile == null || versionId < 0) {
      markIncompleteScan(
          DataNodePipeMessages
              .PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_IDENTIFY_UNREADABLE_WAL_FILE_7BA9F422,
          error);
      return flushed;
    }
    recordSkippedBrokenWalFile(
        versionId, walFile, error, failedFileMetadata, firstSkippedEntryOffset);
    return flushed;
  }

  private boolean isFailedEntryPartOfPendingRequest() {
    return currentReader != null
        && !pendingRequests.isEmpty()
        && isSamePendingRequest(
            currentReader.getCurrentEntryPhysicalTime(),
            currentReader.getCurrentEntryNodeId(),
            currentReader.getCurrentEntryLocalSeq());
  }

  private static String summarizeException(final IOException error) {
    return error.getMessage() == null
        ? error.getClass().getSimpleName()
        : error.getClass().getSimpleName() + ": " + error.getMessage();
  }

  private boolean skipEntries(final ProgressWALReader reader, final int skipEntries)
      throws IOException {
    return reader.skipToEntryIndex(skipEntries);
  }

  private int findFileIndexByVersion(final long versionId) {
    final int fileIndex = Arrays.binarySearch(walFileVersionIds, versionId);
    return fileIndex >= 0 ? fileIndex : -1;
  }

  private boolean openFirstReaderAfterVersion(final long versionId) throws IOException {
    final int matchedFileIndex = Arrays.binarySearch(walFileVersionIds, versionId);
    final int firstFileIndexAfterVersion =
        matchedFileIndex >= 0 ? matchedFileIndex + 1 : -matchedFileIndex - 1;
    for (int i = firstFileIndexAfterVersion; i < walFiles.length; i++) {
      if (openReaderAtIndex(i, 0)) {
        return true;
      }
      if (retryCurrentReader) {
        return false;
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
      final long physicalTime, final int nodeId, final long localSeq) {
    return !pendingRequests.isEmpty()
        && pendingPhysicalTime == physicalTime
        && pendingNodeId == nodeId
        && pendingLocalSeq == localSeq;
  }

  private long getSearchIndexOrMetadataFallback(
      final ByteBuffer buffer, final long metadataSearchIndex) {
    if (buffer.limit() < SEARCH_INDEX_OFFSET + Long.BYTES) {
      return metadataSearchIndex;
    }
    buffer.position(SEARCH_INDEX_OFFSET);
    final long bodySearchIndex = SearchNode.extractSearchIndex(buffer.getLong());
    // A replicated request has no local search index. The metadata fallback is based on the
    // WAL entry offset, which also counts fragments and cannot assign a local index to it.
    return bodySearchIndex;
  }

  private void startPending(
      final long searchIndex,
      final long physicalTime,
      final int nodeId,
      final long localSeq,
      final ByteBuffer buffer) {
    pendingSearchIndex = searchIndex;
    pendingLocalSeq = localSeq;
    pendingPhysicalTime = physicalTime;
    pendingNodeId = nodeId;
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
    result.setPhysicalTime(pendingPhysicalTime).setNodeId(pendingNodeId);
    pendingRequests.clear();
    pendingSearchIndex = Long.MIN_VALUE;
    pendingLocalSeq = Long.MIN_VALUE;
    return result;
  }

  private boolean shouldSkip(final IndexedConsensusRequest request) {
    return request.getSearchIndex() >= 0 && request.getSearchIndex() < minimumSearchIndex;
  }

  private void closeCurrentReader() throws IOException {
    if (currentReader != null) {
      final ProgressWALReader reader = currentReader;
      currentReader = null;
      reader.close();
    }
  }

  private void resetCurrentFileTracking() {
    currentReaderVersionId = -1L;
    currentReaderUsesLiveSnapshot = false;
    consumedEntryCountInCurrentFile = 0;
    retryCurrentReader = false;
    eofRetryVersionId = -1L;
    eofRetryEntryOffset = -1;
  }

  private void markIncompleteScan(final String detail, final IOException cause) {
    incompleteScan = true;
    if (incompleteScanDetail == null) {
      incompleteScanDetail = detail;
    }
    if (lastError == null && cause != null) {
      lastError = cause;
    }
  }
}
