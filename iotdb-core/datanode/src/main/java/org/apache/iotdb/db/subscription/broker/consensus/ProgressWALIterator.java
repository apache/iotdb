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
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
  private File[] walFiles;
  private int currentFileIndex = -1;
  private ProgressWALReader currentReader;
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
    this.logDirectory = logDirectory;
    this.startSearchIndex = startSearchIndex;
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
  }

  private IndexedConsensusRequest advance() throws IOException {
    while (true) {
      if (currentReader != null && currentReader.hasNext()) {
        final ByteBuffer buffer = currentReader.next();
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
      } else {
        closeCurrentReader();
        currentFileIndex++;
        if (currentFileIndex >= walFiles.length - 1) {
          final IndexedConsensusRequest flushed = flushPending();
          currentFileIndex = Math.max(0, walFiles.length - 1);
          if (flushed != null && !shouldSkip(flushed)) {
            return flushed;
          }
          return null;
        }
        try {
          currentReader = new ProgressWALReader(walFiles[currentFileIndex]);
        } catch (final IOException e) {
          skippedBrokenWalVersionIds.add(
              WALFileUtils.parseVersionId(walFiles[currentFileIndex].getName()));
          LOGGER.warn(
              "ProgressWALIterator: failed to open WAL file {}, skipping",
              walFiles[currentFileIndex].getName(),
              e);
        }
      }
    }
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
            pendingSearchIndex,
            pendingLocalSeq,
            new ArrayList<>(pendingRequests));
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
}
