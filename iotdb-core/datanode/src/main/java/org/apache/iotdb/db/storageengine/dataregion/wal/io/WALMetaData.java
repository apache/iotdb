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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.BrokenWALFileException;
import org.apache.iotdb.db.utils.SerializedSize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Metadata exists at the end of each wal file, including each entry's size, search index of first
 * entry and the number of entries.
 *
 * <p>V3 extension adds per-entry epoch and syncIndex arrays, plus file-level timestamp range, to
 * support ordered consensus subscription.
 */
public class WALMetaData implements SerializedSize {

  private static final Logger logger = LoggerFactory.getLogger(WALMetaData.class);
  // search index 8 byte, wal entries' number 4 bytes
  private static final int FIXED_SERIALIZED_SIZE = Long.BYTES + Integer.BYTES;

  // search index of first entry
  private long firstSearchIndex;
  // each entry's size
  private final List<Integer> buffersSize;
  // memTable ids of this wal file
  private final Set<Long> memTablesId;
  private long truncateOffSet = 0;

  // V3 fields: per-entry routing epoch and sync index for ordered consensus subscription
  private final List<Long> epochs;
  private final List<Long> syncIndices;
  // V3 fields: per-logical-request search index and ordering keys
  private final List<Long> logicalSearchIndices;
  private final List<Long> logicalEpochs;
  private final List<Long> logicalSyncIndices;
  private long firstLogicalEpoch = 0L;
  private long firstLogicalSyncIndex = ConsensusReqReader.DEFAULT_SEARCH_INDEX;
  private long lastLogicalEpoch = 0L;
  private long lastLogicalSyncIndex = ConsensusReqReader.DEFAULT_SEARCH_INDEX;
  // V3 fields: file-level data timestamp range for timestamp-based seek
  private long minDataTs = Long.MAX_VALUE;
  private long maxDataTs = Long.MIN_VALUE;

  public WALMetaData() {
    this(ConsensusReqReader.DEFAULT_SEARCH_INDEX, new ArrayList<>(), new HashSet<>());
  }

  public WALMetaData(long firstSearchIndex, List<Integer> buffersSize, Set<Long> memTablesId) {
    this.firstSearchIndex = firstSearchIndex;
    this.buffersSize = buffersSize;
    this.memTablesId = memTablesId;
    this.epochs = new ArrayList<>();
    this.syncIndices = new ArrayList<>();
    this.logicalSearchIndices = new ArrayList<>();
    this.logicalEpochs = new ArrayList<>();
    this.logicalSyncIndices = new ArrayList<>();
  }

  /** V2-compatible add without epoch/syncIndex. */
  public void add(int size, long searchIndex, long memTableId) {
    add(size, searchIndex, memTableId, 0L, searchIndex);
  }

  /** V3 add with epoch and syncIndex for ordered consensus subscription. */
  public void add(int size, long searchIndex, long memTableId, long epoch, long syncIndex) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = searchIndex;
    }
    buffersSize.add(size);
    memTablesId.add(memTableId);
    epochs.add(epoch);
    syncIndices.add(syncIndex);
    if (searchIndex != ConsensusReqReader.DEFAULT_SEARCH_INDEX
        && syncIndex != ConsensusReqReader.DEFAULT_SEARCH_INDEX
        && (logicalSearchIndices.isEmpty()
            || logicalSearchIndices.get(logicalSearchIndices.size() - 1) != searchIndex)) {
      logicalSearchIndices.add(searchIndex);
      logicalEpochs.add(epoch);
      logicalSyncIndices.add(syncIndex);
      if (logicalSearchIndices.size() == 1) {
        firstLogicalEpoch = epoch;
        firstLogicalSyncIndex = syncIndex;
      }
      lastLogicalEpoch = epoch;
      lastLogicalSyncIndex = syncIndex;
    }
  }

  /** Update file-level timestamp range with a data point's timestamp. */
  public void updateTimestampRange(long dataTs) {
    if (dataTs < minDataTs) {
      minDataTs = dataTs;
    }
    if (dataTs > maxDataTs) {
      maxDataTs = dataTs;
    }
  }

  public void addAll(WALMetaData metaData) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = metaData.getFirstSearchIndex();
    }
    buffersSize.addAll(metaData.getBuffersSize());
    memTablesId.addAll(metaData.getMemTablesId());
    epochs.addAll(metaData.getEpochs());
    syncIndices.addAll(metaData.getSyncIndices());
    if (!metaData.logicalSearchIndices.isEmpty()) {
      if (logicalSearchIndices.isEmpty()) {
        firstLogicalEpoch = metaData.firstLogicalEpoch;
        firstLogicalSyncIndex = metaData.firstLogicalSyncIndex;
      }
      logicalSearchIndices.addAll(metaData.logicalSearchIndices);
      logicalEpochs.addAll(metaData.logicalEpochs);
      logicalSyncIndices.addAll(metaData.logicalSyncIndices);
      lastLogicalEpoch = metaData.lastLogicalEpoch;
      lastLogicalSyncIndex = metaData.lastLogicalSyncIndex;
    }
    if (metaData.minDataTs < this.minDataTs) {
      this.minDataTs = metaData.minDataTs;
    }
    if (metaData.maxDataTs > this.maxDataTs) {
      this.maxDataTs = metaData.maxDataTs;
    }
  }

  @Override
  public int serializedSize() {
    return serializedSize(WALFileVersion.V2);
  }

  public int serializedSize(WALFileVersion version) {
    int size =
        FIXED_SERIALIZED_SIZE
            + buffersSize.size() * Integer.BYTES
            + (memTablesId.isEmpty() ? 0 : Integer.BYTES + memTablesId.size() * Long.BYTES);
    if (version == WALFileVersion.V3) {
      // epochs(long[]) + syncIndices(long[]) + minDataTs(long) + maxDataTs(long)
      size += buffersSize.size() * Long.BYTES * 2 + Long.BYTES * 2;
      // first/last logical key + logical entry count + logical search/sync/epoch arrays
      size += Long.BYTES * 4 + Integer.BYTES + logicalSearchIndices.size() * Long.BYTES * 3;
    }
    return size;
  }

  public void serialize(ByteBuffer buffer) {
    serialize(buffer, WALFileVersion.V2);
  }

  public void serialize(ByteBuffer buffer, WALFileVersion version) {
    buffer.putLong(firstSearchIndex);
    buffer.putInt(buffersSize.size());
    for (int size : buffersSize) {
      buffer.putInt(size);
    }
    if (!memTablesId.isEmpty()) {
      buffer.putInt(memTablesId.size());
      for (long memTableId : memTablesId) {
        buffer.putLong(memTableId);
      }
    }
    if (version == WALFileVersion.V3) {
      for (long epoch : epochs) {
        buffer.putLong(epoch);
      }
      for (long syncIndex : syncIndices) {
        buffer.putLong(syncIndex);
      }
      buffer.putLong(minDataTs);
      buffer.putLong(maxDataTs);
      buffer.putLong(firstLogicalEpoch);
      buffer.putLong(firstLogicalSyncIndex);
      buffer.putLong(lastLogicalEpoch);
      buffer.putLong(lastLogicalSyncIndex);
      buffer.putInt(logicalSearchIndices.size());
      for (long logicalSearchIndex : logicalSearchIndices) {
        buffer.putLong(logicalSearchIndex);
      }
      for (long logicalEpoch : logicalEpochs) {
        buffer.putLong(logicalEpoch);
      }
      for (long logicalSyncIndex : logicalSyncIndices) {
        buffer.putLong(logicalSyncIndex);
      }
    }
  }

  public static WALMetaData deserialize(ByteBuffer buffer) {
    return deserialize(buffer, WALFileVersion.V2);
  }

  public static WALMetaData deserialize(ByteBuffer buffer, WALFileVersion version) {
    long firstSearchIndex = buffer.getLong();
    int entriesNum = buffer.getInt();
    List<Integer> buffersSize = new ArrayList<>(entriesNum);
    for (int i = 0; i < entriesNum; ++i) {
      buffersSize.add(buffer.getInt());
    }
    Set<Long> memTablesId = new HashSet<>();
    if (buffer.hasRemaining()) {
      int memTablesIdNum = buffer.getInt();
      for (int i = 0; i < memTablesIdNum; ++i) {
        memTablesId.add(buffer.getLong());
      }
    }
    WALMetaData result = new WALMetaData(firstSearchIndex, buffersSize, memTablesId);
    // V3 extension: per-entry epoch/syncIndex + file-level timestamp range
    if (version == WALFileVersion.V3 && buffer.hasRemaining()) {
      for (int i = 0; i < entriesNum; i++) {
        result.epochs.add(buffer.getLong());
      }
      for (int i = 0; i < entriesNum; i++) {
        result.syncIndices.add(buffer.getLong());
      }
      result.minDataTs = buffer.getLong();
      result.maxDataTs = buffer.getLong();
      if (buffer.remaining() >= Long.BYTES * 4 + Integer.BYTES) {
        result.firstLogicalEpoch = buffer.getLong();
        result.firstLogicalSyncIndex = buffer.getLong();
        result.lastLogicalEpoch = buffer.getLong();
        result.lastLogicalSyncIndex = buffer.getLong();
        final int logicalEntriesNum = buffer.getInt();
        for (int i = 0; i < logicalEntriesNum; i++) {
          result.logicalSearchIndices.add(buffer.getLong());
        }
        for (int i = 0; i < logicalEntriesNum; i++) {
          result.logicalEpochs.add(buffer.getLong());
        }
        for (int i = 0; i < logicalEntriesNum; i++) {
          result.logicalSyncIndices.add(buffer.getLong());
        }
      } else {
        result.rebuildLogicalEntriesFromPerEntryMetadata();
      }
    }
    return result;
  }

  public List<Integer> getBuffersSize() {
    return buffersSize;
  }

  public Set<Long> getMemTablesId() {
    return memTablesId;
  }

  public long getFirstSearchIndex() {
    return firstSearchIndex;
  }

  public List<Long> getEpochs() {
    return epochs;
  }

  public List<Long> getSyncIndices() {
    return syncIndices;
  }

  public List<Long> getLogicalSearchIndices() {
    return logicalSearchIndices;
  }

  public List<Long> getLogicalEpochs() {
    return logicalEpochs;
  }

  public List<Long> getLogicalSyncIndices() {
    return logicalSyncIndices;
  }

  public boolean hasLogicalEntries() {
    return !logicalSearchIndices.isEmpty();
  }

  public long getFirstLogicalSearchIndex() {
    return logicalSearchIndices.isEmpty()
        ? ConsensusReqReader.DEFAULT_SEARCH_INDEX
        : logicalSearchIndices.get(0);
  }

  public long getFirstLogicalEpoch() {
    return firstLogicalEpoch;
  }

  public long getFirstLogicalSyncIndex() {
    return firstLogicalSyncIndex;
  }

  public long getLastLogicalSearchIndex() {
    return logicalSearchIndices.isEmpty()
        ? ConsensusReqReader.DEFAULT_SEARCH_INDEX
        : logicalSearchIndices.get(logicalSearchIndices.size() - 1);
  }

  public long getLastLogicalEpoch() {
    return lastLogicalEpoch;
  }

  public long getLastLogicalSyncIndex() {
    return lastLogicalSyncIndex;
  }

  public WALMetaData copy() {
    WALMetaData copy =
        new WALMetaData(firstSearchIndex, new ArrayList<>(buffersSize), new HashSet<>(memTablesId));
    copy.truncateOffSet = truncateOffSet;
    copy.epochs.addAll(epochs);
    copy.syncIndices.addAll(syncIndices);
    copy.logicalSearchIndices.addAll(logicalSearchIndices);
    copy.logicalEpochs.addAll(logicalEpochs);
    copy.logicalSyncIndices.addAll(logicalSyncIndices);
    copy.firstLogicalEpoch = firstLogicalEpoch;
    copy.firstLogicalSyncIndex = firstLogicalSyncIndex;
    copy.lastLogicalEpoch = lastLogicalEpoch;
    copy.lastLogicalSyncIndex = lastLogicalSyncIndex;
    copy.minDataTs = minDataTs;
    copy.maxDataTs = maxDataTs;
    return copy;
  }

  public long getMinDataTs() {
    return minDataTs;
  }

  public long getMaxDataTs() {
    return maxDataTs;
  }

  private void rebuildLogicalEntriesFromPerEntryMetadata() {
    logicalSearchIndices.clear();
    logicalEpochs.clear();
    logicalSyncIndices.clear();

    long currentSearchIndex = firstSearchIndex;
    for (int i = 0; i < syncIndices.size(); i++) {
      final long entrySyncIndex = syncIndices.get(i);
      if (entrySyncIndex != ConsensusReqReader.DEFAULT_SEARCH_INDEX
          && (logicalSearchIndices.isEmpty()
              || logicalSearchIndices.get(logicalSearchIndices.size() - 1) != currentSearchIndex)) {
        logicalSearchIndices.add(currentSearchIndex);
        logicalEpochs.add(epochs.get(i));
        logicalSyncIndices.add(entrySyncIndex);
      }
      currentSearchIndex++;
    }

    if (!logicalSearchIndices.isEmpty()) {
      firstLogicalEpoch = logicalEpochs.get(0);
      firstLogicalSyncIndex = logicalSyncIndices.get(0);
      lastLogicalEpoch = logicalEpochs.get(logicalEpochs.size() - 1);
      lastLogicalSyncIndex = logicalSyncIndices.get(logicalSyncIndices.size() - 1);
    }
  }

  public static WALMetaData readFromWALFile(File logFile, FileChannel channel) throws IOException {
    if (channel.size() < WALFileVersion.V2.getVersionBytes().length
        || !isValidMagicString(channel)) {
      throw new BrokenWALFileException(logFile);
    }

    // load metadata size
    WALMetaData metaData = null;
    long position;
    try {
      ByteBuffer metadataSizeBuf = ByteBuffer.allocate(Integer.BYTES);
      WALFileVersion version = WALFileVersion.getVersion(channel);
      position = channel.size() - Integer.BYTES - (version.getVersionBytes().length);
      channel.read(metadataSizeBuf, position);
      metadataSizeBuf.flip();
      // load metadata
      int metadataSize = metadataSizeBuf.getInt();
      ByteBuffer metadataBuf = ByteBuffer.allocate(metadataSize);
      channel.read(metadataBuf, position - metadataSize);
      metadataBuf.flip();
      metaData = WALMetaData.deserialize(metadataBuf, version);
      // versions before V1.3, should recover memTable ids from entries
      if (metaData.memTablesId.isEmpty()) {
        int offset = Byte.BYTES;
        for (int size : metaData.buffersSize) {
          channel.position(offset);
          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
          channel.read(buffer);
          buffer.clear();
          metaData.memTablesId.add(buffer.getLong());
          offset += size;
        }
      }
    } catch (IllegalArgumentException e) {
      throw new BrokenWALFileException(logFile);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Unexpected exception", e);
    }
    return metaData;
  }

  private static boolean isValidMagicString(FileChannel channel) throws IOException {
    // V3 magic string is the longest; read enough bytes to check all versions
    int maxMagicLen =
        Math.max(
            WALFileVersion.V3.getVersionBytes().length, WALFileVersion.V2.getVersionBytes().length);
    ByteBuffer magicStringBytes = ByteBuffer.allocate(maxMagicLen);
    channel.read(magicStringBytes, channel.size() - maxMagicLen);
    magicStringBytes.flip();
    String magicString = new String(magicStringBytes.array(), StandardCharsets.UTF_8);
    return magicString.contains(WALFileVersion.V3.getVersionString())
        || magicString.contains(WALFileVersion.V2.getVersionString())
        || magicString.contains(WALFileVersion.V1.getVersionString());
  }

  public void setTruncateOffSet(long offset) {
    this.truncateOffSet = offset;
  }

  public long getTruncateOffSet() {
    return truncateOffSet;
  }
}
