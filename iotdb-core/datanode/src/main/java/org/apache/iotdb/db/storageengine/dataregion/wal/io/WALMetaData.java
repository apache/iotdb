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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Metadata exists at the end of each wal file, including each entry's size, search index of first
 * entry and the number of entries.
 *
 * <p>V3 extension stores per-entry writer progress metadata, plus file-level timestamp range, to
 * support consensus subscription recovery.
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

  // V3 fields: file-level data timestamp range for timestamp-based seek
  private long minDataTs = Long.MAX_VALUE;
  private long maxDataTs = Long.MIN_VALUE;
  // V3 extension for writer-based subscription progress.
  private final List<Long> physicalTimes;
  private final List<Short> nodeIds;
  private final List<Short> writerEpochs;
  private final List<Long> localSeqs;

  private static final short DEFAULT_NODE_ID = (short) -1;
  private static final short DEFAULT_WRITER_EPOCH = 0;
  private static final int V3_EMPTY_METADATA_REMAINING_WITHOUT_MEMTABLE_COUNT =
      Long.BYTES * 2 + Short.BYTES * 2 + Integer.BYTES;

  public WALMetaData() {
    this(ConsensusReqReader.DEFAULT_SEARCH_INDEX, new ArrayList<>(), new HashSet<>());
  }

  public WALMetaData(long firstSearchIndex, List<Integer> buffersSize, Set<Long> memTablesId) {
    this.firstSearchIndex = firstSearchIndex;
    this.buffersSize = buffersSize;
    this.memTablesId = memTablesId;
    this.physicalTimes = new ArrayList<>();
    this.nodeIds = new ArrayList<>();
    this.writerEpochs = new ArrayList<>();
    this.localSeqs = new ArrayList<>();
  }

  /** V2-compatible add without explicit writer progress metadata. */
  public void add(int size, long searchIndex, long memTableId) {
    add(size, searchIndex, memTableId, 0L, DEFAULT_NODE_ID, DEFAULT_WRITER_EPOCH, searchIndex);
  }

  /**
   * Compatibility add using the old (epoch, syncIndex) signature. The values are now interpreted as
   * (physicalTime, localSeq).
   */
  public void add(int size, long searchIndex, long memTableId, long epoch, long syncIndex) {
    add(
        size,
        searchIndex,
        memTableId,
        epoch,
        DEFAULT_NODE_ID,
        DEFAULT_WRITER_EPOCH,
        syncIndex >= 0 ? syncIndex : searchIndex);
  }

  public void add(
      int size,
      long searchIndex,
      long memTableId,
      long physicalTime,
      int nodeId,
      long writerEpoch,
      long localSeq) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = searchIndex;
    }
    buffersSize.add(size);
    memTablesId.add(memTableId);
    physicalTimes.add(physicalTime);
    nodeIds.add(toShortExact(nodeId, "nodeId"));
    writerEpochs.add(toShortExact(writerEpoch, "writerEpoch"));
    localSeqs.add(localSeq);
  }

  private static short toShortExact(long value, String fieldName) {
    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%s %s exceeds short range", fieldName, value));
    }
    return (short) value;
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
    physicalTimes.addAll(metaData.getPhysicalTimes());
    nodeIds.addAll(metaData.getNodeIds());
    writerEpochs.addAll(metaData.getWriterEpochs());
    localSeqs.addAll(metaData.getLocalSeqs());
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
      // minDataTs(long) + maxDataTs(long)
      size += Long.BYTES * 2;
      // physicalTimes(long[]) + localSeqs(long[])
      size += buffersSize.size() * Long.BYTES * 2;
      // defaultNodeId(short) + defaultWriterEpoch(short) + overrideCount(int)
      // + override ordinals(int[]) + override nodeIds(short[]) + override writerEpochs(short[])
      final int overrideCount = getWriterOverrideCount();
      size += Short.BYTES * 2 + Integer.BYTES;
      size += overrideCount * (Integer.BYTES + Short.BYTES + Short.BYTES);
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
      buffer.putLong(minDataTs);
      buffer.putLong(maxDataTs);
      for (long physicalTime : physicalTimes) {
        buffer.putLong(physicalTime);
      }
      for (long localSeq : localSeqs) {
        buffer.putLong(localSeq);
      }
      final short defaultNodeId = computeDefaultNodeId();
      final short defaultWriterEpoch = computeDefaultWriterEpoch();
      final List<Integer> overrideIndexes = new ArrayList<>();
      final List<Short> overrideNodeIds = new ArrayList<>();
      final List<Short> overrideWriterEpochs = new ArrayList<>();
      for (int i = 0; i < buffersSize.size(); i++) {
        final short nodeId = nodeIds.get(i);
        final short writerEpoch = writerEpochs.get(i);
        if (nodeId != defaultNodeId || writerEpoch != defaultWriterEpoch) {
          overrideIndexes.add(i);
          overrideNodeIds.add(nodeId);
          overrideWriterEpochs.add(writerEpoch);
        }
      }
      buffer.putShort(defaultNodeId);
      buffer.putShort(defaultWriterEpoch);
      buffer.putInt(overrideIndexes.size());
      for (int overrideIndex : overrideIndexes) {
        buffer.putInt(overrideIndex);
      }
      for (short nodeId : overrideNodeIds) {
        buffer.putShort(nodeId);
      }
      for (short writerEpoch : overrideWriterEpochs) {
        buffer.putShort(writerEpoch);
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
    final boolean serializedEmptyV3WithoutMemTableCount =
        version == WALFileVersion.V3
            && entriesNum == 0
            && buffer.remaining() == V3_EMPTY_METADATA_REMAINING_WITHOUT_MEMTABLE_COUNT;
    if (buffer.hasRemaining() && !serializedEmptyV3WithoutMemTableCount) {
      int memTablesIdNum = buffer.getInt();
      for (int i = 0; i < memTablesIdNum; ++i) {
        memTablesId.add(buffer.getLong());
      }
    }
    WALMetaData result = new WALMetaData(firstSearchIndex, buffersSize, memTablesId);
    // V3 extension: file-level timestamp range + per-entry writer progress metadata
    if (version == WALFileVersion.V3 && buffer.hasRemaining()) {
      result.minDataTs = buffer.getLong();
      result.maxDataTs = buffer.getLong();
      if (buffer.remaining() >= entriesNum * Long.BYTES * 2 + Short.BYTES * 2 + Integer.BYTES) {
        for (int i = 0; i < entriesNum; i++) {
          result.physicalTimes.add(buffer.getLong());
        }
        for (int i = 0; i < entriesNum; i++) {
          result.localSeqs.add(buffer.getLong());
        }
        final short defaultNodeId = buffer.getShort();
        final short defaultWriterEpoch = buffer.getShort();
        final int overrideCount = buffer.getInt();
        final int[] overrideIndexes = new int[overrideCount];
        final short[] overrideNodeIds = new short[overrideCount];
        final short[] overrideWriterEpochs = new short[overrideCount];
        for (int i = 0; i < overrideCount; i++) {
          overrideIndexes[i] = buffer.getInt();
        }
        for (int i = 0; i < overrideCount; i++) {
          overrideNodeIds[i] = buffer.getShort();
        }
        for (int i = 0; i < overrideCount; i++) {
          overrideWriterEpochs[i] = buffer.getShort();
        }
        for (int i = 0; i < entriesNum; i++) {
          result.nodeIds.add(defaultNodeId);
          result.writerEpochs.add(defaultWriterEpoch);
        }
        for (int i = 0; i < overrideCount; i++) {
          result.nodeIds.set(overrideIndexes[i], overrideNodeIds[i]);
          result.writerEpochs.set(overrideIndexes[i], overrideWriterEpochs[i]);
        }
      } else {
        result.rebuildWriterMetadataWithDefaults();
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

  public List<Long> getPhysicalTimes() {
    return physicalTimes;
  }

  public List<Short> getNodeIds() {
    return nodeIds;
  }

  public List<Short> getWriterEpochs() {
    return writerEpochs;
  }

  public List<Long> getLocalSeqs() {
    return localSeqs;
  }

  private short computeDefaultNodeId() {
    return unpackNodeId(computeDefaultWriterIdentity());
  }

  private short computeDefaultWriterEpoch() {
    return unpackWriterEpoch(computeDefaultWriterIdentity());
  }

  private int getWriterOverrideCount() {
    final short defaultNodeId = computeDefaultNodeId();
    final short defaultWriterEpoch = computeDefaultWriterEpoch();
    int count = 0;
    for (int i = 0; i < buffersSize.size(); i++) {
      if (nodeIds.get(i) != defaultNodeId || writerEpochs.get(i) != defaultWriterEpoch) {
        count++;
      }
    }
    return count;
  }

  private int computeDefaultWriterIdentity() {
    if (nodeIds.isEmpty()) {
      return packWriterIdentity(DEFAULT_NODE_ID, DEFAULT_WRITER_EPOCH);
    }
    final Map<Integer, Integer> counts = new HashMap<>();
    int bestIdentity = packWriterIdentity(nodeIds.get(0), writerEpochs.get(0));
    int bestCount = 0;
    for (int i = 0; i < nodeIds.size(); i++) {
      final int identity = packWriterIdentity(nodeIds.get(i), writerEpochs.get(i));
      final int count = counts.merge(identity, 1, Integer::sum);
      if (count > bestCount) {
        bestCount = count;
        bestIdentity = identity;
      }
    }
    return bestIdentity;
  }

  private static int packWriterIdentity(short nodeId, short writerEpoch) {
    return ((nodeId & 0xFFFF) << 16) | (writerEpoch & 0xFFFF);
  }

  private static short unpackNodeId(int identity) {
    return (short) (identity >>> 16);
  }

  private static short unpackWriterEpoch(int identity) {
    return (short) identity;
  }

  public WALMetaData copy() {
    WALMetaData copy =
        new WALMetaData(firstSearchIndex, new ArrayList<>(buffersSize), new HashSet<>(memTablesId));
    copy.truncateOffSet = truncateOffSet;
    copy.physicalTimes.addAll(physicalTimes);
    copy.nodeIds.addAll(nodeIds);
    copy.writerEpochs.addAll(writerEpochs);
    copy.localSeqs.addAll(localSeqs);
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

  private void rebuildWriterMetadataWithDefaults() {
    physicalTimes.clear();
    nodeIds.clear();
    writerEpochs.clear();
    localSeqs.clear();
    for (int i = 0; i < buffersSize.size(); i++) {
      physicalTimes.add(0L);
      nodeIds.add(DEFAULT_NODE_ID);
      writerEpochs.add(DEFAULT_WRITER_EPOCH);
      localSeqs.add(firstSearchIndex + i);
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
