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
import org.apache.iotdb.db.utils.SerializedSize;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Metadata exists at the end of each wal file, including each entry's size, search index of first
 * entry and the number of entries.
 */
public class WALMetaData implements SerializedSize {
  // search index 8 byte, wal entries' number 4 bytes
  private static final int FIXED_SERIALIZED_SIZE = Long.BYTES + Integer.BYTES;

  // search index of first entry
  private long firstSearchIndex;
  // each entry's size
  private final List<Integer> buffersSize;
  // memTable ids of this wal file
  private final Set<Long> memTablesId;

  public WALMetaData() {
    this(ConsensusReqReader.DEFAULT_SEARCH_INDEX, new ArrayList<>(), new HashSet<>());
  }

  public WALMetaData(long firstSearchIndex, List<Integer> buffersSize, Set<Long> memTablesId) {
    this.firstSearchIndex = firstSearchIndex;
    this.buffersSize = buffersSize;
    this.memTablesId = memTablesId;
  }

  public void add(int size, long searchIndex, long memTableId) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = searchIndex;
    }
    buffersSize.add(size);
    memTablesId.add(memTableId);
  }

  public void addAll(WALMetaData metaData) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = metaData.getFirstSearchIndex();
    }
    buffersSize.addAll(metaData.getBuffersSize());
    memTablesId.addAll(metaData.getMemTablesId());
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE
        + buffersSize.size() * Integer.BYTES
        + (memTablesId.isEmpty() ? 0 : Integer.BYTES + memTablesId.size() * Long.BYTES);
  }

  public void serialize(ByteBuffer buffer) {
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
  }

  public static WALMetaData deserialize(ByteBuffer buffer) {
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
    return new WALMetaData(firstSearchIndex, buffersSize, memTablesId);
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

  public static WALMetaData readFromWALFile(File logFile, FileChannel channel) throws IOException {
    if (channel.size() < WALWriter.MAGIC_STRING_BYTES || !isValidMagicString(channel)) {
      throw new IOException(String.format("Broken wal file %s", logFile));
    }
    // load metadata size
    ByteBuffer metadataSizeBuf = ByteBuffer.allocate(Integer.BYTES);
    long position = channel.size() - WALWriter.MAGIC_STRING_BYTES - Integer.BYTES;
    channel.read(metadataSizeBuf, position);
    metadataSizeBuf.flip();
    // load metadata
    int metadataSize = metadataSizeBuf.getInt();
    ByteBuffer metadataBuf = ByteBuffer.allocate(metadataSize);
    channel.read(metadataBuf, position - metadataSize);
    metadataBuf.flip();
    WALMetaData metaData = WALMetaData.deserialize(metadataBuf);
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
    return metaData;
  }

  private static boolean isValidMagicString(FileChannel channel) throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(WALWriter.MAGIC_STRING_BYTES);
    channel.read(magicStringBytes, channel.size() - WALWriter.MAGIC_STRING_BYTES);
    magicStringBytes.flip();
    String magicString = new String(magicStringBytes.array());
    return magicString.equals(WALWriter.MAGIC_STRING)
        || magicString.startsWith(WALWriter.MAGIC_STRING_V1);
  }
}
