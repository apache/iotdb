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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.utils.SerializedSize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.wal.node.WALNode.DEFAULT_SEARCH_INDEX;

/**
 * Metadata exists at the end of each wal file, including each entry's size, search index of first
 * entry and the number of entries.
 */
public class WALMetaData implements SerializedSize {
  /** search index 8 byte, wal entries' number 4 bytes */
  private static final int FIXED_SERIALIZED_SIZE = Long.BYTES + Integer.BYTES;

  /** search index of first entry */
  private long firstSearchIndex;

  /** each entry's size */
  private final List<Integer> buffersSize;

  public WALMetaData() {
    this(DEFAULT_SEARCH_INDEX, new ArrayList<>());
  }

  public WALMetaData(long firstSearchIndex, List<Integer> buffersSize) {
    this.firstSearchIndex = firstSearchIndex;
    this.buffersSize = buffersSize;
  }

  public void add(int size, long searchIndex) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = searchIndex;
    }
    buffersSize.add(size);
  }

  public void addAll(WALMetaData metaData) {
    if (buffersSize.isEmpty()) {
      firstSearchIndex = metaData.getFirstSearchIndex();
    }
    buffersSize.addAll(metaData.getBuffersSize());
  }

  @Override
  public int serializedSize() {
    return FIXED_SERIALIZED_SIZE + buffersSize.size() * Integer.BYTES;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putLong(firstSearchIndex);
    buffer.putInt(buffersSize.size());
    for (int size : buffersSize) {
      buffer.putInt(size);
    }
  }

  public static WALMetaData deserialize(ByteBuffer buffer) {
    long firstSearchIndex = buffer.getLong();
    int entriesNum = buffer.getInt();
    List<Integer> buffersSize = new ArrayList<>(entriesNum);
    for (int i = 0; i < entriesNum; ++i) {
      buffersSize.add(buffer.getInt());
    }
    return new WALMetaData(firstSearchIndex, buffersSize);
  }

  public List<Integer> getBuffersSize() {
    return buffersSize;
  }

  public long getFirstSearchIndex() {
    return firstSearchIndex;
  }
}
