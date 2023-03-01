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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry;

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Represents the chunk index structure in tifile */
public class ChunkIndex implements IDiskEntry {
  private List<ChunkMetaEntry> chunkMetaEntries;

  private ChunkMetaHeader chunkMetaHeader;

  public ChunkIndex(List<ChunkMetaEntry> chunkIndexEntries, ChunkMetaHeader chunkMetaHeader) {
    this.chunkMetaEntries = chunkIndexEntries;
    this.chunkMetaHeader = chunkMetaHeader;
  }

  public ChunkIndex() {
    this.chunkMetaEntries = new ArrayList<>();
  }

  public List<ChunkMetaEntry> getChunkMetaEntries() {
    return chunkMetaEntries;
  }

  public void setChunkMetaEntries(List<ChunkMetaEntry> chunkMetaEntries) {
    this.chunkMetaEntries = chunkMetaEntries;
  }

  public ChunkMetaHeader getChunkMetaHeader() {
    return chunkMetaHeader;
  }

  public void setChunkMetaHeader(ChunkMetaHeader chunkMetaHeader) {
    this.chunkMetaHeader = chunkMetaHeader;
  }

  public int getAllCount() {
    if (chunkMetaEntries == null || chunkMetaEntries.size() == 0) {
      return 0;
    }
    int count = 0;
    for (ChunkMetaEntry chunkMetaEntry : chunkMetaEntries) {
      count += chunkMetaEntry.getCount();
    }
    return count;
  }

  public ChunkMetaHeader getChunkIndexHeader() {
    return chunkMetaHeader;
  }

  public void setChunkIndexHeader(ChunkMetaHeader chunkMetaHeader) {
    this.chunkMetaHeader = chunkMetaHeader;
  }

  @Override
  public String toString() {
    return "ChunkMeta{"
        + "chunkIndexEntries="
        + chunkMetaEntries
        + ", chunkMetaHeader="
        + chunkMetaHeader
        + '}';
  }

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    int len = chunkMetaHeader.serialize(out);
    for (ChunkMetaEntry chunkMetaEntry : chunkMetaEntries) {
      len += chunkMetaEntry.serialize(out);
    }
    return len;
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    chunkMetaHeader = new ChunkMetaHeader();
    chunkMetaHeader.deserialize(input);
    chunkMetaEntries = new ArrayList<>(chunkMetaHeader.getSize());
    for (int i = 0; i < chunkMetaHeader.getSize(); i++) {
      ChunkMetaEntry chunkMetaEntry = new ChunkMetaEntry();
      chunkMetaEntry.deserialize(input);
      chunkMetaEntries.add(chunkMetaEntry);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkIndex that = (ChunkIndex) o;
    return Objects.equals(chunkMetaEntries, that.chunkMetaEntries)
        && Objects.equals(chunkMetaHeader, that.chunkMetaHeader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkMetaEntries, chunkMetaHeader);
  }
}
