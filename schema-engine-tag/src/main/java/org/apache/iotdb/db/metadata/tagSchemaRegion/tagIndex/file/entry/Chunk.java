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

import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;

import org.roaringbitmap.InvalidRoaringFormat;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class Chunk implements IEntry {

  private RoaringBitmap roaringBitmap;

  private ChunkHeader chunkHeader;

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    roaringBitmap.serialize(out);
    chunkHeader.serialize(out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    roaringBitmap.serialize(byteBuffer);
    chunkHeader.serialize(byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    roaringBitmap = new RoaringBitmap();
    roaringBitmap.deserialize(input);
    chunkHeader = new ChunkHeader();
    chunkHeader.deserialize(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    roaringBitmap = new RoaringBitmap();
    try {
      roaringBitmap.deserialize(byteBuffer);
    } catch (IOException e) {
      throw new InvalidRoaringFormat(e.getMessage());
    }
    chunkHeader = new ChunkHeader();
    chunkHeader.deserialize(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Chunk chunk = (Chunk) o;
    return Objects.equals(chunkHeader, chunk.chunkHeader)
        && Objects.equals(roaringBitmap, chunk.roaringBitmap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkHeader, roaringBitmap);
  }

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  public void setChunkHeader(ChunkHeader chunkHeader) {
    this.chunkHeader = chunkHeader;
  }

  public RoaringBitmap getRoaringBitmap() {
    return roaringBitmap;
  }

  public void setRoaringBitmap(RoaringBitmap roaringBitmap) {
    this.roaringBitmap = roaringBitmap;
  }

  @Override
  public String toString() {
    return "Chunk{" + "chunkHeader=" + chunkHeader + ", roaringBitmap=" + roaringBitmap + '}';
  }
}
