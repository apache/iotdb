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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ChunkHeader implements IEntry {

  private int size;

  private int count;

  private int maxID;

  private int minID;

  public ChunkHeader() {}

  public ChunkHeader(int size, int count, int maxID, int minID) {
    this.size = size;
    this.count = count;
    this.maxID = maxID;
    this.minID = minID;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    out.writeInt(size);
    out.writeInt(count);
    // TODO if count == 0
    out.writeInt(maxID);
    out.writeInt(minID);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    byteBuffer.putInt(size);
    byteBuffer.putInt(count);
    byteBuffer.putInt(maxID);
    byteBuffer.putInt(minID);
  }

  @Override
  public IEntry deserialize(DataInputStream in) throws IOException {
    this.size = in.readInt();
    count = in.readInt();
    maxID = in.readInt();
    minID = in.readInt();
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    this.size = byteBuffer.getInt();
    count = byteBuffer.getInt();
    maxID = byteBuffer.getInt();
    minID = byteBuffer.getInt();
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkHeader that = (ChunkHeader) o;
    return size == that.size && count == that.count && maxID == that.maxID && minID == that.minID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, count, maxID, minID);
  }

  @Override
  public String toString() {
    return "ChunkHeader{"
        + "size="
        + size
        + ", count="
        + count
        + ", maxID="
        + maxID
        + ", minID="
        + minID
        + '}';
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getMaxID() {
    return maxID;
  }

  public void setMaxID(int maxID) {
    this.maxID = maxID;
  }

  public int getMinID() {
    return minID;
  }

  public void setMinID(int minID) {
    this.minID = minID;
  }
}
