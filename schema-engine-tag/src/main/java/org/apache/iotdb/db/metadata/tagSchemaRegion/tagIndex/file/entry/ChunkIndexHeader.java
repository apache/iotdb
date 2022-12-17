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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ChunkIndexHeader implements IEntry {
  private int size;

  public ChunkIndexHeader() {}

  public ChunkIndexHeader(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public String toString() {
    return "ChunkIndexHeader{" + "size=" + size + '}';
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(size, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(size, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    size = ReadWriteIOUtils.readInt(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    size = ReadWriteIOUtils.readInt(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChunkIndexHeader that = (ChunkIndexHeader) o;
    return size == that.size;
  }

  @Override
  public int hashCode() {
    return Objects.hash(size);
  }
}
