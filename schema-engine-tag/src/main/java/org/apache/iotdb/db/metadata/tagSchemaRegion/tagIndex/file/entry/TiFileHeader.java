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

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.bplustree.entry.IEntry;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TiFileHeader implements IEntry {

  private int bPLusTreePageSize;

  private long tagKeyIndexOffset;

  private long bloomFilterOffset;

  public TiFileHeader() {
    bPLusTreePageSize =
        TagSchemaDescriptor.getInstance().getTagSchemaConfig().getBPlusTreePageSize();
  }

  public int getbPLusTreePageSize() {
    return bPLusTreePageSize;
  }

  public void setbPLusTreePageSize(int bPLusTreePageSize) {
    this.bPLusTreePageSize = bPLusTreePageSize;
  }

  public long getTagKeyIndexOffset() {
    return tagKeyIndexOffset;
  }

  public void setTagKeyIndexOffset(long tagKeyIndexOffset) {
    this.tagKeyIndexOffset = tagKeyIndexOffset;
  }

  public long getBloomFilterOffset() {
    return bloomFilterOffset;
  }

  public void setBloomFilterOffset(long bloomFilterOffset) {
    this.bloomFilterOffset = bloomFilterOffset;
  }

  public static int getSerializeSize() {
    return 20;
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(bPLusTreePageSize, out);
    ReadWriteIOUtils.write(tagKeyIndexOffset, out);
    ReadWriteIOUtils.write(bloomFilterOffset, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(bPLusTreePageSize, byteBuffer);
    ReadWriteIOUtils.write(tagKeyIndexOffset, byteBuffer);
    ReadWriteIOUtils.write(bloomFilterOffset, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    bPLusTreePageSize = ReadWriteIOUtils.readInt(input);
    tagKeyIndexOffset = ReadWriteIOUtils.readLong(input);
    bloomFilterOffset = ReadWriteIOUtils.readLong(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    bPLusTreePageSize = ReadWriteIOUtils.readInt(byteBuffer);
    tagKeyIndexOffset = ReadWriteIOUtils.readLong(byteBuffer);
    bloomFilterOffset = ReadWriteIOUtils.readLong(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TiFileHeader that = (TiFileHeader) o;
    return bPLusTreePageSize == that.bPLusTreePageSize
        && tagKeyIndexOffset == that.tagKeyIndexOffset
        && bloomFilterOffset == that.bloomFilterOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bPLusTreePageSize, tagKeyIndexOffset, bloomFilterOffset);
  }

  @Override
  public String toString() {
    return "TiFileHeader{"
        + "bPLusTreePageSize="
        + bPLusTreePageSize
        + ", tagKeyIndexOffset="
        + tagKeyIndexOffset
        + ", bloomFilterOffset="
        + bloomFilterOffset
        + '}';
  }
}
