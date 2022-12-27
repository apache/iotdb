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
package org.apache.iotdb.lsm.sstable.bplustree.entry;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Record some additional information about the b+ tree */
public class BPlusTreeHeader implements IEntry {

  // Maximum Records Saved
  String max;

  // Minimum records Saved
  String min;

  // offset of the root node
  long rootNodeOffset = -1;

  // offset of the first leaf node
  long firstLeftNodeOffset = -1;

  // The number of leaf nodes
  int leftNodeCount = -1;

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(max, out);
    ReadWriteIOUtils.write(min, out);
    ReadWriteIOUtils.write(rootNodeOffset, out);
    ReadWriteIOUtils.write(firstLeftNodeOffset, out);
    ReadWriteIOUtils.write(leftNodeCount, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(max, byteBuffer);
    ReadWriteIOUtils.write(min, byteBuffer);
    ReadWriteIOUtils.write(rootNodeOffset, byteBuffer);
    ReadWriteIOUtils.write(firstLeftNodeOffset, byteBuffer);
    ReadWriteIOUtils.write(leftNodeCount, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    max = ReadWriteIOUtils.readString(input);
    min = ReadWriteIOUtils.readString(input);
    rootNodeOffset = ReadWriteIOUtils.readLong(input);
    firstLeftNodeOffset = ReadWriteIOUtils.readLong(input);
    leftNodeCount = ReadWriteIOUtils.readInt(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    max = ReadWriteIOUtils.readString(byteBuffer);
    min = ReadWriteIOUtils.readString(byteBuffer);
    rootNodeOffset = ReadWriteIOUtils.readLong(byteBuffer);
    firstLeftNodeOffset = ReadWriteIOUtils.readLong(byteBuffer);
    leftNodeCount = ReadWriteIOUtils.readInt(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BPlusTreeHeader that = (BPlusTreeHeader) o;
    return rootNodeOffset == that.rootNodeOffset
        && firstLeftNodeOffset == that.firstLeftNodeOffset
        && leftNodeCount == that.leftNodeCount
        && Objects.equals(max, that.max)
        && Objects.equals(min, that.min);
  }

  @Override
  public int hashCode() {
    return Objects.hash(max, min, rootNodeOffset, firstLeftNodeOffset, leftNodeCount);
  }

  @Override
  public String toString() {
    return "BPlusTreeHeader{"
        + "max='"
        + max
        + '\''
        + ", min='"
        + min
        + '\''
        + ", rootNodeOffset="
        + rootNodeOffset
        + ", firstLeftNodeOffset="
        + firstLeftNodeOffset
        + ", leftNodeCount="
        + leftNodeCount
        + '}';
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public long getRootNodeOffset() {
    return rootNodeOffset;
  }

  public void setRootNodeOffset(long rootNodeOffset) {
    this.rootNodeOffset = rootNodeOffset;
  }

  public long getFirstLeftNodeOffset() {
    return firstLeftNodeOffset;
  }

  public void setFirstLeftNodeOffset(long firstLeftNodeOffset) {
    this.firstLeftNodeOffset = firstLeftNodeOffset;
  }

  public int getLeftNodeCount() {
    return leftNodeCount;
  }

  public void setLeftNodeCount(int leftNodeCount) {
    this.leftNodeCount = leftNodeCount;
  }
}
