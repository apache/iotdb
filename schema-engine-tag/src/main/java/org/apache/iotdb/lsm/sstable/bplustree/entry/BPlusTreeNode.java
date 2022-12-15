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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BPlusTreeNode implements IEntry {

  private BPlusTreeNodeType bPlusTreeNodeType;

  private int count;

  private List<BPlusTreeEntry> bPlusTreeEntries;

  public BPlusTreeNode() {}

  public BPlusTreeNode(BPlusTreeNodeType bPlusTreeNodeType) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
    bPlusTreeEntries = new ArrayList<>();
    count = bPlusTreeEntries.size();
  }

  public BPlusTreeNode(BPlusTreeNodeType bPlusTreeNodeType, List<BPlusTreeEntry> bPlusTreeEntries) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
    this.bPlusTreeEntries = bPlusTreeEntries;
    count = bPlusTreeEntries.size();
  }

  public BPlusTreeNodeType getbPlusTreeNodeType() {
    return bPlusTreeNodeType;
  }

  public void setbPlusTreeNodeType(BPlusTreeNodeType bPlusTreeNodeType) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
  }

  public List<BPlusTreeEntry> getbPlusTreeEntries() {
    return bPlusTreeEntries;
  }

  public void setbPlusTreeEntries(List<BPlusTreeEntry> bPlusTreeEntries) {
    this.bPlusTreeEntries = bPlusTreeEntries;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return "BPlusTreeNode{"
        + "bPlusTreeNodeType="
        + bPlusTreeNodeType
        + ", bPlusTreeEntries="
        + bPlusTreeEntries
        + '}';
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    bPlusTreeNodeType.serialize(out);
    out.writeInt(count);
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      bPlusTreeEntry.serialize(out);
    }
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    bPlusTreeNodeType.serialize(byteBuffer);
    byteBuffer.putInt(count);
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      bPlusTreeEntry.serialize(byteBuffer);
    }
  }

  @Override
  public IEntry deserialize(DataInput input) throws IOException {
    return null;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    bPlusTreeNodeType = BPlusTreeNodeType.INVALID_NODE;
    bPlusTreeNodeType.deserialize(byteBuffer);
    count = byteBuffer.getInt();
    bPlusTreeEntries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      BPlusTreeEntry bPlusTreeEntry = new BPlusTreeEntry();
      bPlusTreeEntries.add((BPlusTreeEntry) bPlusTreeEntry.deserialize(byteBuffer));
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BPlusTreeNode that = (BPlusTreeNode) o;
    return count == that.count
        && bPlusTreeNodeType == that.bPlusTreeNodeType
        && Objects.equals(bPlusTreeEntries, that.bPlusTreeEntries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bPlusTreeNodeType, count, bPlusTreeEntries);
  }

  public void add(BPlusTreeEntry bPlusTreeEntry) {
    if (bPlusTreeEntries == null) {
      bPlusTreeEntries = new ArrayList<>();
    }
    bPlusTreeEntries.add(bPlusTreeEntry);
    count++;
  }

  public String getMin() {
    if (bPlusTreeEntries == null || bPlusTreeEntries.size() == 0) return null;
    return bPlusTreeEntries.get(0).getName();
  }

  public String getMax() {
    if (bPlusTreeEntries == null || bPlusTreeEntries.size() == 0) return null;
    return bPlusTreeEntries.get(bPlusTreeEntries.size() - 1).getName();
  }

  public boolean needToSplit(int degree) {
    if (bPlusTreeEntries == null) return false;
    return bPlusTreeEntries.size() >= degree;
  }
}
