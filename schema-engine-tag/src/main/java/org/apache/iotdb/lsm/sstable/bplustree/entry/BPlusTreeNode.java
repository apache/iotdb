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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BPlusTreeNode implements IEntry {

  private BPlusTreeNodeType bPlusTreeNodeType;

  private int count;

  private List<BPlusTreeEntry> bPlusTreeEntries;

  public BPlusTreeNode() {}

  public BPlusTreeNode(BPlusTreeNodeType bPlusTreeNodeType) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
    bPlusTreeEntries = new ArrayList<>();
    count = 0;
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
  public IEntry deserialize(DataInputStream input) throws IOException {
    byte type = input.readByte();
    if (type == 0) {
      bPlusTreeNodeType = BPlusTreeNodeType.INVALID_NODE;
    } else if (type == 1) {
      bPlusTreeNodeType = BPlusTreeNodeType.INTERNAL_NODE;
    } else {
      bPlusTreeNodeType = BPlusTreeNodeType.LEAF_NODE;
    }
    count = input.readInt();
    bPlusTreeEntries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      BPlusTreeEntry bPlusTreeEntry = new BPlusTreeEntry();
      bPlusTreeEntries.add((BPlusTreeEntry) bPlusTreeEntry.deserialize(input));
    }
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    if (type == 0) {
      bPlusTreeNodeType = BPlusTreeNodeType.INVALID_NODE;
    } else if (type == 1) {
      bPlusTreeNodeType = BPlusTreeNodeType.INTERNAL_NODE;
    } else {
      bPlusTreeNodeType = BPlusTreeNodeType.LEAF_NODE;
    }
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

  public List<BPlusTreeEntry> getBPlusTreeEntryFromLeafNode(Set<String> names) {
    if (!bPlusTreeNodeType.equals(BPlusTreeNodeType.LEAF_NODE)) {
      return null;
    }
    if (bPlusTreeEntries == null
        || bPlusTreeEntries.size() == 0
        || names == null
        || names.size() == 0) {
      return new ArrayList<>();
    }
    List<BPlusTreeEntry> bPlusTreeEntryList = new ArrayList<>();
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      if (names.contains(bPlusTreeEntry.getName())) {
        bPlusTreeEntryList.add(bPlusTreeEntry);
      }
    }
    return bPlusTreeEntryList;
  }

  public Map<BPlusTreeEntry, Set<String>> getBPlusTreeEntryFromInternalNode(Set<String> names) {
    if (!bPlusTreeNodeType.equals(BPlusTreeNodeType.INTERNAL_NODE)) {
      return null;
    }
    if (bPlusTreeEntries == null
        || bPlusTreeEntries.size() == 0
        || names == null
        || names.size() == 0) {
      return new HashMap<>();
    }
    Map<BPlusTreeEntry, Set<String>> map = new HashMap<>();
    for (String name : names) {
      for (int i = bPlusTreeEntries.size() - 1; i >= 0; i--) {
        BPlusTreeEntry bPlusTreeEntry = bPlusTreeEntries.get(i);
        if (bPlusTreeEntry.getName().compareTo(name) <= 0) {
          if (!map.containsKey(bPlusTreeEntry)) {
            map.put(bPlusTreeEntry, new HashSet<>());
          }
          map.get(bPlusTreeEntry).add(name);
          break;
        }
      }
    }
    return map;
  }
}
