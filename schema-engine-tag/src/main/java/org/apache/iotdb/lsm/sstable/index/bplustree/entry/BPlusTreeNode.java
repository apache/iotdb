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
package org.apache.iotdb.lsm.sstable.index.bplustree.entry;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Represents a b+ tree node */
public class BPlusTreeNode implements IDiskEntry {

  // leaf node or internal node
  private BPlusTreeNodeType bPlusTreeNodeType;

  // The number of BPlusTreeEntry saved by the b+ tree node
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
        + ", count="
        + count
        + ", bPlusTreeEntries="
        + bPlusTreeEntries
        + '}';
  }

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    int len = bPlusTreeNodeType.serialize(out);
    len += ReadWriteIOUtils.write(count, out);
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      len += bPlusTreeEntry.serialize(out);
    }
    return len;
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    byte type = input.readByte();
    if (type == 0) {
      bPlusTreeNodeType = BPlusTreeNodeType.INVALID_NODE;
    } else if (type == 1) {
      bPlusTreeNodeType = BPlusTreeNodeType.INTERNAL_NODE;
    } else {
      bPlusTreeNodeType = BPlusTreeNodeType.LEAF_NODE;
    }
    count = ReadWriteIOUtils.readInt(input);
    bPlusTreeEntries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      BPlusTreeEntry bPlusTreeEntry = new BPlusTreeEntry();
      bPlusTreeEntries.add((BPlusTreeEntry) bPlusTreeEntry.deserialize(input));
    }
    return this;
  }

  @TestOnly
  public IDiskEntry deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    if (type == 0) {
      bPlusTreeNodeType = BPlusTreeNodeType.INVALID_NODE;
    } else if (type == 1) {
      bPlusTreeNodeType = BPlusTreeNodeType.INTERNAL_NODE;
    } else {
      bPlusTreeNodeType = BPlusTreeNodeType.LEAF_NODE;
    }
    count = ReadWriteIOUtils.readInt(byteBuffer);
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

  public boolean addAndSerialize(BPlusTreeEntry bPlusTreeEntry, ByteBuffer byteBuffer) {
    int position = byteBuffer.position();
    try {
      bPlusTreeEntry.serialize(byteBuffer);
    } catch (BufferOverflowException e) {
      if (count <= 1) {
        throw new RuntimeException("b+ tree page size is too small");
      }
      byteBuffer.position(position);
      return false;
    }
    add(bPlusTreeEntry);
    return true;
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

  /**
   * Determine whether the b+ tree leaf node has a record matching names at one time, and return all
   * records if there is
   *
   * @param names A set of names to be matched
   * @return Return all records that can match any name in the name set
   */
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
    for (String name : names) {
      BPlusTreeEntry bPlusTreeEntry = binarySearch(name);
      if (bPlusTreeEntry != null && bPlusTreeEntry.getName().equals(name)) {
        bPlusTreeEntryList.add(bPlusTreeEntry);
      }
    }
    return bPlusTreeEntryList;
  }

  /**
   * According to the names set, find the next entry that needs to be matched and the names to be
   * matched by the entry in the internal node
   *
   * @param names A set of names to be matched
   * @return a map, The key is the b+ tree entry, and the value represents the name collection that
   *     needs to be searched on the b+ tree node corresponding to the entry
   */
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
      BPlusTreeEntry bPlusTreeEntry = binarySearch(name);
      if (bPlusTreeEntry != null) {
        if (!map.containsKey(bPlusTreeEntry)) {
          map.put(bPlusTreeEntry, new HashSet<>());
        }
        map.get(bPlusTreeEntry).add(name);
      }
    }
    return map;
  }

  private BPlusTreeEntry binarySearch(String name) {
    int l = 0;
    int r = bPlusTreeEntries.size() - 1;
    while (l < r) {
      int mid = r - ((r - l) >> 1);
      if (bPlusTreeEntries.get(mid).getName().compareTo(name) <= 0) {
        l = mid;
      } else {
        r = mid - 1;
      }
    }
    return bPlusTreeEntries.get(l).getName().compareTo(name) <= 0 ? bPlusTreeEntries.get(l) : null;
  }
}
