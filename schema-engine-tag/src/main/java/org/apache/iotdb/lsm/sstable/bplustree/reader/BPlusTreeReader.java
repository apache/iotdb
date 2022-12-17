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
package org.apache.iotdb.lsm.sstable.bplustree.reader;

import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNodeType;
import org.apache.iotdb.lsm.sstable.fileIO.FileInput;
import org.apache.iotdb.lsm.sstable.fileIO.IFileInput;

import org.apache.commons.lang3.tuple.MutableTriple;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

public class BPlusTreeReader implements IBPlusTreeReader {

  private IFileInput fileInput;

  private BPlusTreeNode next;

  private int index;

  private long bPlusTreeStartOffset;

  private BPlusTreeHeader bPlusTreeHeader;

  public BPlusTreeReader(File file, long bPlusTreeStartOffset) throws IOException {
    fileInput = new FileInput(file);
    this.bPlusTreeStartOffset = bPlusTreeStartOffset;
    fileInput.position(bPlusTreeStartOffset);
    index = 0;
  }

  public BPlusTreeHeader readBPlusTreeHeader(long bPlusTreeStartOffset) throws IOException {
    BPlusTreeHeader bPlusTreeHeader = new BPlusTreeHeader();
    fileInput.read(bPlusTreeHeader, bPlusTreeStartOffset);
    return bPlusTreeHeader;
  }

  @Override
  public BPlusTreeNode readBPlusTreeRootNode() throws IOException {
    if (bPlusTreeHeader == null) {
      bPlusTreeHeader = readBPlusTreeHeader(bPlusTreeStartOffset);
    }
    return readBPlusTreeRootNode(bPlusTreeHeader.getRootNodeOffset());
  }

  @Override
  public BPlusTreeNode readBPlusTreeRootNode(long bPlusTreeRootNodeOffset) throws IOException {
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode();
    fileInput.read(bPlusTreeNode, bPlusTreeRootNodeOffset);
    return bPlusTreeNode;
  }

  @Override
  public List<BPlusTreeEntry> getBPlusTreeEntries(Set<String> names) throws IOException {
    if (bPlusTreeHeader == null) {
      bPlusTreeHeader = readBPlusTreeHeader(bPlusTreeStartOffset);
    }
    return getBPlusTreeEntries(bPlusTreeHeader, names);
  }

  @Override
  public void close() throws IOException {
    fileInput.close();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (next != null) {
      return true;
    }
    if (bPlusTreeHeader == null) {
      bPlusTreeHeader = readBPlusTreeHeader(bPlusTreeStartOffset);
      if (bPlusTreeHeader.getLeftNodeCount() == 0) {
        return false;
      }
      fileInput.position(bPlusTreeHeader.getFirstLeftNodeOffset());
    }
    while (index < bPlusTreeHeader.getLeftNodeCount()) {
      BPlusTreeNode bPlusTreeNode = new BPlusTreeNode();
      fileInput.read(bPlusTreeNode);
      next = bPlusTreeNode;
      index++;
      return true;
    }
    return false;
  }

  @Override
  public BPlusTreeNode next() throws IOException {
    if (next == null) {
      throw new NoSuchElementException();
    }
    BPlusTreeNode now = next;
    next = null;
    return now;
  }

  /**
   * Read the b+ tree once to get all matching b+ tree entries
   *
   * @param rootNode {@link org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode
   *     BPlusTreeNode} of the b+ tree
   * @param names the name of all entries that need to be found
   */
  @Override
  public List<BPlusTreeEntry> getBPlusTreeEntries(BPlusTreeNode rootNode, Set<String> names)
      throws IOException {
    if (rootNode == null
        || rootNode.getbPlusTreeNodeType().equals(BPlusTreeNodeType.INVALID_NODE)) {
      return new ArrayList<>();
    }
    if (rootNode.getbPlusTreeNodeType().equals(BPlusTreeNodeType.LEAF_NODE)) {
      return rootNode.getBPlusTreeEntryFromLeafNode(names);
    }
    // order I/O
    PriorityQueue<MutableTriple<Long, BPlusTreeNode, Set<String>>> bPlusTreeNodeQueue =
        new PriorityQueue<>((o1, o2) -> Long.compare(o2.getLeft(), o1.getLeft()));
    List<BPlusTreeEntry> bPlusTreeEntries = new ArrayList<>();
    bPlusTreeNodeQueue.add(new MutableTriple<>(-1L, rootNode, names));
    while (!bPlusTreeNodeQueue.isEmpty()) {
      MutableTriple<Long, BPlusTreeNode, Set<String>> triple = bPlusTreeNodeQueue.poll();
      BPlusTreeNode currentNode = triple.middle;
      Set<String> currentName = triple.right;
      if (currentNode.getbPlusTreeNodeType().equals(BPlusTreeNodeType.INTERNAL_NODE)) {
        // order I/O
        TreeMap<BPlusTreeEntry, Set<String>> bPlusTreeEntrySetMap =
            new TreeMap<>((o1, o2) -> BPlusTreeEntry.compareWithOffset(o2, o1));
        bPlusTreeEntrySetMap.putAll(currentNode.getBPlusTreeEntryFromInternalNode(currentName));
        for (Map.Entry<BPlusTreeEntry, Set<String>> entry : bPlusTreeEntrySetMap.entrySet()) {
          BPlusTreeEntry bPlusTreeEntry = entry.getKey();
          BPlusTreeNode childNode = new BPlusTreeNode();
          fileInput.read(childNode, bPlusTreeEntry.getOffset());
          long offset;
          if (childNode.getbPlusTreeEntries().size() == 0) {
            offset = Long.MIN_VALUE;
          } else {
            offset = childNode.getbPlusTreeEntries().get(0).getOffset();
          }
          bPlusTreeNodeQueue.add(new MutableTriple<>(offset, childNode, entry.getValue()));
        }
      } else if (currentNode.getbPlusTreeNodeType().equals(BPlusTreeNodeType.LEAF_NODE)) {
        bPlusTreeEntries.addAll(currentNode.getBPlusTreeEntryFromLeafNode(currentName));
      } else {
        throw new InvalidPropertiesFormatException("read a invalid b+ tree node");
      }
    }
    return bPlusTreeEntries;
  }

  @Override
  public List<BPlusTreeEntry> getBPlusTreeEntries(
      BPlusTreeHeader bPlusTreeHeader, Set<String> names) throws IOException {
    if (bPlusTreeHeader.getLeftNodeCount() == 0) {
      return new ArrayList<>();
    }
    BPlusTreeNode root = new BPlusTreeNode();
    fileInput.read(root, bPlusTreeHeader.getRootNodeOffset());
    return getBPlusTreeEntries(root, names);
  }
}
