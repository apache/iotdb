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
package org.apache.iotdb.lsm.sstable.index.bplustree.writer;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.fileIO.ISSTableOutputStream;
import org.apache.iotdb.lsm.sstable.fileIO.SSTableOutputStream;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.index.bplustree.entry.BPlusTreeNodeType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

public class BPlusTreeWriter implements IBPlusTreeWriter {

  private Queue<BPlusTreeEntry> currentBPlusTreeEntryQueue;

  private Queue<BPlusTreeEntry> upperLevelBPlusTreeEntryQueue;

  private ISSTableOutputStream fileOutput;

  private final TagSchemaConfig bPlushTreeConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  private BPlusTreeHeader bPlushTreeHeader;

  private ByteBuffer byteBuffer;

  public BPlusTreeWriter(ISSTableOutputStream fileOutput) {
    this.fileOutput = fileOutput;
    this.currentBPlusTreeEntryQueue = new ArrayDeque<>();
    this.upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    bPlushTreeHeader = new BPlusTreeHeader();
  }

  public BPlusTreeWriter(
      Queue<BPlusTreeEntry> bPlusTreeEntryQueue, ISSTableOutputStream fileOutput) {
    this.currentBPlusTreeEntryQueue = bPlusTreeEntryQueue;
    this.upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    this.fileOutput = fileOutput;
    bPlushTreeHeader = new BPlusTreeHeader();
  }

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @param ordered whether the queue is in order
   * @return start offset of the b+ tree
   * @throws IOException
   */
  @Override
  public long write(Map<String, Long> records, boolean ordered) throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    BPlusTreeHeader bPlusTreeHeader = writeBPlusTree();
    return fileOutput.write(bPlusTreeHeader);
  }

  /**
   * generate a b+ tree for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @param ordered whether the queue is in order
   * @return b+ tree header
   * @throws IOException
   */
  @Override
  public BPlusTreeHeader writeBPlusTree(Map<String, Long> records, boolean ordered)
      throws IOException {
    setCurrentBPlusTreeEntryQueue(records, ordered);
    return writeBPlusTree();
  }

  /**
   * generate a b+ tree for records and write to disk
   *
   * @return b+ tree header
   * @throws IOException
   */
  private BPlusTreeHeader writeBPlusTree() throws IOException {
    setBPlushTreeHeader();
    if (byteBuffer == null) {
      byteBuffer = ByteBuffer.allocate(bPlushTreeConfig.getBPlusTreePageSize());
      byteBuffer.position(5);
    }
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    BPlusTreeEntry bPlusTreeEntry = null;
    int count = 0;
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (bPlusTreeNode.getCount() >= bPlushTreeConfig.getDegree()
          || !bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer)) {
        writeBPlusTreePage(bPlusTreeNode);
        count++;
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
        bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer);
      }
    }
    if (bPlusTreeEntry != null) {
      bPlushTreeHeader.setMax(bPlusTreeEntry.getName());
    }
    if (bPlusTreeNode.getCount() > 0) {
      writeBPlusTreePage(bPlusTreeNode);
      count++;
    }
    currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
    bPlushTreeHeader.setLeftNodeCount(count);
    if (count == 1) {
      bPlushTreeHeader.setRootNodeOffset(bPlushTreeHeader.getFirstLeftNodeOffset());
      return bPlushTreeHeader;
    }
    return writeInternalNode();
  }

  private BPlusTreeHeader writeInternalNode() throws IOException {
    upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
    int count = 0;
    long rootNodeOffset = 0;
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      BPlusTreeEntry bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (bPlusTreeNode.getCount() >= bPlushTreeConfig.getDegree()
          || !bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer)) {
        rootNodeOffset = writeBPlusTreePage(bPlusTreeNode);
        count++;
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        bPlusTreeNode.addAndSerialize(bPlusTreeEntry, byteBuffer);
      }
      if (currentBPlusTreeEntryQueue.isEmpty()) {
        if (bPlusTreeNode.getCount() > 0) {
          rootNodeOffset = writeBPlusTreePage(bPlusTreeNode);
          count++;
        }
        if (count == 1) {
          bPlushTreeHeader.setRootNodeOffset(rootNodeOffset);
          return bPlushTreeHeader;
        }
        currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
        upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        count = 0;
      }
    }
    return bPlushTreeHeader;
  }

  public Queue<BPlusTreeEntry> getBPlusTreeEntryQueue() {
    return currentBPlusTreeEntryQueue;
  }

  public ISSTableOutputStream getFileOutput() {
    return fileOutput;
  }

  public void setFileOutput(SSTableOutputStream fileOutput) {
    this.fileOutput = fileOutput;
  }

  private void setBPlushTreeHeader() throws IOException {
    if (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlushTreeHeader.setMax(currentBPlusTreeEntryQueue.peek().getName());
      bPlushTreeHeader.setMin(currentBPlusTreeEntryQueue.peek().getName());
      bPlushTreeHeader.setFirstLeftNodeOffset(fileOutput.getPosition());
    }
  }

  private long writeBPlusTreePage(BPlusTreeNode bPlusTreeNode) throws IOException {
    byteBuffer.put(0, bPlusTreeNode.getbPlusTreeNodeType().getType());
    byteBuffer.putInt(1, bPlusTreeNode.getCount());
    byteBuffer.flip();
    long startOffset = fileOutput.getPosition();
    fileOutput.write(byteBuffer.array(), 0, byteBuffer.limit());
    upperLevelBPlusTreeEntryQueue.add(new BPlusTreeEntry(bPlusTreeNode.getMin(), startOffset));
    byteBuffer.clear();
    byteBuffer.position(5);
    return startOffset;
  }

  private void setCurrentBPlusTreeEntryQueue(Map<String, Long> records, boolean ordered) {
    if (ordered) {
      records.forEach(
          (key, value) -> currentBPlusTreeEntryQueue.add(new BPlusTreeEntry(key, value)));
    } else {
      records.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              entry ->
                  currentBPlusTreeEntryQueue.add(
                      new BPlusTreeEntry(entry.getKey(), entry.getValue())));
    }
  }

  @Override
  public void close() throws IOException {
    if (currentBPlusTreeEntryQueue != null) {
      currentBPlusTreeEntryQueue.clear();
    }
    if (upperLevelBPlusTreeEntryQueue != null) {
      upperLevelBPlusTreeEntryQueue.clear();
    }
    if (fileOutput != null) {
      fileOutput.close();
    }
  }
}
