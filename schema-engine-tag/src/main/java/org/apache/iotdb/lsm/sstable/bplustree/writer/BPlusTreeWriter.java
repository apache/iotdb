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
package org.apache.iotdb.lsm.sstable.bplustree.writer;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaConfig;
import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNodeType;
import org.apache.iotdb.lsm.sstable.writer.FileOutput;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

public class BPlusTreeWriter implements IBPlusTreeWriter {

  private Queue<BPlusTreeEntry> currentBPlusTreeEntryQueue;

  private Queue<BPlusTreeEntry> upperLevelBPlusTreeEntryQueue;

  private FileOutput fileOutput;

  private final TagSchemaConfig bPlushTreeConfig =
      TagSchemaDescriptor.getInstance().getTagSchemaConfig();

  private BPlusTreeHeader bPlushTreeHeader;

  public BPlusTreeWriter(Queue<BPlusTreeEntry> bPlusTreeEntryQueue, FileOutput fileOutput) {
    this.currentBPlusTreeEntryQueue = bPlusTreeEntryQueue;
    this.upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    this.fileOutput = fileOutput;
    bPlushTreeHeader = new BPlusTreeHeader();
  }

  public void write(Map<String, Integer> entries) {}

  public BPlusTreeHeader writeLeafNode() throws IOException {
    setBPlushTreeHeaderMaxAndMin();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    BPlusTreeEntry bPlusTreeEntry = null;
    if (currentBPlusTreeEntryQueue.size() <= bPlushTreeConfig.getDegree()) {
      return directWriteOneBPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    }
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (!bPlusTreeNode.needToSplit()) {
        bPlusTreeNode.add(bPlusTreeEntry);
      } else {
        writeBPlusTreeNode(bPlusTreeNode);
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
        bPlusTreeNode.add(bPlusTreeEntry);
      }
    }
    if (bPlusTreeEntry != null) {
      bPlushTreeHeader.setMax(bPlusTreeEntry.getName());
    }
    if (bPlusTreeNode.getCount() > 0) {
      writeBPlusTreeNode(bPlusTreeNode);
    }
    currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
    return writeInternalNode();
  }

  private BPlusTreeHeader writeInternalNode() throws IOException {
    upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
    int size = currentBPlusTreeEntryQueue.size();
    while (!currentBPlusTreeEntryQueue.isEmpty()) {
      if (size <= bPlushTreeConfig.getDegree()) {
        return directWriteOneBPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
      }
      BPlusTreeEntry bPlusTreeEntry = currentBPlusTreeEntryQueue.poll();
      if (!bPlusTreeNode.needToSplit()) {
        bPlusTreeNode.add(bPlusTreeEntry);
      } else {
        writeBPlusTreeNode(bPlusTreeNode);
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
        bPlusTreeNode.add(bPlusTreeEntry);
      }
      if (currentBPlusTreeEntryQueue.isEmpty()) {
        if (bPlusTreeNode.getCount() > 0) {
          writeBPlusTreeNode(bPlusTreeNode);
        }
        currentBPlusTreeEntryQueue = upperLevelBPlusTreeEntryQueue;
        size = currentBPlusTreeEntryQueue.size();
        upperLevelBPlusTreeEntryQueue = new ArrayDeque<>();
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.INTERNAL_NODE);
      }
    }
    return bPlushTreeHeader;
  }

  public Queue<BPlusTreeEntry> getBPlusTreeEntryQueue() {
    return currentBPlusTreeEntryQueue;
  }

  public FileOutput getFileOutput() {
    return fileOutput;
  }

  public void setFileOutput(FileOutput fileOutput) {
    this.fileOutput = fileOutput;
  }

  private void setBPlushTreeHeaderMaxAndMin() {
    if (!currentBPlusTreeEntryQueue.isEmpty()) {
      bPlushTreeHeader.setMax(currentBPlusTreeEntryQueue.peek().getName());
      bPlushTreeHeader.setMin(currentBPlusTreeEntryQueue.peek().getName());
    }
  }

  private long writeBPlusTreeNode(BPlusTreeNode bPlusTreeNode) throws IOException {
    long startOffset = fileOutput.write(bPlusTreeNode);
    upperLevelBPlusTreeEntryQueue.add(new BPlusTreeEntry(bPlusTreeNode.getMin(), startOffset));
    return startOffset;
  }

  private BPlusTreeHeader directWriteOneBPlusTreeNode(BPlusTreeNodeType type) throws IOException {
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(type);
    for (BPlusTreeEntry bPlusTreeEntry : currentBPlusTreeEntryQueue) {
      bPlusTreeNode.add(bPlusTreeEntry);
    }
    if (bPlusTreeNode.getCount() > 0) {
      long rootNodeOffset = writeBPlusTreeNode(bPlusTreeNode);
      bPlushTreeHeader.setOffset(rootNodeOffset);
    }
    return bPlushTreeHeader;
  }

  @Override
  public void close() throws IOException {
    currentBPlusTreeEntryQueue.clear();
    upperLevelBPlusTreeEntryQueue.clear();
    fileOutput.close();
  }
}
