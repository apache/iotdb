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

import org.apache.iotdb.lsm.sstable.bplustree.config.BPlushTreeConfig;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNodeType;
import org.apache.iotdb.lsm.sstable.writer.FileOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

public class BPlusTreeWriter implements IBPlusTreeWriter {

  private final Queue<BPlusTreeEntry> bPlusTreeEntryQueue;

  private FileOutput fileOutput;

  private BPlushTreeConfig bPlushTreeConfig;

  private ByteBuffer byteBuffer;

  public BPlusTreeWriter() {
    this.bPlusTreeEntryQueue = new ArrayDeque<>();
    byteBuffer = ByteBuffer.allocate(1024 * 1024);
  }

  public BPlusTreeWriter(Queue<BPlusTreeEntry> bPlusTreeEntryQueue, FileOutput fileOutput) {
    this.bPlusTreeEntryQueue = bPlusTreeEntryQueue;
    this.fileOutput = fileOutput;
  }

  public BPlusTreeWriter(
      Queue<BPlusTreeEntry> bPlusTreeEntryQueue,
      FileOutput fileOutput,
      BPlushTreeConfig bPlushTreeConfig) {
    this.bPlusTreeEntryQueue = bPlusTreeEntryQueue;
    this.fileOutput = fileOutput;
    this.bPlushTreeConfig = bPlushTreeConfig;
  }

  public void write(Map<String, Integer> entries) {}

  public void writeLeafNode() throws IOException {
    Queue<BPlusTreeEntry> nextLevelBPlusTreeEntryQueue = new ArrayDeque<>();
    int count = 0;
    BPlusTreeNode bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
    while (!bPlusTreeEntryQueue.isEmpty()) {
      BPlusTreeEntry bPlusTreeEntry = bPlusTreeEntryQueue.poll();
      if (count < bPlushTreeConfig.getDegree()) {
        bPlusTreeNode.add(bPlusTreeEntry);
      } else {
        long startOffset = fileOutput.write(bPlusTreeNode);
        bPlusTreeNode = new BPlusTreeNode(BPlusTreeNodeType.LEAF_NODE);
        bPlusTreeEntry = new BPlusTreeEntry(bPlusTreeEntry.getName(), startOffset);
        nextLevelBPlusTreeEntryQueue.add(bPlusTreeEntry);
      }
    }
  }

  public Queue<BPlusTreeEntry> getbPlusTreeEntryQueue() {
    return bPlusTreeEntryQueue;
  }

  public FileOutput getFileOutput() {
    return fileOutput;
  }

  public void setFileOutput(FileOutput fileOutput) {
    this.fileOutput = fileOutput;
  }

  public BPlushTreeConfig getbPlushTreeConfig() {
    return bPlushTreeConfig;
  }

  public void setbPlushTreeConfig(BPlushTreeConfig bPlushTreeConfig) {
    this.bPlushTreeConfig = bPlushTreeConfig;
  }
}
