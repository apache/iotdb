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

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.TagSchemaDescriptor;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class BPlusTreeWriterTest {

  File file;

  BPlusTreeWriter bPlusTreeWriter;

  FileInputStream fileInputStream;

  Queue<BPlusTreeEntry> orderedQueue;

  Queue<BPlusTreeEntry> unorderedQueue;

  int degree;

  int bPlusTreePageSize;

  @Before
  public void setUp() throws Exception {
    file = new File("BPlusTreeWriterTest.txt");
    degree = TagSchemaDescriptor.getInstance().getTagSchemaConfig().getDegree();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(4);
    bPlusTreePageSize =
        TagSchemaDescriptor.getInstance().getTagSchemaConfig().getbPlusTreePageSize();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setbPlusTreePageSize(50);
    orderedQueue = new ArrayDeque<>();
    orderedQueue.add(new BPlusTreeEntry("aaa", 0));
    orderedQueue.add(new BPlusTreeEntry("bbb", 1));
    orderedQueue.add(new BPlusTreeEntry("c", 2));
    orderedQueue.add(new BPlusTreeEntry("dd", 3));
    orderedQueue.add(new BPlusTreeEntry("eeeee", 4));
    orderedQueue.add(new BPlusTreeEntry("fff", 5));
    orderedQueue.add(new BPlusTreeEntry("gggg", 6));
    orderedQueue.add(new BPlusTreeEntry("hhhhhhhhhh", 7));
    orderedQueue.add(new BPlusTreeEntry("x", 8));
    orderedQueue.add(new BPlusTreeEntry("yyyy", 9));
    orderedQueue.add(new BPlusTreeEntry("zz", 10));
    unorderedQueue = new ArrayDeque<>();
    unorderedQueue.add(new BPlusTreeEntry("bbb", 1));
    unorderedQueue.add(new BPlusTreeEntry("c", 2));
    unorderedQueue.add(new BPlusTreeEntry("aaa", 0));
    unorderedQueue.add(new BPlusTreeEntry("dd", 3));
    unorderedQueue.add(new BPlusTreeEntry("fff", 5));
    unorderedQueue.add(new BPlusTreeEntry("eeeee", 4));
    unorderedQueue.add(new BPlusTreeEntry("gggg", 6));
    unorderedQueue.add(new BPlusTreeEntry("hhhhhhhhhh", 7));
    unorderedQueue.add(new BPlusTreeEntry("zz", 10));
    unorderedQueue.add(new BPlusTreeEntry("yyyy", 9));
    unorderedQueue.add(new BPlusTreeEntry("x", 8));
  }

  @After
  public void tearDown() throws Exception {
    if (bPlusTreeWriter != null) {
      bPlusTreeWriter.close();
    }
    if (fileInputStream != null) {
      fileInputStream.close();
    }
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(degree);
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setbPlusTreePageSize(bPlusTreePageSize);
    bPlusTreeWriter = null;
    file.delete();
    orderedQueue = null;
    unorderedQueue = null;
  }

  @Test
  public void testWriteBPlusTree() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);
    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);
    orderedQueue.forEach(
        bPlusTreeEntry ->
            bPlusTreeWriter.collectRecord(bPlusTreeEntry.getName(), bPlusTreeEntry.getOffset()));

    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeBPlusTree();

    assertTest(bPlusTreeHeader);
  }

  @Test
  public void testSortAndWriteBPlusTree() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);
    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);
    unorderedQueue.forEach(
        bPlusTreeEntry ->
            bPlusTreeWriter.collectRecord(bPlusTreeEntry.getName(), bPlusTreeEntry.getOffset()));
    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.sortAndWriteBPlusTree();

    assertTest(bPlusTreeHeader);
  }

  @Test
  public void testWriteBPlusTreeFromOrderedQueue() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);

    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);

    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeBPlusTree(orderedQueue, true);

    assertTest(bPlusTreeHeader);
  }

  @Test
  public void testWriteBPlusTreeFromUnOrderedQueue() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);

    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);

    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeBPlusTree(unorderedQueue, false);

    assertTest(bPlusTreeHeader);
  }

  @Test
  public void testWriteBPlusTreeFromOrderedMap() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);

    Map<String, Long> map = new TreeMap<>();

    orderedQueue.forEach(
        bPlusTreeEntry -> map.put(bPlusTreeEntry.getName(), bPlusTreeEntry.getOffset()));
    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);

    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeBPlusTree(map, true);

    assertTest(bPlusTreeHeader);
  }

  @Test
  public void testWriteBPlusTreeFromUnOrderedMap() throws IOException {

    FileOutputStream fileOutputStream = new FileOutputStream(file);
    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);

    Map<String, Long> map = new HashMap<>();

    unorderedQueue.forEach(
        bPlusTreeEntry -> map.put(bPlusTreeEntry.getName(), bPlusTreeEntry.getOffset()));
    bPlusTreeWriter = new BPlusTreeWriter(fileOutput);

    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeBPlusTree(map, false);

    assertTest(bPlusTreeHeader);
  }

  private void assertTest(BPlusTreeHeader bPlusTreeHeader) throws IOException {

    assertEquals(bPlusTreeHeader.getMax(), "zz");
    assertEquals(bPlusTreeHeader.getMin(), "aaa");
    assertEquals(bPlusTreeHeader.getFirstLeftNodeOffset(), 0);
    assertEquals(bPlusTreeHeader.getLeftNodeCount(), 5);

    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    fileInputStream = new FileInputStream(file);
    fileInputStream.getChannel().read(buffer);

    buffer.flip();
    List<BPlusTreeNode> bPlusTreeNodes = new ArrayList<>();

    while (buffer.position() < buffer.limit()) {
      BPlusTreeNode bPlusTreeNode = new BPlusTreeNode();
      bPlusTreeNode.deserialize(buffer);
      bPlusTreeNodes.add(bPlusTreeNode);
    }

    assertEquals(bPlusTreeNodes.size(), 8);

    buffer.position((int) bPlusTreeHeader.getRootNodeOffset());
    BPlusTreeNode rootNode = new BPlusTreeNode();
    rootNode.deserialize(buffer);

    assertEquals(rootNode, bPlusTreeNodes.get(bPlusTreeNodes.size() - 1));

    buffer.position((int) rootNode.getbPlusTreeEntries().get(0).getOffset());
    BPlusTreeNode nextLevelNode = new BPlusTreeNode();
    nextLevelNode.deserialize(buffer);

    assertEquals(nextLevelNode, bPlusTreeNodes.get(bPlusTreeNodes.size() - 3));

    buffer.position((int) nextLevelNode.getbPlusTreeEntries().get(0).getOffset());
    nextLevelNode = new BPlusTreeNode();
    nextLevelNode.deserialize(buffer);

    assertEquals(nextLevelNode, bPlusTreeNodes.get(0));

    buffer.position((int) bPlusTreeHeader.getFirstLeftNodeOffset());
    for (int i = 0; i < bPlusTreeHeader.getLeftNodeCount(); i++) {
      BPlusTreeNode leftNode = new BPlusTreeNode();
      leftNode.deserialize(buffer);
      assertEquals(leftNode, bPlusTreeNodes.get(i));
    }
  }
}
