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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class BPlusTreeWriterTest {

  File file;

  BPlusTreeWriter bPlusTreeWriter;

  int degree;

  @Before
  public void setUp() throws Exception {
    file = new File("BPlusTreeWriterTest.txt");
    degree = TagSchemaDescriptor.getInstance().getTagSchemaConfig().getDegree();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(4);
  }

  @After
  public void tearDown() throws Exception {
    bPlusTreeWriter.close();
    TagSchemaDescriptor.getInstance().getTagSchemaConfig().setDegree(degree);
    bPlusTreeWriter = null;
    file.delete();
  }

  @Test
  public void testWriteBPlusTree() throws IOException {

    //    FileOutputStream fileOutputStream = new FileOutputStream(file);
    //    FileOutput fileOutput = new FileOutput(fileOutputStream, 1024 * 1024);
    //    Queue<BPlusTreeEntry> queue = new ArrayDeque<>();
    //    queue.add(new BPlusTreeEntry("aaa", 0));
    //    queue.add(new BPlusTreeEntry("bbb", 1));
    //    queue.add(new BPlusTreeEntry("c", 2));
    //    queue.add(new BPlusTreeEntry("dd", 3));
    //    queue.add(new BPlusTreeEntry("eeeee", 4));
    //    queue.add(new BPlusTreeEntry("fff", 5));
    //    queue.add(new BPlusTreeEntry("gggg", 6));
    //    queue.add(new BPlusTreeEntry("hhhhhhhhhh", 7));
    //    queue.add(new BPlusTreeEntry("x", 8));
    //    queue.add(new BPlusTreeEntry("yyyy", 9));
    //    queue.add(new BPlusTreeEntry("zz", 10));
    //
    //    bPlusTreeWriter = new BPlusTreeWriter(queue, fileOutput);
    //
    //    BPlusTreeHeader bPlusTreeHeader = bPlusTreeWriter.writeLeafNode();
    //
    //    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    //
    //    FileInputStream fileInputStream = new FileInputStream(file);
    //    fileInputStream.getChannel().read(buffer);
    //
    //    buffer.flip();

    //    buffer.position((int) bPlusTreeHeader.getOffset());
    //        BPlusTreeNode rootNode = new BPlusTreeNode();
    //        rootNode.deserialize(buffer);
    //    while (true) {
    //      BPlusTreeNode bPlusTreeNode = new BPlusTreeNode();
    //      System.out.println(bPlusTreeNode.deserialize(buffer));
    //    }
  }
}
