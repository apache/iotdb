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

import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNode;
import org.apache.iotdb.lsm.sstable.fileIO.FileInput;
import org.apache.iotdb.lsm.sstable.fileIO.IFileInput;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

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
}
