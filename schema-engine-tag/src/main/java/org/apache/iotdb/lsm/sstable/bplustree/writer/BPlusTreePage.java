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

import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeNodeType;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class BPlusTreePage {
  private BPlusTreeNodeType bPlusTreeNodeType;

  private int count;

  private String min;

  private ByteBuffer byteBuffer;

  public BPlusTreePage(BPlusTreeNodeType bPlusTreeNodeType, ByteBuffer byteBuffer) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
    byteBuffer.clear();
    this.byteBuffer = byteBuffer;
    byteBuffer.position(5);
    count = 0;
  }

  public boolean add(BPlusTreeEntry bPlusTreeEntry) {
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
    count++;
    if (min == null) {
      min = bPlusTreeEntry.getName();
    } else {
      min = min.compareTo(bPlusTreeEntry.getName()) < 0 ? min : bPlusTreeEntry.getName();
    }
    return true;
  }

  public int getCount() {
    return count;
  }

  public String getMin() {
    return min;
  }

  public BPlusTreeNodeType getbPlusTreeNodeType() {
    return bPlusTreeNodeType;
  }
}
