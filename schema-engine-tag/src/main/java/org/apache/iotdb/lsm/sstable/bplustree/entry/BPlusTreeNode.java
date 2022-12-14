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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode implements IEntry {

  private BPlusTreeNodeType bPlusTreeNodeType;

  private List<BPlusTreeEntry> bPlusTreeEntries;

  public BPlusTreeNode(BPlusTreeNodeType bPlusTreeNodeType) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
  }

  public BPlusTreeNode(BPlusTreeNodeType bPlusTreeNodeType, List<BPlusTreeEntry> bPlusTreeEntries) {
    this.bPlusTreeNodeType = bPlusTreeNodeType;
    this.bPlusTreeEntries = bPlusTreeEntries;
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
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      bPlusTreeEntry.serialize(out);
    }
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    bPlusTreeNodeType.serialize(byteBuffer);
    for (BPlusTreeEntry bPlusTreeEntry : bPlusTreeEntries) {
      bPlusTreeEntry.serialize(byteBuffer);
    }
  }

  @Override
  public IEntry deserialize(DataInput input) throws IOException {
    return null;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  public void add(BPlusTreeEntry bPlusTreeEntry) {
    if (bPlusTreeEntries == null) {
      bPlusTreeEntries = new ArrayList<>();
    }
    bPlusTreeEntries.add(bPlusTreeEntry);
  }
}
