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

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public enum BPlusTreeNodeType implements IDiskEntry {
  INVALID_NODE((byte) 0),

  INTERNAL_NODE((byte) 1),

  LEAF_NODE((byte) 2);

  private byte type;

  BPlusTreeNodeType(byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }

  @Override
  public int serialize(DataOutputStream out) throws IOException {
    return ReadWriteIOUtils.write(type, out);
  }

  @Override
  public IDiskEntry deserialize(DataInputStream input) throws IOException {
    throw new UnsupportedOperationException("deserialize BPlusTreeNodeType");
  }

  @Override
  public String toString() {
    return "BPlusTreeNodeType{" + "type=" + type + '}';
  }
}
