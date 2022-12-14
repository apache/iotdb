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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum BPlusTreeNodeType implements IEntry {
  INTERNAL_NODE((byte) 0),

  LEAF_NODE((byte) 1);

  private final byte type;

  BPlusTreeNodeType(byte type) {
    this.type = type;
  }

  public byte getType() {
    return type;
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(type, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(type, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInput input) throws IOException {
    return null;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    return null;
  }
}
